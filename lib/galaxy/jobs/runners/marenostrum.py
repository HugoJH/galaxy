"""
Job control via the DRMAA API.
"""

import json
import logging
import sys
import os
import re
import string
import conda_pack
import paramiko
import tempfile
import shutil
from hashlib import md5
from galaxy import model
from galaxy.jobs import JobDestination
from galaxy.jobs.handler import DEFAULT_JOB_PUT_FAILURE_MESSAGE
from galaxy.jobs.runners import (
    AsynchronousJobRunner,
    AsynchronousJobState
)
from galaxy.util import (
    asbool,
    commands,
    unicodify
)

from galaxy.jobs.runners.util.marenostrum import (
    slurm,
    ssh_credentials,
    ssh_session
)

from galaxy.jobs.runners.util.cli import split_params
drmaa = None

log = logging.getLogger(__name__)

__all__ = ('MarenostrumJobRunner',)

DEFAULT_EMBED_METADATA_IN_JOB = True
RETRY_EXCEPTIONS_LOWER = frozenset({'invalidjobexception', 'internalexception'})
UNKNOWN = 0
SUBMITTED = 1
RUNNING = 2
CANCELLED = 3
FINISHED = 4
CLOSING = 5

class MarenostrumJobRunner(AsynchronousJobRunner):
    """
    Job runner backed by a finite pool of worker threads. FIFO scheduling
    """
    runner_name = "MarenostrumRunner"
    restrict_job_name_length = 15
    MN4_library_name = "MN4 keys"
    MN4_library_description = "Library to store MN4 keys"
    MN4_library_synopsis = "This library is intended to store Marenostrum 4 library keys"
    is_key_missing = False
    task = None

    def __init__(self, app, nworkers, **kwargs):
        """Start the job runner"""
        log.info("Job being created and scheduled in Marenostrum")
        super().__init__(app, nworkers, **kwargs)
        self._init_monitor_thread()
        self._init_worker_threads()
        self.config = app.config

    def queue_job(self, job_wrapper):
        self._task_setup(job_wrapper)
        embed_metadata = asbool(job_wrapper.job_destination.params.get("embed_metadata_in_job", DEFAULT_EMBED_METADATA_IN_JOB))
        if not self.prepare_job(job_wrapper, include_metadata=embed_metadata):
            return

        with open(os.path.join(job_wrapper.working_directory, "tool_script.sh"), "r") as f:
            script = f.read()

        self._send_tools_to_MN4(job_wrapper, script)
        prepared_script = self._prepare_runscript(job_wrapper, script)
        self._send_inputs_to_MN4(job_wrapper)

        # job was deleted while we were preparing it
        if job_wrapper.get_state() in (model.Job.states.DELETED, model.Job.states.STOPPED):
            log.debug("(%s) Job deleted/stopped by user before it entered the queue", job_wrapper.get_id_tag())
            if job_wrapper.cleanup_job in ("always", "onsuccess"):
                job_wrapper.cleanup()
            return

        self.task.set_remote_base_path(self.MN4_wd)
        job_id = self.task.submit(queue_settings="debug", job_name=self._job_name(job_wrapper),
                                  local_run_script=prepared_script)
        self._save_task(job_wrapper)
        job_wrapper.set_external_id(job_id)
        self.ajs = AsynchronousJobState(files_dir=job_wrapper.working_directory, job_id=job_id, job_wrapper=job_wrapper, job_destination=job_wrapper.job_destination)
        self.monitor_queue.put(self.ajs)

    def check_watched_item(self, job_state):
        if self.is_key_missing:
            self.set_public_key_in_stderr(job_state.output_file,
                                          job_state.exit_code_file,
                                          job_state.error_file)
            job_state.running = False
            job_state.job_wrapper.change_state(model.Job.states.ERROR)
            self.mark_as_finished(job_state)
            return None

        job_id = job_state.job_id
        job_status = self.task.get_status(job_id)
        state = model.Job.states.QUEUED

        print("---------------JOB_STATUS------------------------")
        print(job_status)
        print("---------------JOB_STATUS------------------------")

        if job_status == "RUNNING":
            job_state.running = True
            state = model.Job.states.RUNNING
        elif job_status == "COMPLETING":
            job_state.running = False
            state = model.Job.states.STOPPING
        elif job_status == "COMPLETED":
            job_state.running = False
            state = model.Job.states.OK
            self.create_log_files(job_state.output_file,
                                  job_state.exit_code_file,
                                  job_state.error_file,
                                  job_state.job_wrapper)
            self._get_outputs(job_state.job_wrapper.compute_outputs())
            self.mark_as_finished(job_state)
        elif job_status == "FAILED":
            job_state.running = False
            state = model.Job.states.FAILED
            self.create_log_files(job_state.output_file,
                                  job_state.exit_code_file,
                                  job_state.error_file,
                                  job_state.job_wrapper)
            self.mark_as_failed(job_state)
        elif job_status == "CANCELLED+":
            job_state.running = False
            state = model.Job.states.DELETED

        job_state.job_wrapper.change_state(state)
        if job_status == "COMPLETED" or job_status == "FAILED":
            return None
        return job_state

    def stop_job(self, job_wrapper):
        """Attempts to delete a job from the DRM queue"""

        log.info("Job being stopped in Marenostrum")

    def recover(self, job, job_wrapper):
        """Recovers jobs stuck in the queued/running state when Galaxy started"""
        self._task_setup(job_wrapper)
        ajs = AsynchronousJobState(files_dir=job_wrapper.working_directory, job_wrapper=job_wrapper)
        ajs.job_id = str(job_wrapper.job_id)
        ajs.job_destination = job_wrapper.job_destination
        job_wrapper.command_line = job.command_line
        ajs.job_wrapper = job_wrapper
        if job.state == model.Job.states.RUNNING:
            ajs.old_state = 'R'
            ajs.running = True
        elif job.state == model.Job.states.QUEUED:
            ajs.old_state = 'Q'
            ajs.running = False
        self.monitor_queue.put(ajs)

    def _task_setup(self, job_wrapper):
        self._prepare_preferences(job_wrapper)
        self._prepare_keys(job_wrapper.app.user_manager.by_id(job_wrapper.user_id).username, self.host_MN4, self.user_MN4)
        self._prepare_task(self.host_MN4, self.user_MN4, job_wrapper)

    def _prepare_preferences(self, job_wrapper):
        log.info("Getting user preferences")
        preferences = job_wrapper.app.user_manager.preferences(job_wrapper.app.user_manager.by_id(job_wrapper.user_id))
        self.user_MN4 = json.loads(preferences["extra_user_preferences"])["MN4_user|MN4_user"]
        self.conda_MN4 = json.loads(preferences["extra_user_preferences"])["MN4_conda|MN4_conda"]
        self.host_MN4 = json.loads(preferences["extra_user_preferences"])["MN4_host|MN4_host"]
        self.jobs_MN4 = json.loads(preferences["extra_user_preferences"])["MN4_jobs|MN4_jobs"]
        self.file_path = os.path.dirname(self.jobs_MN4) + "/files"
        self.tool_path = os.path.dirname(self.jobs_MN4) + "/tools"
        self.task_cache = self.config.get('data_dir') + "/MN4_task_cache"
        os.makedirs(self.task_cache, exist_ok=True)

    def _prepare_keys(self, username, host, MN4_username):
        log.info("Preparing keys")
        ssh_folder = os.path.join(self.config.get('mn4_ssh_keys_path'), username)
        os.makedirs(ssh_folder, exist_ok=True)
        self.keys_path = os.path.join(ssh_folder, "MN4_keys")
        self.public_path = os.path.join(ssh_folder, "MN4_public")
        self.private_path = os.path.join(ssh_folder, "MN4_private")
        try: #If keys exist, load them from file
            self.credentials = ssh_credentials.SSHCredentials()
            self.credentials.load_from_file(self.keys_path)
        except: #Otherwise create them
            self.credentials = ssh_credentials.SSHCredentials(host=host,
                             userid=MN4_username,
                             generate_key=False)

            self.credentials.generate_key(2048)
            self.credentials.save(output_path=self.keys_path,
                                  public_key_path=self.public_path,
                                  private_key_path=self.private_path)
        self._check_host_auth()

    def _prepare_task(self, host, userid, job_wrapper):
        self.task = slurm.Slurm(host, userid, False)
        self.task.set_credentials(self.credentials)

        #Send galaxy's working dir to Marenostrum designated folder
        id_path ="/".join(job_wrapper.working_directory.split("/")[-2:])
        self.MN4_wd = self.jobs_MN4 + "/" + id_path
        self.task.set_local_data_bundle(local_data_path=job_wrapper.working_directory)
        self.task.send_input_data(self.MN4_wd)
        self.task.load_host_config(host_config_path=os.path.join(self.config.config_dir,"BSC_MN4.json"))

    def _recover_task(self,host, userid, job_id):
        self.task = slurm.Slurm(host, userid, False)
        self.task.set_credentials(self.credentials)
        try:
           self.task.load_data_from_file(self.task_cache + "/" + job_id + ".task")
        except Exception as e:
            print (e)

    def _save_task(self, job_wrapper):
        job_id = job_wrapper.job_id
        self.task.save(self.task_cache + "/" + str(job_id) + ".task")

    def _check_host_auth(self):
        try:
            self.credentials.check_host_auth()
        except paramiko.ssh_exception.SSHException as e:
            log.info(str(e))
            log.info("Connection to Marenostrum failed.Please copy the public key in the authorized host file")
            self.is_key_missing = True
            return False

    def _job_name(self, job_wrapper):
        external_runjob_script = job_wrapper.get_destination_configuration("drmaa_external_runjob_script", None)
        galaxy_id_tag = job_wrapper.get_id_tag()

        # define job attributes
        self.job_name = 'g%s' % galaxy_id_tag
        if job_wrapper.tool.old_id:
            self.job_name += '_%s' % job_wrapper.tool.old_id
        if not self.redact_email_in_job_name and external_runjob_script is None:
            self.job_name += '_%s' % job_wrapper.user
        self.job_name = ''.join(x if x in (string.ascii_letters + string.digits + '_') else '_' for x in self.job_name)
        if self.restrict_job_name_length:
            self.job_name = self.job_name[:self.restrict_job_name_length]
        return self.job_name

    def set_public_key_in_stderr(self, output_file, exit_code_file, error_file):
        with open(self.public_path,'r') as pkf:
            public_key = pkf.read()
        with open(job_state.output_file, 'w') as f:
            f.write("")
        with open(job_state.exit_code_file, 'w') as f:
            f.write("1")
        with open(job_state.error_file, 'w') as f:
            f.write("Connection to Marenostrum failed.Please copy the public key in the authorized host file\n\n")
            f.write("Copy the following key to the authorized_keys file in your Marenostrum account:\n\n ")
            f.write(public_key)

    def _send_conda_env(self, conda_env_path, remote_env_path):
        if not os.path.isfile(conda_env_path + ".tar.gz"):
            conda_pack.CondaEnv.from_prefix(conda_env_path).pack(output=conda_env_path + ".tar.gz", verbose=True)

        if self.local_checksum(conda_env_path + ".tar.gz") != self.remote_checksum(remote_env_path + ".tar.gz"):
            self.task.set_local_file(local_data_filepath=conda_env_path + ".tar.gz")
            self.task.send_input_data(remote_env_path, overwrite=False)
            self.task.ssh_session.run_command("mkdir -p " + remote_env_path + "/" + os.path.basename(conda_env_path))
            self.task.ssh_session.run_command("tar --skip-old-files -z -x  -v -f " + remote_env_path + "/" + os.path.basename(conda_env_path + ".tar.gz") +
                                              " -C " + remote_env_path + "/" + os.path.basename(conda_env_path))

    def create_log_files(self, output_file, exit_code_file, error_file, job_wrapper):
        self.task.get_output_data(job_wrapper.working_directory)
        output_files = self.task.task_data['output_data_bundle'].files
        remote_output = job_wrapper.working_directory + '/' + [out for out in output_files if "out" in out][0]
        remote_error =  job_wrapper.working_directory + '/' + [err for err in output_files if "err" in err][0]
        os.rename(remote_error, error_file)
        os.rename(remote_output, output_file)

    # def _galaxy_to_MN4_paths (self, galaxy_path):
    def local_checksum(self, path):
        digest = ""
        with open(path,'rb') as f:
            file_content=f.read()
            digest = md5(file_content).hexdigest()
        return digest

    def remote_checksum(self, path):
        md5sum = ""
        try:
            command_output = self.task.ssh_session.run_command("md5sum " + path)[0]
            md5sum = command_output.split()[0]
        except:
            print("No remote file")
        return md5sum

    def _send_inputs_to_MN4(self, job_wrapper):
        for path in job_wrapper.get_input_paths():
            input_path = path.real_path
            basepath = re.sub(self.config.get('file_path'), "", input_path)
            self.task.ssh_session.run_command("mkdir -p " + self.file_path + os.path.dirname(basepath))
            self.task.set_local_file(local_data_filepath=input_path)
            self.task.send_input_data(self.file_path + "/" + os.path.dirname(basepath))

    def _send_default_tool(self, tool_path, job_wrapper):
        self.task.set_local_file(local_data_filepath=tool_path)
        self.task.send_input_data(remote_base_path=self.tool_path + "/" + os.path.basename(job_wrapper.tool.tool_dir))

    def _send_tools_to_MN4(self, job_wrapper, script):
        # ENVIAR ENTORNO CONDA A MN4
        if self.config.get('tool_path') in job_wrapper.tool.tool_dir:
            print (script.split())
            print(job_wrapper.tool.tool_dir)
            try:
                tool_full_path = [x for x in re.split("'| |\n", script) if job_wrapper.tool.tool_dir in x][0]
                self._send_default_tool(tool_full_path, job_wrapper)
            except IndexError:
                print("Builtin tool in script")
        try:
            local_conda_path = [x for x in script.split("'")  if "_conda/envs/_" in x][0]
            self._send_conda_env(local_conda_path, self.conda_MN4)
        except:
            print("No conda env found")
        # ENVIAR ENTORNO DE CONDA BASE
        self._send_conda_env(self.config.get('tool_dependency_dir') + "/_conda", self.conda_MN4)


    def _get_outputs(self, job_outputs):
        for output in job_outputs:
            output_path = output.real_path
            remote_path = self.file_path + "/" + "/".join(output_path.split("/")[-2:])
            self.task.ssh_session.run_sftp('get', remote_path, output_path)

    def _prepare_runscript(self, job_wrapper, raw_script):
        script = re.sub(self.config.get('tool_dependency_dir') + "/_conda/envs", self.conda_MN4, raw_script)
        script = "source " + self.conda_MN4 + "/etc/profile.d/conda.sh\n" + script
        # REPLACE LOCAL GALAXY CONDA PATH WITH MN4 CONDA PATH
        script = re.sub(self.config.get('tool_dependency_dir') + "/_conda", self.conda_MN4, script)
        # REPLACE GALAXY TOOLS PATH WITH MN4 TOOLS PATH
        script = re.sub(self.config.get('tool_path'), self.tool_path, script)
        # REPLACE LOCAL GALAXY DATASETS PATH WITH MN4 DATASETS PATH
        script = re.sub(self.config.get('file_path'), self.file_path, script)
        script = re.sub(self.config.get('job_working_directory'), self.jobs_MN4, script)
        return script
