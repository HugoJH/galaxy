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
        self.embed_metadata_in_job = asbool(job_wrapper.job_destination.params.get("embed_metadata_in_job", DEFAULT_EMBED_METADATA_IN_JOB))
        if not self.prepare_job(job_wrapper, include_metadata=self.embed_metadata_in_job):
            return

        ajs = AsynchronousJobState(files_dir=job_wrapper.working_directory,
                                   job_wrapper=job_wrapper,
                                   job_destination=job_wrapper.job_destination)

        script = self._get_raw_script(job_wrapper)
        print("-------SENDING TOOL TO MN4----------------")
        self._send_tools_to_MN4(job_wrapper, script)
        print("-------SENDING MUTABLE DATA TO MN4--------")
        self._send_mutable_data_to_MN4(job_wrapper)
        print("-------PREPARING TOOL SCRIPT-------------------")
        self._send_tool_script_to_MN4(job_wrapper)
        print("-------SENDING INPUTS TO MN4--------------")
        self._send_inputs_to_MN4(job_wrapper)
        print("-------SENDING METADATA INPUTS TO MN4-----")
        self._send_metadata_files_to_MN4(job_wrapper)
        print("-------SENDING DATATYPE SCRIPTS TO MN4----")
        self._send_datatypes_scripts_to_MN4(job_wrapper)
        print("-------CREATING REMOTE FOLDERS------------")
        self._compute_remote_output_paths_dirs(script)
        
        #self.task.ssh_session.run_command("mkdir -p " + self.metadata_files_path)
        # job was deleted while we were preparing it
        if job_wrapper.get_state() in (model.Job.states.DELETED, model.Job.states.STOPPED):
            log.debug("(%s) Job deleted/stopped by user before it entered the queue", job_wrapper.get_id_tag())
            if job_wrapper.cleanup_job in ("always", "onsuccess"):
                job_wrapper.cleanup()
            return

        self.task.set_remote_base_path(self._get_remote_working_dir(job_wrapper))

        wrapper_script = self.get_job_file(job_wrapper, exit_code_path=ajs.exit_code_file, shell=job_wrapper.shell)
        wrapper_script = re.split("cd working; /bin/bash", wrapper_script)[1].split('\n')[0]
        prepared_wrapper_script = self._prepare_script(job_wrapper,wrapper_script)
        print(prepared_wrapper_script)

        with open(ajs.job_file, "w") as f:
            f.write(prepared_wrapper_script) 
        print("-------SUBMITTING TASK-------------------")
        job_id = self.task.submit(queue_settings="debug",
                                  job_name=self._job_name(job_wrapper),
                                  local_run_script=ajs.job_file)
        self._save_task(job_wrapper)
        job_wrapper.set_external_id(job_id)
        ajs.job_id = job_id
        self.monitor_queue.put(ajs)

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

        if "RUNNING" in job_status:
            job_state.running = True
            state = model.Job.states.RUNNING
        elif "COMPLETING" in job_status:
            job_state.running = False
            state = model.Job.states.STOPPING
        elif "COMPLETED" in job_status:
            job_state.running = False
            state = model.Job.states.OK
            self.create_log_files(job_state.output_file,
                                  job_state.exit_code_file,
                                  job_state.error_file,
                                  job_state.job_wrapper)
            self._get_outputs(job_state.job_wrapper)
            job_state.job_wrapper.change_state(state)
            if self.embed_metadata_in_job:
                self._handle_metadata_externally(job_state.job_wrapper)
             
            self.mark_as_finished(job_state)
        elif "FAILED" in job_status:
            job_state.running = False
            state = model.Job.states.FAILED
            self.create_log_files(job_state.output_file,
                                  job_state.exit_code_file,
                                  job_state.error_file,
                                  job_state.job_wrapper)
            self.mark_as_failed(job_state)
        elif "CANCELLED+" in job_status:
            job_state.running = False
            state = model.Job.states.DELETED

        job_state.job_wrapper.change_state(state)
        if "COMPLETED" in job_status or "FAILED" in job_status:
            return None
        return job_state

    def stop_job(self, job_wrapper):
        """Attempts to delete a job from the DRM queue"""
        self.task.cancel()
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
        self.file_path_MN4 = os.path.dirname(self.jobs_MN4) + "/files"
        self.datatypes_MN4 = os.path.dirname(self.jobs_MN4) + "/datatypes"
        self.metadata_files_path = self.file_path_MN4 + "/_metadata_files/"
        self.tool_path = os.path.dirname(self.jobs_MN4) + "/tools"
        self.mutable_data_path = os.path.dirname(self.jobs_MN4) + "/mutable_data"
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
        self.task.set_local_data_bundle(local_data_path=job_wrapper.working_directory)
        self.task.send_input_data(self._get_remote_working_dir(job_wrapper))
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
        with open(output_file, 'w') as f:
            f.write("")
        with open(exit_code_file, 'w') as f:
            f.write("1")
        with open(error_file, 'w') as f:
            f.write("Connection to Marenostrum failed.Please copy the public key in the authorized host file\n\n")
            f.write("Copy the following key to the authorized_keys file in your Marenostrum account:\n\n ")
            f.write(public_key)

    def _send_conda_envs(self, script):
        conda_envs = [x for x in re.split("'| |\n", script) if "_conda/envs/" in x]
        for conda_env in conda_envs:
            self._send_conda_env(conda_env, self.conda_MN4 + "/" + os.path.basename(conda_env))


    def _send_conda_env(self, conda_env_path, remote_env_path):
        if not os.path.isfile(conda_env_path + ".tar.gz"):
            conda_pack.CondaEnv.from_prefix(conda_env_path).pack(output=conda_env_path + ".tar.gz", verbose=True)

        if self.local_checksum(conda_env_path + ".tar.gz") != self.remote_checksum(remote_env_path + ".tar.gz"):
            self.task.set_local_file(local_data_filepath=conda_env_path + ".tar.gz")
            self.task.send_input_data(self.conda_MN4, overwrite=False)
            self.task.ssh_session.run_command("mkdir -p " + remote_env_path)
            self.task.ssh_session.run_command("tar --skip-old-files -z -x  -v -f " + remote_env_path + ".tar.gz" +
                                              " -C " + remote_env_path)
    def _send_mutable_data_to_MN4(self, job_wrapper):
        mutable_data_paths = self._get_path_from_script(job_wrapper, "mutable_data/shed_tools/")
        for path in mutable_data_paths:
            remote_path = re.sub(os.path.dirname(self.config.get('tool_data_path')), self.mutable_data_path, path)
            self.task.ssh_session.run_command("mkdir -p " + os.path.dirname(remote_path))
            self.task.set_local_file(local_data_filepath=path)
            self.task.send_input_data(os.path.dirname(remote_path), overwrite=False)

    def _send_datatypes_scripts_to_MN4(self, job_wrapper):
        datatypes_paths = self._get_path_from_script(job_wrapper, "galaxy/datatypes/")
        for path in datatypes_paths:
            remote_path = self.datatypes_MN4 + '/' + re.split("/datatypes/", path)[1]
            self.task.ssh_session.run_command("mkdir -p " + os.path.dirname(remote_path))
            self.task.set_local_file(local_data_filepath=path)
            self.task.send_input_data(os.path.dirname(remote_path), overwrite=False)


    def create_log_files(self, output_file, exit_code_file, error_file, job_wrapper):
        self.task.get_output_data(job_wrapper.working_directory)
        output_files = self.task.task_data['output_data_bundle'].files
        remote_output = job_wrapper.working_directory + '/' + [out for out in output_files if ".o" in out][0]
        remote_error =  job_wrapper.working_directory + '/' + [err for err in output_files if ".e" in err][0]
        os.rename(remote_error, error_file)
        os.rename(remote_output, output_file)

    def _get_path_from_script(self, job_wrapper, pattern):
        paths = [x for x in re.split("'| |\n", self._get_raw_script(job_wrapper)) if pattern in x]
        return paths

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

    def _send_tool_script_to_MN4(self, job_wrapper):
        prepared_script = self._prepare_script(job_wrapper, self._get_raw_script(job_wrapper))
        prepared_script = "cd working; bash " + prepared_script
        with open(job_wrapper.working_directory + "/tool_script.sh", "w") as f:
            f.write(prepared_script) 
        self.task.set_local_file(local_data_filepath=job_wrapper.working_directory + "/tool_script.sh")
        self.task.send_input_data(self._get_remote_working_dir(job_wrapper))


    def _send_inputs_to_MN4(self, job_wrapper):
        for path in job_wrapper.get_input_paths():
            input_path = path.real_path
            basepath = re.sub(self.config.get('file_path'), "", input_path)
            self.task.ssh_session.run_command("mkdir -p " + self.file_path_MN4 + os.path.dirname(basepath))
            self.task.set_local_file(local_data_filepath=input_path)
            self.task.send_input_data(self.file_path_MN4 + "/" + os.path.dirname(basepath))

    def _send_metadata_files_to_MN4(self, job_wrapper):
        metadata_paths = [x for x in re.split("'| |\n", self._get_raw_script(job_wrapper)) if "_metadata_files" in x]
        for path in metadata_paths:
            print(path)
            remote_path = re.sub(self.config.get('file_path') + "/_metadata_files", self.metadata_files_path, path)
            self.task.set_local_file(local_data_filepath=path)
            self.task.send_input_data(os.path.dirname(remote_path))

    def _send_default_tool(self, tool_path, job_wrapper):
        self.task.set_local_file(local_data_filepath=tool_path)
        self.task.send_input_data(remote_base_path=self.tool_path + "/" + os.path.basename(job_wrapper.tool.tool_dir))

    def _send_tools_to_MN4(self, job_wrapper, script):
        # ENVIAR ENTORNO CONDA A MN4
        if self.config.get('tool_path') in job_wrapper.tool.tool_dir:
            try:
                tool_full_path = [x for x in re.split("'| |\n", script) if job_wrapper.tool.tool_dir in x][0]
                self._send_default_tool(tool_full_path, job_wrapper)
            except IndexError:
                print("Builtin tool in script")
        self._send_conda_envs(script)

    def _get_raw_script(self, job_wrapper):
        with open(os.path.join(job_wrapper.working_directory, "tool_script.sh"), "r") as f:
            script = f.read()
        return script
    
    def _get_remote_working_dir(self, job_wrapper):
        id_path ="/".join(job_wrapper.working_directory.split("/")[-2:])
        return self.jobs_MN4 + "/" + id_path

    def _get_outputs(self, job_wrapper):
        script = self._get_raw_script(job_wrapper)
        outputs = [x for x in re.split("'| |\n", script) if "/files/" in x]
        print("--------------------OUTPUTS-----------------")
        print(outputs)
        print("--------------------OUTPUTS-----------------")
        for output_path in outputs:
            remote_path = re.sub(self.config.get('file_path'), self.file_path_MN4, output_path)
            print(f"getting output {remote_path} => {output_path}")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            self.task.ssh_session.run_sftp('get', remote_path, output_path)
        if self._remote_file_exists(self._get_remote_working_dir(job_wrapper) + "/outfile"): 
            self.task.ssh_session.run_sftp('get', self._get_remote_working_dir(job_wrapper) + "/outfile",  job_wrapper.working_directory + "/working/outfile")
            os.rename(job_wrapper.working_directory + "/working/outfile", job_wrapper.compute_outputs()[0].real_path)
    
    def _remote_file_exists(self, remote_path):
        try:
            print(self.task.ssh_session.run_sftp('stat', remote_path))
            return (self.task.ssh_session.run_sftp('stat', remote_path) != False)
        except IOError:
            print(remote_path, " not found")

    def _compute_remote_output_paths_dirs(self, script):
        output_paths = [x for x in re.split("'| |\n", script) if "/files/" in x]
        for path in output_paths:
            remote_path = os.path.dirname(re.sub(self.config.get('file_path'), self.file_path_MN4, path))
            self.task.ssh_session.run_command("mkdir -p " + remote_path)

    def _get_datatypes_base_path(self, job_wrapper):
        path = ""
        datatypes_paths = self._get_path_from_script(job_wrapper, "/galaxy/datatypes/")
        if datatypes_paths:
            path = re.split("datatypes", datatypes_paths[0])[0] + "datatypes"
        return path

    def _prepare_script(self, job_wrapper, raw_script):
        #script = "cd " + self._get_remote_working_dir(job_wrapper) + "\n"
        #script = script + "export GALAXY_SLOTS=40\n" + raw_script
        script = re.sub(self.config.get('tool_dependency_dir') + "/_conda/envs", self.conda_MN4, raw_script)
        script = "source " + self.conda_MN4 + "/etc/profile.d/conda.sh\n" + script
        # REPLACE LOCAL GALAXY CONDA PATH WITH MN4 CONDA PATH
        script = re.sub(self.config.get('tool_dependency_dir') + "/_conda", self.conda_MN4, script)
        # REPLACE GALAXY TOOLS PATH WITH MN4 TOOLS PATH
        script = re.sub(self.config.get('tool_path'), self.tool_path, script)
        # REPLACE LOCAL GALAXY DATASETS PATH WITH MN4 DATASETS PATH
        script = re.sub(self.config.get('file_path') + "/_metadata_files", self.metadata_files_path, script)
        script = re.sub(self.config.get('file_path'), self.file_path_MN4, script)
        script = re.sub(self.config.get('job_working_directory'), self.jobs_MN4, script)
        script = re.sub(os.path.dirname(self.config.get('tool_data_path')), self.mutable_data_path, script)
        datatypes_path = self._get_datatypes_base_path(job_wrapper)
        if datatypes_path:
            script = re.sub(datatypes_path, self.datatypes_MN4, script)
        return script
