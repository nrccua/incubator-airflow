# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os

from airflow import configuration
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.log.file_task_handler import FileTaskHandler


class GCSTaskHandler(FileTaskHandler, LoggingMixin):
    """
    GCSTaskHandler is a python log handler that handles and reads
    task instance logs. It extends airflow FileTaskHandler and
    uploads to and reads from GCS remote storage. Upon log reading
    failure, it reads from host machine's local disk.
    """
    def __init__(self, base_log_folder, gcs_log_folder, filename_template):
        super(GCSTaskHandler, self).__init__(base_log_folder, filename_template)
        self.remote_base = gcs_log_folder
        self.log_relative_path = ''
        self._hook = None
        self.closed = False

    def _build_hook(self):
        remote_conn_id = configuration.get('core', 'REMOTE_LOG_CONN_ID')
        try:
            from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
            return GoogleCloudStorageHook(
                google_cloud_storage_conn_id=remote_conn_id
            )
        except:
            self.log.error(
                'Could not create a GoogleCloudStorageHook with connection id '
                '"%s". Please make sure that airflow[gcp_api] is installed '
                'and the GCS connection exists.', remote_conn_id
            )

    @property
    def hook(self):
        if self._hook is None:
            self._hook = self._build_hook()
        return self._hook

    def set_context(self, ti):
        super(GCSTaskHandler, self).set_context(ti)
        # Log relative path is used to construct local and remote
        # log path to upload log files into GCS and read from the
        # remote location.
        self.log_relative_path = self._render_filename(ti, ti.try_number + 1)

    def close(self):
        """
        Close and upload local log file to remote storage S3.
        """
        # When application exit, system shuts down all handlers by
        # calling close method. Here we check if logger is already
        # closed to prevent uploading the log to remote storage multiple
        # times when `logging.shutdown` is called.
        if self.closed:
            return

        super(GCSTaskHandler, self).close()

        local_loc = os.path.join(self.local_base, self.log_relative_path)
        remote_loc = os.path.join(self.remote_base, self.log_relative_path)
        if os.path.exists(local_loc):
            # read log and remove old logs to get just the latest additions
            with open(local_loc, 'r') as logfile:
                log = logfile.read()
            self.gcs_write(log, remote_loc)

        # Mark closed so we don't double write if close is called twice
        self.closed = True

    def _read(self, ti, try_number):
        """
        Read logs of given task instance and try_number from GCS.
        If failed, read the log from task instance host machine.
        :param ti: task instance object
        :param try_number: task instance try_number to read logs from
        """
        # Explicitly getting log relative path is necessary as the given
        # task instance might be different than task instance passed in
        # in set_context method.
        log_relative_path = self._render_filename(ti, try_number + 1)
        remote_loc = os.path.join(self.remote_base, log_relative_path)

        log = 'HERE I AM IN GCSTaskHandler'
        log += 'remote loc is: %s' % remote_loc

        if self.gcs_log_exists(remote_loc):
            log += 'remote log exists'
            # If GCS remote file exists, we do not fetch logs from task instance
            # local machine even if there are errors reading remote logs, as
            # remote_log will contain error message.
            remote_log = self.gcs_read(remote_loc, return_error=True)
            log = '*** Reading remote log from {}.\n{}\n'.format(
                remote_loc, remote_log)
        else:
            log += 'remote log doesnt exist'
            # log = super(GCSTaskHandler, self)._read(ti, try_number)

        return log

    def read(self, task_instance, try_number=None):
        """
        Read logs of given task instance from local machine.
        :param task_instance: task instance object
        :param try_number: task instance try_number to read logs from. If None
                           it returns all logs separated by try_number
        :return: a list of logs
        """
        # Task instance increments its try number when it starts to run.
        # So the log for a particular task try will only show up when
        # try number gets incremented in DB, i.e logs produced the time
        # after cli run and before try_number + 1 in DB will not be displayed.
        logging.info('LOGGING FOR JESSICA. IN GCS TASK HANDLER.')
        next_try = task_instance.try_number

        if try_number is None:
            try_numbers = list(range(next_try))
        elif try_number < 0:
            logs = ['Error fetching the logs. Try number {} is invalid.'.format(try_number)]
            return logs
        else:
            try_numbers = [try_number]

        logs = [''] * len(try_numbers)
        for i, try_number in enumerate(try_numbers):
            logs[i] += self._read(task_instance, try_number)

        return logs

    def gcs_log_exists(self, remote_log_location):
        """
        Check if remote_log_location exists in remote storage
        :param remote_log_location: log's location in remote storage
        :return: True if location exists else False
        """
        try:
            bkt, blob = self.parse_gcs_url(remote_log_location)
            return self.hook.exists(bkt, blob)
        except Exception:
            pass
        return False

    def gcs_read(self, remote_log_location, return_error=False):
        """
        Returns the log found at the remote_log_location.
        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: string (path)
        :param return_error: if True, returns a string error message if an
            error occurs. Otherwise returns '' when an error occurs.
        :type return_error: bool
        """
        try:
            bkt, blob = self.parse_gcs_url(remote_log_location)
            return self.hook.download(bkt, blob).decode()
        except:
            # return error if needed
            if return_error:
                msg = 'Could not read logs from {}'.format(remote_log_location)
                self.log.error(msg)
                return msg

    def gcs_write(self, log, remote_log_location, append=True):
        """
        Writes the log to the remote_log_location. Fails silently if no hook
        was created.
        :param log: the log to write to the remote_log_location
        :type log: string
        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: string (path)
        :param append: if False, any existing log file is overwritten. If True,
            the new log is appended to any existing logs.
        :type append: bool
        """
        if append:
            old_log = self.gcs_read(remote_log_location)
            log = '\n'.join([old_log, log]) if old_log else log

        try:
            bkt, blob = self.parse_gcs_url(remote_log_location)
            from tempfile import NamedTemporaryFile
            with NamedTemporaryFile(mode='w+') as tmpfile:
                tmpfile.write(log)
                # Force the file to be flushed, since we're doing the
                # upload from within the file context (it hasn't been
                # closed).
                tmpfile.flush()
                self.hook.upload(bkt, blob, tmpfile.name)
        except Exception as e:
            self.log.error('Could not write logs to %s: %s', remote_log_location, e)

    def parse_gcs_url(self, gsurl):
        """
        Given a Google Cloud Storage URL (gs://<bucket>/<blob>), returns a
        tuple containing the corresponding bucket and blob.
        """
        # Python 3
        try:
            from urllib.parse import urlparse
        # Python 2
        except ImportError:
            from urlparse import urlparse

        parsed_url = urlparse(gsurl)
        if not parsed_url.netloc:
            raise AirflowException('Please provide a bucket name')
        else:
            bucket = parsed_url.netloc
            blob = parsed_url.path.strip('/')
            return bucket, blob
