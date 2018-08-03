import os
import paramiko
import time
from datetime import datetime
from glob import glob
import gzip
import shutil

host = 'catalog-api-logstash1-gpt-msk.int.1s.ru'
user = 'kkon'
secret = 'NdZV3SpiPm8apq8J4LTQ'
port = 22

localpath = '../data/logstash'
paramiko.util.log_to_file(os.path.join(localpath, 'paramiko.logs'))


def as_day(ts):
    return datetime.fromtimestamp(ts).strftime('%Y-%m-%d')


def sync(localpath):
    transport = paramiko.Transport((host, port))
    transport.connect(username=user, password=secret)
    sftp = paramiko.SFTPClient.from_transport(transport)

    remotepath = '/logstash'

    local_files = glob(localpath + '/*.log')
    local_files = {os.path.split(f)[1] + '.gz' for f in local_files}

    cur_date = as_day(time.time())
    remote_files = set(sftp.listdir('logstash'))
    for fname in remote_files - local_files:
        if cur_date in fname:
            print('passing current date log %s ' % fname)
            continue

        remote_name = os.path.join(remotepath, fname)
        local_name = os.path.join(localpath, fname)
        sftp.get(remote_name, local_name)

        fname_out = os.path.splitext(local_name)[0]
        try:
            with gzip.open(local_name, 'rb') as f_in:
                with open(fname_out, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            print(fname)
        except EOFError:
            print('file %s is corrupted' % local_name)
            try:
                os.remove(fname_out)
            except OSError:
                pass
            continue
        finally:
            os.remove(local_name)

    sftp.close()
    transport.close()


if __name__ == '__main__':
    sync(localpath)
