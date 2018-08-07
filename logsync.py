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


def get_fname2date(fnames):
    ret = {}
    for fn in fnames:
        date = fn.lstrip('1c-catalog-').rstrip('.log.gz')
        ret[fn] = datetime.strptime(date, "%Y-%m-%d").date()
    return ret


def sync(localpath):
    transport = paramiko.Transport((host, port))
    transport.connect(username=user, password=secret)
    sftp = paramiko.SFTPClient.from_transport(transport)

    remotepath = '/logstash'

    local_files = glob(localpath + '/*.log')
    local_files = {os.path.split(f)[1] + '.gz' for f in local_files}
    if local_files:
        max_local_date = max(get_fname2date(local_files).values())
    else:
        max_local_date = datetime.strptime('0001-01-01', "%Y-%m-%d").date()

    cur_date = datetime.now().date()
    remote_files = set(sftp.listdir('logstash'))
    fname2date = get_fname2date(remote_files - local_files)
    diff = [fn for fn, date in fname2date.items() if
            max_local_date < date < cur_date]
    for fname in diff:
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
    #1133

if __name__ == '__main__':
    sync(localpath)
