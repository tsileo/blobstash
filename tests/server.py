from subprocess import Popen
import time
import shutil
import os

PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


class BlobStash(object):
    def __init__(self, rebuild=True):
        self.process = None

    def run(self):
        """Execute `blobsfs-mount {fs_name} {fs_name}` and return the running process."""
        self.process = Popen(['blobstash', '--loglevel', 'error', './tests/blobstash.yaml'], env=os.environ)
        time.sleep(1)
        if self.process.poll():
            raise Exception('failed to mount')

    def cleanup(self):
        """Cleanup func."""
        try:
            shutil.rmtree('blobstash_data')
        except:
            pass

    def shutdown(self):
        if self.process:
            self.process.terminate()
            self.process.wait()
