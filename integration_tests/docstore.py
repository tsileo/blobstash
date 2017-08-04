import logging
import os
import time

import sys

p = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, p)

from tests.client import Client
from tests.server import BlobStash

MORE_BLOBS = 999

logging_log_level = logging.INFO
log_level = 'error'
if os.getenv('BLOBSTASH_DEBUG'):
    logging_log_level = logging.DEBUG
    log_level = 'debug'


logging.basicConfig(level=logging_log_level)
logging.info('Running integration tests...')

b = BlobStash()
b.cleanup()
c = Client()
logging.info('Start BlobStash')
b.run(log_level=log_level)

# Shutdown BlobStash
b.shutdown()
logging.info('Success \o/')
