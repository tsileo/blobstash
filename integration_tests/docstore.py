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


def clean_doc(doc):
    new_doc = doc.copy()
    for k in doc.keys():
        if k.startswith('_'):
            del new_doc[k]
    return new_doc

COL = 'hello'
DOCS_COUNT = 1000
docs = []
for i in range(DOCS_COUNT):
    doc = dict(hello=i)
    resp = c.put_doc(COL, doc)
    doc.update(resp)
    docs.append(doc)

for doc in docs:
    rdoc = c.get_doc(COL, doc['_id'])['data']
    assert clean_doc(doc) == clean_doc(rdoc)

rdocs = []
cursor = ''
while 1:
    resp = c.get_docs(COL, cursor=cursor)
    rdocs.extend(resp['data'])
    pagination = resp['pagination']
    if not pagination['has_more']:
        break
    cursor = pagination['cursor']

for i, rdoc in enumerate(rdocs):
    assert clean_doc(rdoc) == clean_doc(docs[DOCS_COUNT-(1+i)])

# Shutdown BlobStash
b.shutdown()
logging.info('Success \o/')
