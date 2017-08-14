import logging
import os

from blobstash.docstore import DocStoreClient
from blobstash.docstore import Q
from blobstash.base.test_utils import BlobStash


logging_log_level = logging.INFO
log_level = 'error'
if os.getenv('BLOBSTASH_DEBUG'):
    logging_log_level = logging.DEBUG
    log_level = 'debug'


logging.basicConfig(level=logging_log_level)
logging.info('Running integration tests...')

b = BlobStash(config='tests/blobstash.yaml')
b.cleanup()
client = DocStoreClient(api_key='123')
logging.info('Start BlobStash')
b.run(log_level=log_level)

col1 = client.col1

for i in range(10):
    col1.insert({'lol': i+1})

col2 = client.col2

COL = 'hello'
DOCS_COUNT = 1000
docs = []
for i in range(DOCS_COUNT):
    doc = dict(hello=i)
    resp = col2.insert(doc)
    docs.append(doc)

for doc in docs:
    rdoc = col2.get_by_id(doc['_id'])
    assert rdoc == doc

rdocs = []
for rdoc in col2.query():
    rdocs.append(rdoc)

assert rdocs == docs[::-1]

col3 = client.col3

for i in range(50):
    col3.insert({'i': i, 'nested': {'i': i}, 'l': [True, i]})

assert len(list(col3.query(Q['nested']['i'] >= 25))) == 25

assert sorted(['col1', 'col2', 'col3']) == sorted([c.name for c in client.collections()])

# Shutdown BlobStash
b.shutdown()
logging.info('Success \o/')
