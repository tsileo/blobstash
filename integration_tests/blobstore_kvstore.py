import logging
import os
import time

import sys

p = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, p)

from tests.client import Blob
from tests.client import Client
from blobstash.base.test_utils import BlobStash

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

logging.info('[STEP 1] Testing the blob store...')
# FIXME(tsileo): only GET/POST at / and GET /{hash}

logging.info('Insert test blob')
blob = Blob.from_data(b'hello')
resp = c.put_blob(blob)
assert resp.status_code == 200, 'failed to put blob {}'.format(blob.hash)

logging.info('Fetch test blob back')
blob2 = c.get_blob(blob.hash, to_blob=True)
assert blob2.data == blob.data, 'failed to fetch blob {} != {}'.format(blob.data, blob2.data)

# TODO(tsileo): test 404 and malformed hash

logging.info('Enumerating blobs')
blobs_resp = c._get('/api/blobstore/blobs').json()
assert len(blobs_resp['refs']) == 1, 'failed to enumate blobs, expected 1 got {}'.format(len(blobs_resp['refs']))
blob_ref = blobs_resp['refs'][0]
assert blob_ref['hash'] == blob2.hash, 'failed to enumate blobs, hash does not match, expected {} got {}'.format(
    blob_ref['hash'], blob2.hash
)

logging.info('Now adding more blobs')
more_blobs = [blob]
for _ in range(MORE_BLOBS):
    current_blob = Blob.from_random()
    more_blobs.append(current_blob)
    resp = c.put_blob(current_blob)
    assert resp.status_code == 200, 'failed to put blob {}'.format(blob.hash)

logging.info('Restart BlobStash, and enumerate all %d the blobs', len(more_blobs))
b.shutdown()
b.run()

# TODO(tsileo):
# - test pagination (cursor), bad int, > 1000 error
blobs_resp = c._get('/api/blobstore/blobs?limit=1000').json()
assert len(blobs_resp['refs']) == len(more_blobs), 'failed to enumate blobs, expected {} got {}'.format(
    len(more_blobs),
    len(blobs_resp['refs']),
)

logging.info('Ensures we can read them all')
for blob in more_blobs:
    blob2 = c.get_blob(blob.hash, to_blob=True)
    assert blob2.data == blob.data, 'failed to fetch blob {} != {}'.format(blob.data, blob2.data)

logging.info('[STEP 2] Testing the key-value store')

KV_COUNT = 10
KV_VERSIONS_COUNT = 100

keys = {}
for x in range(KV_COUNT):
    key = 'k{}'.format(x)
    if key not in keys:
        keys[key] = []
    for y in range(KV_VERSIONS_COUNT):
        val = 'value.{}.{}'.format(x, y)
        kv = c.put_kv('k{}'.format(x), val, version=y+1)
        keys[key].append(kv)

for key in keys.keys():
    kv = c.get_kv(key)
    assert kv == keys[key][-1]
    versions = c.get_kv_versions(key)
    for i, version in enumerate(versions['versions']):
        assert version == keys[key][KV_VERSIONS_COUNT-(1+i)]

b.shutdown()
for f in [
    'blobstash_data/.80a3e998d3248e3f44c5c608fd8dc813e00567a3',
    'blobstash_data/.82481ffa006d3077c01fb135f375eaa25816881c',
    'blobstash_data/.e7ecafda402e922e0fcefb3741538bd152c35405',
    'blobstash_data/vkv',
]:
    os.unlink(f)


b.run(reindex=True, log_level=log_level)

time.sleep(2)

for key in keys.keys():
    kv = c.get_kv(key)
    assert kv == keys[key][-1]
    versions = c.get_kv_versions(key)
    assert len(versions['versions']) == KV_VERSIONS_COUNT
    for i, version in enumerate(versions['versions']):
        assert version == keys[key][KV_VERSIONS_COUNT-(1+i)]

keys_resp = c.get_kv_keys()
for key_resp in keys_resp['keys']:
    assert key_resp == keys[key_resp['key']][-1]


all_blobs = []

cursor = ''
while 1:
    resp = c._get('/api/blobstore/blobs?limit=100&start='+cursor).json()

    if len(resp['refs']) == 0 or not resp['cursor']:
        break

    cursor = resp['cursor']
    all_blobs.extend(resp['refs'])
    print(len(all_blobs))
    print(len(resp['refs']))

# Ensure one meta blob for each key-value has been created
assert len(all_blobs) == len(more_blobs) + KV_COUNT * KV_VERSIONS_COUNT

# Shutdown BlobStash
b.shutdown()
logging.info('Success \o/')
