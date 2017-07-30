import logging

from tests.client import Blob
from tests.client import Client
from tests.server import BlobStash

MORE_BLOBS = 999

logging.basicConfig(level=logging.DEBUG)
logging.info('Running integration tests...')

b = BlobStash()
b.cleanup()
c = Client()

logging.info('Start BlobStash')
b.run()

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
assert blob_ref['Hash'] == blob2.hash, 'failed to enumate blobs, hash does not match, expected {} got {}'.format(
    blob_ref['Hash'], blob2.hash
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

keys = {}
for x in range(10):
    key = 'k{}'.format(x)
    if key not in keys:
        keys[key] = []
    for y in range(200):
        val = 'value.{}.{}'.format(x, y)
        kv = c.put_kv('k{}'.format(x), val, version=y+1)
        keys[key].append(kv)

for key in keys.keys():
    kv = c.get_kv(key)
    assert kv == keys[key][-1]
    versions = c.get_kv_versions(key)
    for i, version in enumerate(versions['versions']):
        assert version == keys[key][200-(1+i)]


# TODO(tsileo): recount the number of blob, check meta blob, check restart, check index rebuild

# Shutdown BlobStash
b.shutdown()
logging.info('Success \o/')
