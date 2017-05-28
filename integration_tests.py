import logging

from tests.client import Blob
from tests.client import Client
from tests.server import BlobStash

logging.basicConfig(level=logging.DEBUG)
logging.info('Running integration tests...')

b = BlobStash()
c = Client()

logging.info('Start BlobStash')
b.run()

logging.info('Insert test blob')
blob = Blob.from_data(b'hello')
resp = c.put_blob(blob)
assert resp.status_code == 200, 'failed to put blob {}'.format(blob)

logging.info('Fetch test blob back')
blob2 = c.get_blob(blob.hash, to_blob=True)
assert blob2.data == blob.data, 'failed to fetch blob {} != {}'.format(blob.data, blob2.data)

blobs_resp = c._get('/api/blobstore/blobs').json()
assert len(blobs_resp['refs']) == 1, 'failed to enumate blobs, expected 1 got {}'.format(len(blobs_resp['refs']))
blob_ref = blobs_resp['refs'][0]
assert blob_ref['Hash'] == blob2.hash, 'failed to enumate blobs, hash does not match, expected {} got {}'.format(
    blob_ref['Hash'], blob2.hash
)

# Shutdown BlobStash
b.shutdown()
logging.info('Success \o/')
