from hashlib import blake2b
from urllib.parse import urljoin
import os

import requests
BASE_URL = 'http://localhost:8050'
API_KEY = '123'


class Blob:
    def __init__(self, hash, data):
        self.hash = hash
        self.data = data

    @classmethod
    def from_data(cls, data):
        h = blake2b(digest_size=32)
        h.update(data)
        return cls(h.hexdigest(), data)

    @classmethod
    def from_random(cls, size=256):
        return cls.from_data(os.urandom(size))


class Client:
    def __init__(self, base_url=None, api_key=None):
        self.base_url = base_url or BASE_URL
        self.api_key = api_key or API_KEY

    def _get(self, path, params={}):
        r = requests.get(urljoin(self.base_url, path), auth=('', self.api_key), params={})
        return r

    def put_blob(self, blob):
        files = {blob.hash: blob.data}
        r = requests.post(urljoin(self.base_url, '/api/blobstore/upload'), auth=('', self.api_key), files=files)
        return r

    def get_blob(self, hash, to_blob=True):
        r = self._get('/api/blobstore/blob/{}'.format(hash))
        if not to_blob:
            return r
        r.raise_for_status()
        return Blob(hash, r.text.encode('utf-8'))
