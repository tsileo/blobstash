from tests.server import BlobStash
import time

b = BlobStash()
b.run()

time.sleep(3)
b.shutdown()
print('success')
