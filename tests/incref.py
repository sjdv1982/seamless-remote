import asyncio
import seamless.config

seamless.config.init()

from seamless import Buffer, Checksum

buf = Buffer(b"This is my buffer")
buf.incref()
print(buf.checksum)

asyncio.run(asyncio.sleep(3))
