import asyncio
import seamless.config

seamless.config.init()

from seamless import Buffer, Checksum

buf = Buffer(b"This is my buffer")
buf.incref()
print(buf.checksum)

loop = asyncio.get_event_loop()
loop.run_until_complete(asyncio.sleep(3))
