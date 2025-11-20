import asyncio
import seamless.config

seamless.config.init()

from seamless import Buffer, Checksum

b = Buffer("test", "str")
asyncio.run(b.write())
b.checksum.save("test")
