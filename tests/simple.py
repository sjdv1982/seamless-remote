import asyncio
import seamless
import seamless.config

seamless.config.init()

from seamless import Buffer

b = Buffer("test", "str")
asyncio.run(b.write())
b.checksum.save("test")

seamless.close()
