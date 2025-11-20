import asyncio
import seamless.config

seamless.config.init()

from seamless import Buffer, Checksum


async def main(content, sleep):
    buf = Buffer(content)
    await buf.get_checksum_async()  # ensure checksum is known
    buf.incref()  # triggers buffer_writer.register/init while loop runs
    print(buf.checksum)
    if sleep:
        await asyncio.sleep(3)  # keep loop alive so worker can process


asyncio.run(main(b"test buffer 1", False))
asyncio.run(main(b"test buffer 2", False))
asyncio.run(main(b"test buffer 3", True))
