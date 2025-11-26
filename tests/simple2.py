import seamless
import seamless.config

seamless.config.init()

from seamless import Buffer, Checksum

c = Checksum.load("test.CHECKSUM")
b = c.resolve()
assert isinstance(b, Buffer)
print(b, b.content, b.get_value("str"))

seamless.close()
