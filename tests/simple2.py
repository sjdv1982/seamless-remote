import seamless.config

seamless.config.init()

from seamless import Buffer, Checksum

c = Checksum.load("test.CHECKSUM")
b = c.resolve()
print(b, b.content, b.get_value("str"))
