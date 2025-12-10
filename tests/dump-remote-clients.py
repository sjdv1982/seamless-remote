import sys, json
import seamless
import seamless.config
from seamless.config import collect_remote_clients

outfile = sys.argv[1]
seamless.config.init()
from seamless_config.select import get_current

cluster = get_current()[0]

x = collect_remote_clients(cluster)
with open(outfile, "w") as f:
    json.dump(x, f)

seamless.close()
