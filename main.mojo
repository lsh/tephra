from python import Python
from tephra import Value, Cid
from tephra.utils import Reader
from tephra.car import CarReader
import tephra.varint
from utils import Span
from collections import Dict


def main():
    client = Python.import_module("websockets.sync.client")
    # requests = Python.import_module("requests")
    connect = client.connect
    websocket = connect(
        "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"
    )
    for msg in websocket:
        buf = List[UInt8](capacity=len(msg))
        # BAD
        for b in msg:
            buf.append(int(b))
        r = Reader(buf)
        head = Value.decode(r)
        if head.dict()["t"].string() == "#commit":
            body = Value.decode(r)
            b = Pointer.address_of(body.dict())
            block_bytes = b[]["blocks"].bytes()
            block_reader = Reader(block_bytes)
            cr = CarReader(block_reader)
            nodes = Dict[Cid, List[Byte]]()
            while True:
                try:
                    cid, bytes = cr.next_block()
                    nodes[cid] = bytes
                except:
                    break

            did = b[]["repo"].string()

            # to get author name
            # not doing it because it's slow
            # resp = requests.get("https://plc.directory/" + did)
            # author = resp.json()["alsoKnownAs"][0]

            for op in b[]["ops"].list():
                if (
                    op[]
                    .dict()["path"]
                    .string()
                    .startswith("app.bsky.feed.post/")
                    and op[].dict()["action"].string() == "create"
                ):
                    bytes = nodes[op[].dict()["cid"].cid()]
                    br = Reader(bytes)
                    val = Value.decode(br)
                    print(did, ":", val.dict()["text"])
