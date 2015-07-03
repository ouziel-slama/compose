import json
import hashlib


def json_hash(obj):
    dump = json.dumps(obj, sort_keys=True, separators=(',', ':'))
    h = hashlib.sha256()
    h.update(dump.encode('utf8'))
    return h.hexdigest()
