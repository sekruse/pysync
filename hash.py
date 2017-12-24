import hashlib

def sha256(path):
    m = hashlib.sha256()
    with open(path, 'rb') as f:
        while True:
            data = f.read(4096)
            if not data: break
            m.update(data)
    return m.hexdigest()
