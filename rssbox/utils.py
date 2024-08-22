import hashlib
import os
import shutil


def delete_file(*files):
    for file in files:
        if os.path.exists(file):
            if os.path.isdir(file):
                shutil.rmtree(file)
            else:
                os.remove(file)


def md5hash(name: str) -> str:
    h = hashlib.md5()
    h.update(name.encode("utf-8"))
    return h.hexdigest()