import os
from pathlib import Path
import hashlib

class DuplicateScanner:
    def __init__(self, root_path):
        self.root_path = Path(root_path)
        self.fdict = {}

    def compute_hash(self,  path, chunk_size=65536):
        sha = hashlib.sha256()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(chunk_size), b''):
                sha.update(chunk)
        return sha.hexdigest()

    def scan(self):
        for root, dirs, files in os.walk(self.root_path):
            for file in files:
                p = Path(root) / file
                try:
                    hp = self.compute_hash(p)
                    self.fdict.setdefault(hp, []).append(p)
                except Exception as e:
                    print(f"Error hashing {p}: {e}")
        return {k: v for k, v in self.fdict.items() if len(v) > 1}
