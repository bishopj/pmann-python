from PySide6.QtCore import QObject, QRunnable, Signal, Slot
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import hashlib
import os
import threading

class ScannerSignals(QObject):
    progress = Signal(str)               # Emit file path
    finished = Signal(dict)              # Emit final result
    error = Signal(str)                  # Emit error message
    cancelled = Signal()                 # Emit if cancelled

class ScannerWorker(QRunnable):
    def __init__(self, root_path: str, cancel_flag, dupe_only : bool, max_workers: int = 8):
        super().__init__()
        self.root_path = Path(root_path)
        self.max_workers = max_workers
        self.signals = ScannerSignals()
        self.cancel_flag = cancel_flag
        self.dupe_only = dupe_only
        self.fdict = {}

    def compute_hash(self, path: Path, chunk_size: int = 65536) -> str:
        hasher = hashlib.sha256()
        with path.open("rb") as f:
            while chunk := f.read(chunk_size):
                hasher.update(chunk)
        return hasher.hexdigest()

    def hash_file(self, path):
        hash = self.compute_hash(path)
        return (path, hash)

    """
    @Slot()
    def run8(self):
        try:
            futures = []
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                for root, dirs, files in os.walk(self.root_path):
                    for file in files:
                        if self.cancel_flag.is_set():
                            self.signals.cancelled.emit()
                            return
                        path = Path(root) / file
                        future = executor.submit(self.compute_hash, path)
                        futures.append((future, path))

                for future, path in futures:
                    if self.cancel_flag.is_set():
                        self.signals.cancelled.emit()
                        return
                    try:
                        hash_val = future.result()
                        self.fdict.setdefault(hash_val, []).append(path)
                        self.signals.progress.emit(str(path))
                    except Exception as e:
                        self.signals.error.emit(f"{path}: {e}")

            if self.dupe_only:
                duplicates = {k: v for k, v in self.fdict.items() if len(v) > 1}
                self.signals.finished.emit(duplicates)
            else:
                self.signals.finished.emit(self.fdict)

        except Exception as e:
            self.signals.error.emit(str(e))
    """
    
    @Slot()
    def run(self):
        try:
            futures = []
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                for root, dirs, files in os.walk(self.root_path):
                    for file in files:
                        if self.cancel_flag.is_set():
                            self.signals.cancelled.emit()
                            return
                        futures.append(executor.submit(self.hash_file, Path(root) / file))

                for future in as_completed(futures):
                    if self.cancel_flag.is_set():
                        self.signals.cancelled.emit()
                        return
                    try:
                        path, hash_val = future.result()
                        self.fdict.setdefault(hash_val, []).append(path)
                        self.signals.progress.emit(str(path))
                    except Exception as e:
                        self.signals.error.emit(f"{path}: {e}")

            if self.dupe_only:
                duplicates = {k: v for k, v in self.fdict.items() if len(v) > 1}
                self.signals.finished.emit(duplicates)
            else:
                self.signals.finished.emit(self.fdict)
                
        except Exception as e:
            self.signals.error.emit(str(e))
