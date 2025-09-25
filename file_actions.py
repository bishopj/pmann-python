import os, shutil, subprocess
from pathlib import Path

def open_folder(path: Path):
    subprocess.Popen(f'explorer /select,"{path}"')

def open_file(path: Path):
    os.startfile(path)

def move_to_bucket(path: Path, bucket: Path):
    shutil.move(str(path), str(bucket))

def delete_file(path: Path):
    os.remove(path)

def show_properties(path: Path):
    subprocess.Popen(['cmd', '/c', 'start', 'shell32.dll,Control_RunDLL', str(path)])
