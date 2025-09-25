import ctypes
from ctypes import wintypes
from pathlib import Path

def open_with_dialog(path: Path):
    """Invoke the windows recommended app list for openning a file

    Args:
        path (Path): file to open
    """
    SEE_MASK_INVOKEIDLIST = 0x0000000C
    ShellExecuteEx = ctypes.windll.shell32.ShellExecuteExW

    class SHELLEXECUTEINFO(ctypes.Structure):
        _fields_ = [
            ("cbSize", wintypes.DWORD),
            ("fMask", wintypes.ULONG),
            ("hwnd", wintypes.HWND),
            ("lpVerb", wintypes.LPCWSTR),
            ("lpFile", wintypes.LPCWSTR),
            ("lpParameters", wintypes.LPCWSTR),
            ("lpDirectory", wintypes.LPCWSTR),
            ("nShow", ctypes.c_int),
            ("hInstApp", wintypes.HINSTANCE),
            ("lpIDList", wintypes.LPVOID),
            ("lpClass", wintypes.LPCWSTR),
            ("hkeyClass", wintypes.HKEY),
            ("dwHotKey", wintypes.DWORD),
            ("hIcon", wintypes.HANDLE),
            ("hProcess", wintypes.HANDLE),
        ]

    sei = SHELLEXECUTEINFO()
    sei.cbSize = ctypes.sizeof(sei)
    sei.fMask = SEE_MASK_INVOKEIDLIST
    sei.hwnd = None
    sei.lpVerb = "openas"
    sei.lpFile = str(path)
    sei.lpParameters = None
    sei.lpDirectory = None
    sei.nShow = 1  # SW_SHOWNORMAL

    success = ShellExecuteEx(ctypes.byref(sei))
    return bool(success and sei.hProcess)
