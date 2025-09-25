from dataclasses import dataclass
from PySide6.QtCore import QRect

@dataclass
class FaceRegion:
    rect: QRect
    name: str = ""
    selected: bool = False
