from PySide6.QtWidgets import QLineEdit
from PySide6.QtCore import Qt, QUrl
from PySide6.QtGui import QDropEvent, QDragEnterEvent
from pathlib import Path


class DropDirLineEdit(QLineEdit):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setAcceptDrops(True)

    def dragEnterEvent(self, event: QDragEnterEvent):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
            self.setStyleSheet("background-color: lightblue;")


    def dropEvent(self, event: QDropEvent):
        urls = event.mimeData().urls()
        if urls:
            path = urls[0].toLocalFile()
            if Path(path).is_dir():
                self.setText(path)
        self.setStyleSheet("")
        
    def dragLeaveEvent(self, event):
        self.setStyleSheet("")



