
from PySide6.QtWidgets import QTableWidget, QApplication, QAbstractItemView
from PySide6.QtGui import QMouseEvent
from PySide6.QtCore import QMimeData, QUrl, Qt
from PySide6.QtGui import QDrag,  QBrush, QColor
from pathlib import Path

class DraggableTableWidget(QTableWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        
        # Enable multi-item selection
        self.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.setSelectionMode(QAbstractItemView.ExtendedSelection)

        self.setDragEnabled(True)
        self.setDragDropMode(QAbstractItemView.DragOnly)

        self._drag_start_pos = None

 

    def startDrag(self):
        selected_items = self.selectedItems()
        if not selected_items:
            return

        file_paths = []
        self._dragged_items = []
        for item in selected_items:
            path = Path(item.text())
            if path.exists():
                file_paths.append(QUrl.fromLocalFile(str(path)))
                self._dragged_items.append((item.row(), item.column()))


        if not file_paths:
            return

        mime_data = QMimeData()
        mime_data.setUrls(file_paths)

        drag = QDrag(self)
        drag.setMimeData(mime_data)
        result = drag.exec(Qt.CopyAction)
        if result in (Qt.CopyAction, Qt.MoveAction):
            # Drop likely succeeded — not canceled
            self.mark_items_as_dropped()


    def mousePressEvent(self, event: QMouseEvent):
        if event.button() == Qt.LeftButton:
            self._drag_start_pos = event.pos()
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event: QMouseEvent):
        if not (event.buttons() & Qt.LeftButton):
            return
        if self._drag_start_pos is None:
            return
        if (event.pos() - self._drag_start_pos).manhattanLength() < QApplication.startDragDistance():
            return

        self.startDrag()

    def mark_items_as_dropped(self):
        for row, col in self._dragged_items:
            item = self.item(row, col)
            if item:
                item.setBackground(QBrush(QColor("#d0f0c0")))  # Light green
                item.setData(Qt.UserRole + 1, "copied")
    
    def restore_item_state(self, item):
        item.setBackground(QBrush(Qt.white))
        item.setData(Qt.UserRole + 1, None)

