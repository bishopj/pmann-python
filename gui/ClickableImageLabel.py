from PySide6.QtWidgets import QLabel
from PySide6.QtGui import QPainter, QPen, QColor, QCursor
from PySide6.QtCore import Qt, QRect, QPoint
from core.face_region import FaceRegion

class ClickableImageLabel(QLabel):
    def __init__(self, parent=None, on_region_selected=None):
        super().__init__(parent)
        self.face_regions: list[FaceRegion] = []
        self.on_region_selected = on_region_selected

        #self.face_rects = []

    #def set_face_rects(self, rects):
    #    self.face_rects = rects
        
    def clear_face_regions(self):
        self.face_regions = []
        
    def add_face_region(self, rect):
        self.face_regions.append(FaceRegion(rect))

    def mousePressEvent(self, event):
        click_pos = self._adjusted_mouse_pos(event.pos())
        for i, region in enumerate(self.face_regions):
            if region.rect.contains(click_pos):
                region.selected = True
                self.selected_index = i
                if self.on_region_selected:
                    self.on_region_selected(region.name)
                #print(f"Face {i+1} clicked at {click_pos}")
                # You could emit a signal or trigger a tagging dialog here
                self.update() #trigger repaint
                break

    def mouseMoveEvent(self, event):
        adjusted_pos = self._adjusted_mouse_pos(event.pos())
        for region in self.face_regions:
            if region.rect.contains(adjusted_pos):
                self.setCursor(QCursor(Qt.PointingHandCursor))
                return
        self.setCursor(QCursor(Qt.ArrowCursor))

    def paintEvent(self, event):
        super().paintEvent(event)
        if not self.pixmap():
            return

        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)

        x_offset = max(0, (self.width() - self.pixmap().width()) // 2)
        y_offset = max(0, (self.height() - self.pixmap().height()) // 2)

        for i, region  in enumerate(self.face_regions):
            adjusted_rect = region.rect.translated(x_offset, y_offset)
            if i == getattr(self, 'selected_index', -1):
                pen = QPen(QColor("orange"), 3)
            else:
                pen = QPen(QColor("lime"), 1.5)
            painter.setPen(pen)
            painter.drawRect(adjusted_rect)

    def _adjusted_mouse_pos(self, pos):
        if not self.pixmap():
            return pos

        pixmap_size = self.pixmap().size()
        label_size = self.size()

        x_offset = max(0, (label_size.width() - pixmap_size.width()) // 2)
        y_offset = max(0, (label_size.height() - pixmap_size.height()) // 2)

        return QPoint(pos.x() - x_offset, pos.y() - y_offset)
