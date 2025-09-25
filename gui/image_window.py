from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
    QLineEdit, QLabel, QDialog, QFileDialog
)
from PySide6.QtGui import QPixmap, QDragEnterEvent, QDropEvent, QPainter, QPen, QColor
from PySide6.QtCore import Qt, QRect, QPoint
from gui.ClickableImageLabel import ClickableImageLabel
from pathlib import Path
import hashlib


import face_recognition

class FaceTaggingWindow(QDialog):
    def __init__(self, parent=None, image_path=None):
        super().__init__(parent)
        self.hash = ""
        self.image_path = image_path
        self.setWindowTitle("Face Tagging")
        self.resize(600, 400)

        self.setAcceptDrops(True)

        # Layouts
        main_layout = QVBoxLayout()
        control_layout = QHBoxLayout()

        # Controls
        self.detect_button = QPushButton("Detect Faces")
        self.name_input = QLineEdit()
        self.save_button = QPushButton("Save")
        self.clear_button = QPushButton("Clear")

        control_layout.addWidget(self.detect_button)
        control_layout.addWidget(self.name_input)
        control_layout.addWidget(self.save_button)
        control_layout.addWidget(self.clear_button)

        # Image display
        self.image_label = ClickableImageLabel("Drop an image here",on_region_selected=self.update_name_field)
        self.image_label.setAlignment(Qt.AlignCenter)
        self.image_label.setStyleSheet("border: 1px dashed gray;")
        self.image_label.setMinimumSize(400, 300)

        # Assemble
        main_layout.addLayout(control_layout)
        main_layout.addWidget(self.image_label)
        self.setLayout(main_layout)

        # Connect buttons
        self.detect_button.clicked.connect(self.detect_faces)
        self.save_button.clicked.connect(self.save_name)
        self.clear_button.clicked.connect(self.clear_selection)

        self.load_image_viewer()

    def dragEnterEvent(self, event: QDragEnterEvent):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()

    def dropEvent(self, event: QDropEvent):
        urls = event.mimeData().urls()
        if urls:
            self.image_path = urls[0].toLocalFile()
            self.load_image_viewer()

    def load_image_viewer(self):
        #load the viewer from the image_path
        if self.image_path:
            #self.face_rects = []
            self.hash = self.compute_hash(Path(self.image_path))
            pixmap = QPixmap(self.image_path)
            self.scaled_pixmap = pixmap.scaled(
                self.image_label.size(), Qt.KeepAspectRatio, Qt.SmoothTransformation
            )
            self.image_label.setPixmap(self.scaled_pixmap)
            self.image_label.clear_face_regions()
    
    def compute_hash(self, path: Path, chunk_size: int = 65536) -> str:
        hasher = hashlib.sha256()
        with path.open("rb") as f:
            while chunk := f.read(chunk_size):
                hasher.update(chunk)
        return hasher.hexdigest()

    def hash_file(self, path):
        hash = self.compute_hash(path)
        return (path, hash)
    
    def detect_faces(self):
        # Placeholder for face detection logic
        #print(f"Detecting faces in: {self.image_path}")
        self.load_face_detector()

    def update_name_field(self, name):
        self.name_input.setText(name)

    def save_name(self):
        # Placeholder for saving name logic
        name = self.name_input.text()
        index = getattr(self.image_label, 'selected_index', -1)
        if index >= 0:
            self.image_label.face_regions[index].name = name
            #print(f"Saved name '{name}' for face {index + 1}")


    def clear_selection(self):
        # Placeholder for clearing bounding box
        index = getattr(self.image_label, 'selected_index', -1)
        if index >= 0:
            self.image_label.face_regions[index].name = ""
            self.name_input.clear()
            #print(f"Cleared name for face {index + 1}")

    def draw_bounding_boxes(self, image):
        painter = QPainter(self.scaled_pixmap)
        pen = QPen(QColor("red"))
        pen.setWidth(2)
        painter.setPen(pen)

        #self.face_rects = []  # Store QRect objects for interaction

        x_scale = self.scaled_pixmap.width() / image.shape[1]
        y_scale = self.scaled_pixmap.height() / image.shape[0]
        
        for top, right, bottom, left in self.face_locations:
            x = int(left * x_scale)
            y = int(top * y_scale)
            w = int((right - left) * x_scale)
            h = int((bottom - top) * y_scale)

            rect = QRect(x, y, w, h)
            self.image_label.add_face_region(rect)
            #self.face_rects.append(rect)
            painter.drawRect(rect)
            
        painter.end()

        self.image_label.setPixmap(self.scaled_pixmap)
        #self.image_label.set_face_rects(self.face_rects)

        
    def old_draw_bounding_boxes(self, image):
        painter = QPainter(self.scaled_pixmap)
        pen = QPen(QColor("red"))
        pen.setWidth(2)
        painter.setPen(pen)

        # Draw each face bounding box
        for top, right, bottom, left in self.face_locations:
            # Scale coordinates to match the displayed image
            x_scale = self.scaled_pixmap.width() / image.shape[1]
            y_scale = self.scaled_pixmap.height() / image.shape[0]

            x = int(left * x_scale)
            y = int(top * y_scale)
            w = int((right - left) * x_scale)
            h = int((bottom - top) * y_scale)

            painter.drawRect(x, y, w, h)

        painter.end()

        self.image_label.setPixmap(self.scaled_pixmap)
            
    def load_face_detector(self):
        # Load your image
        image = face_recognition.load_image_file(str(self.image_path))  
          
        # Find all face locations
        self.face_locations = face_recognition.face_locations(image)

        # Optionally get facial landmarks
        face_landmarks = face_recognition.face_landmarks(image)

        # Draw bounding boxes
        self.draw_bounding_boxes(image)
        
        #print(f"Found {len(self.face_locations)} face(s)")
