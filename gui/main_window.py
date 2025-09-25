from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QLineEdit, QPushButton,
    QFileDialog, QTableWidget, QTableWidgetItem, QMessageBox, QProgressBar, 
    QApplication, QTextEdit, QMenuBar, QMenu, QRadioButton, QButtonGroup, QFrame 
)
from PySide6.QtGui import QKeySequence, QAction, QDragEnterEvent, QDropEvent, QDragLeaveEvent
from PySide6.QtCore import QEvent
from PySide6.QtCore import Qt, QThreadPool, QPoint, QTimer, QUrl
from scanner import DuplicateScanner
from file_actions import open_folder, open_file, delete_file, move_to_bucket, show_properties
from pathlib import Path
from gui.widgets import DropDirLineEdit
import threading
import subprocess
import sys
from gui.win_open_with_dlg import open_with_dialog
from gui.scanner_worker import ScannerWorker
from gui.image_window import FaceTaggingWindow
from gui.DraggableTableWidget import DraggableTableWidget
from core.csv_json_tools import load_dict_from_json, save_dict_to_json
from enum import IntEnum

class ViewMode(IntEnum):
    ALL = 1
    DUPLICATES = 2
    UNIQUE = 3

class DictMode(IntEnum):
    MASTER = 1
    CANDIDATE = 2

class DuplicateViewerWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.threadpool = QThreadPool()
        self.cancel_flag = threading.Event()
        self.master = {}
        self.master_tags={}
        self.candidate = {}
        self._dict_mode = DictMode.MASTER
        #self.active_dict = self.master 
        self.setWindowTitle("Photo Dedupe Viewer")
        self.resize(1000, 600)
        container = QWidget()
        self.setup_menu_bar() 
        container.setLayout(self.build_framelayout())
        self.setCentralWidget(container)
        #delete_action.setShortcut("Del")
        #DictMode(self.dict_group.checkedId())== DictMode.MASTER

    @property
    def active_dict(self):
        return self.master if self._dict_mode == DictMode.MASTER else self.candidate

    @active_dict.setter
    def active_dict(self, new_dict):
        if self._dict_mode == DictMode.MASTER:
            self.master = new_dict
        else:
            self.candidate = new_dict

    def set_dict_mode(self, mode, update_button=True):
        self._dict_mode = mode  # Called when radio button changes
        if update_button: 
            if mode == DictMode.MASTER:
                self.radio_master.setChecked(True)
            else:
                self.radio_candidate.setChecked(True)
    
    def keyPressEvent(self, event):
        if event.matches(QKeySequence.Copy):
            self.copy_selected_cells_as_csv()
    
    def setup_menu_bar(self):
        menu_bar = self.menuBar()

        # File Menu
        file_menu = menu_bar.addMenu("File")

        load_dict_action = QAction("Load Master from JSon", self)
        load_dict_action.triggered.connect(self.load_master_dict)
        file_menu.addAction(load_dict_action)

        save_dict_action = QAction("Save Master to JSon", self)
        save_dict_action.triggered.connect(self.save_master_dict)
        file_menu.addAction(save_dict_action)

        export_table_action = QAction("Export Table to CSV", self)
        export_table_action.triggered.connect(self.export_table)
        file_menu.addAction(export_table_action)


        exit_action = QAction("Exit", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        # Edit Menu
        edit_menu = menu_bar.addMenu("Edit")

        copy_action = QAction("Copy Selected as CSV", self)
        copy_action.setShortcut("Ctrl+C")
        copy_action.triggered.connect(self.copy_selected_cells_as_csv)
        edit_menu.addAction(copy_action)

        resize_action = QAction("Resize all columns", self)
        resize_action.setShortcut("Ctrl+R")
        resize_action.triggered.connect(self.slow_col_resize)
        edit_menu.addAction(resize_action)
        
    
    def show_context_menu(self, position: QPoint):
        index = self.table.indexAt(position)
        if not index.isValid():
            return  # Clicked outside any cell

        row, col = index.row(), index.column()
        item = self.table.item(row, col)
        if not item:
            return

        path = Path(item.text())  # Assuming cell text is a file path

        menu = QMenu(self)

        view_action = QAction("View With...", self)
        view_action.triggered.connect(lambda: self.view_with(path))

        open_folder_action = QAction("Open Containing Folder", self)
        open_folder_action.triggered.connect(lambda: self.open_containing_folder(path))

        delete_action = QAction("Delete", self)
        delete_action.setShortcut("Del")
        delete_action.triggered.connect(lambda: self.delete_file(row, col, path))

        restore_action = QAction("Mark as Uncopied", self)
        restore_action.triggered.connect(lambda: self.table.restore_item_state(item))

        menu.addAction(view_action)
        menu.addAction(open_folder_action)
        menu.addSeparator()
        menu.addAction(delete_action)
        menu.addSeparator()
        menu.addAction(restore_action)

        menu.exec(self.table.viewport().mapToGlobal(position))

    def setup_view_selector(self):
        self.radio_all = QRadioButton("All")
        self.radio_duplicates = QRadioButton("Duplicates")
        self.radio_unique = QRadioButton("Unique")

        self.radio_all.setChecked(True)  # Default view

        self.view_group = QButtonGroup(self)
        self.view_group.addButton(self.radio_all)
        self.view_group.addButton(self.radio_duplicates)
        self.view_group.addButton(self.radio_unique)
        
        self.view_group.setId(self.radio_all, ViewMode.ALL)
        self.view_group.setId(self.radio_duplicates, ViewMode.DUPLICATES)
        self.view_group.setId(self.radio_unique, ViewMode.UNIQUE)

        frame = QFrame()
        frame.setFrameShape(QFrame.StyledPanel)
        frame.setFrameShadow(QFrame.Sunken)


        layout = QHBoxLayout()
        layout.addWidget(self.radio_all)
        layout.addWidget(self.radio_duplicates)
        layout.addWidget(self.radio_unique)

        frame.setLayout(layout)
        self.radio_all.toggled.connect(lambda: self.update_table_view()) #"all"))
        self.radio_duplicates.toggled.connect(lambda: self.update_table_view()) #"dupes"))
        self.radio_unique.toggled.connect(lambda: self.update_table_view()) #"unique"))

        return frame

    def setup_dict_selector(self):
        self.radio_master = QRadioButton("Master")
        self.radio_candidate = QRadioButton("Candidate")

        self.radio_master.setChecked(True)  # Default view

        self.dict_group = QButtonGroup(self)
        self.dict_group.addButton(self.radio_master)
        self.dict_group.addButton(self.radio_candidate)
        
        self.dict_group.setId(self.radio_master, DictMode.MASTER)
        self.dict_group.setId(self.radio_candidate, DictMode.CANDIDATE)

        frame = QFrame()
        frame.setFrameShape(QFrame.StyledPanel)
        frame.setFrameShadow(QFrame.Sunken)


        layout = QHBoxLayout()
        layout.addWidget(self.radio_master)
        layout.addWidget(self.radio_candidate)

        frame.setLayout(layout)
        self.radio_master.toggled.connect(lambda: self.update_table_view()) #"all"))
        self.radio_candidate.toggled.connect(lambda: self.update_table_view()) #"dupes"))
    
        return frame
       

    def build_framelayout(self):
        self.root_dir_input = DropDirLineEdit()
        self.root_dir_input.setPlaceholderText("Enter or drag folder path here")
        self.root_dir_input.setAcceptDrops(True)
        #self.root_dir_input.installEventFilter(self)

        browse_btn = QPushButton("Browse")
        browse_btn.clicked.connect(self.browse_folder)

        scan_btn = QPushButton("Scan")
        scan_btn.clicked.connect(self.start_scan) #scan_for_duplicates)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)  # Indeterminate mode
        self.progress_bar.setFixedHeight(24)
        self.progress_bar.setVisible(False)
        self.progress_bar.setTextVisible(False)  # Optional: hide percentage text

        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.setVisible(False)
        self.cancel_button.clicked.connect(self.cancel_scan)
        
        self.output = QTextEdit()
        self.output.setVisible(False)
        
        self.process_Image_button = QPushButton("Image View")
        self.process_Image_button.setFixedWidth(160)
        self.process_Image_button.setVisible(True)
        self.process_Image_button.clicked.connect(self.open_face_tagging_window)

        
        self.notinmast_button = QPushButton("Not In Master")
        self.notinmast_button.setFixedWidth(160)
        self.notinmast_button.setVisible(True)
        self.notinmast_button.clicked.connect(self.notinmaster_dict)


        top_bar = QHBoxLayout()
        top_bar.addWidget(self.root_dir_input)
        top_bar.addWidget(browse_btn)
        top_bar.addWidget(scan_btn)

        prog_bar = QHBoxLayout()
        prog_bar.addWidget(self.progress_bar)
        prog_bar.addWidget(self.cancel_button)

        selector_bar = QHBoxLayout()
        selector_bar.addWidget(self.process_Image_button)
        selector_bar.addWidget(self.setup_view_selector())
        selector_bar.addWidget(self.setup_dict_selector())
        selector_bar.addWidget(self.notinmast_button)
        
        top_bar_box = QVBoxLayout()
        top_bar_box.addLayout(top_bar)
        top_bar_box.addLayout(selector_bar)
        top_bar_box.addLayout(prog_bar)
        top_bar_box.addWidget(self.output)
        
        self.table = DraggableTableWidget()
        self.table.setColumnCount(3)
        self.table.setHorizontalHeaderLabels(["File 1", "File 2", "File 3"])
        self.table.cellDoubleClicked.connect(self.handle_double_click)
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.show_context_menu)
        self.table.setSortingEnabled(True)
        self.table.setDragEnabled(True)

        layout = QVBoxLayout()
        layout.addLayout(top_bar_box)
        layout.addWidget(self.table)

        return layout

    def set_progress_visibility( self, turn_on ):
        self.progress_bar.setVisible(turn_on)
        self.cancel_button.setVisible(turn_on)
        self.output.setVisible(turn_on)
     
    def update_table_view(self):
        self.set_dict_mode(DictMode(self.dict_group.checkedId()), False)
        self.populate_table(self.active_dict, ViewMode(self.view_group.checkedId()) )
        
        """
        self.table.setRowCount(0)  # Clear existing rows

        for hash_key, paths in self.master.items():
            if selected_mode == ViewMode.DUPLICATES and len(paths) <= 1:
                continue
            if selected_mode == ViewMode.UNIQUE and len(paths) != 1:
                continue

            row = self.table.rowCount()
            self.table.insertRow(row)

            for col, path in enumerate(paths):
                item = QTableWidgetItem(str(path))
                self.table.setItem(row, col, item)

            # Add hidden hash column
            hash_item = QTableWidgetItem(hash_key)
            self.table.setItem(row, self.table.columnCount() - 1, hash_item)
        self.hidden_index = self.table.columnCount() - 1
        self.table.resizeColumnsToContents()
        #QTimer.singleShot(0, self.table.resizeColumnsToContents)
        """
        
    def populate_table(self, dupes: dict, selected_mode = ViewMode.ALL):
        self.table.setRowCount(0)
        if not dupes: 
            return
        # Determine max group size to set column count
        if selected_mode == ViewMode.UNIQUE:
            max_cols = 1
        else:
            max_cols = max(len(group) for group in dupes.values())
        self.table.setColumnCount(max_cols)
        self.table.setHorizontalHeaderLabels([f"File {i+1}" for i in range(max_cols)])
        for i in range(max_cols): self.table.setColumnHidden(i, False)
        self.table.setColumnCount(max_cols + 1)
        self.table.setColumnHidden(max_cols, True)
        self.hidden_index = max_cols

        self.table.setSortingEnabled(False)

        for key, group in dupes.items():
            if selected_mode == ViewMode.DUPLICATES and len(group) <= 1:
                continue
            if selected_mode == ViewMode.UNIQUE and len(group) != 1:
                continue
            row = self.table.rowCount()
            self.table.insertRow(row)
            for col, file_path in enumerate(group):
                item = QTableWidgetItem(str(file_path))
                item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
                self.table.setItem(row, col, item)
            self.table.setItem(row, self.hidden_index, QTableWidgetItem(str(key)))
    
        if selected_mode == ViewMode.ALL:
            self.resize_columns_fully( self.table)
        else:
            self.table.resizeColumnsToContents()
        self.table.setSortingEnabled(True)


    def slow_col_resize(self):
        self.resize_columns_fully( self.table)

    def resize_columns_fully(self, table):
        font_metrics = table.fontMetrics()
        for col in range(table.columnCount()):
            max_width = 0
            for row in range(table.rowCount()):
                item = table.item(row, col)
                if item:
                    text = item.text()
                    width = font_metrics.boundingRect(text).width()
                    max_width = max(max_width, width)
            table.setColumnWidth(col, max_width + 20)  # Add padding

        
    def copy_selected_cells_as_csv(self):
        selected = self.table.selectedIndexes()
        if not selected:
            return

        selected.sort(key=lambda x: (x.row(), x.column()))
        rows = {}

        for index in selected:
            item = self.table.item(index.row(), index.column())
            text = item.text() if item is not None else ""
            rows.setdefault(index.row(), {})[index.column()] = text

        output = []
        for row in sorted(rows):
            cols = rows[row]
            line = ",".join(f'"{cols.get(col, "")}"' for col in range(self.table.columnCount()))
            output.append(line)

        clipboard = QApplication.clipboard()
        clipboard.setText("\n".join(output))
            
    def export_table_to_csv(self, filepath: Path):
        with filepath.open("w", encoding="utf-8") as f:
            for row in range(self.table.rowCount()):
                row_data = []
                for col in range(self.table.columnCount()):
                    item = self.table.item(row, col)
                    row_data.append(item.text() if item else "")
                f.write(",".join(f'"{cell}"' for cell in row_data) + "\n")

    def view_with(self, path):
        launched = open_with_dialog(path)
        return
    
        #IGNORE THE FOLLOWING CODE - WORKING BUT NOT REQUIRED
        #Keep as probably useful for a Linux/Mac version
        if not launched:
            # Prompt user to select an app to open the file
            app_path, _ = QFileDialog.getOpenFileName(
                self,
                "Select Viewer Application",
                "",
                "Executables (*.exe);;All Files (*)"
            )
            if app_path:
                try:
                    # subprocess.Popen(["xdg-open", str(path)])  # Linux
                    # subprocess.Popen(["open", str(path)])     # macOS
                    # os.startfile(str(path))                   # Windows
                    subprocess.Popen([app_path, str(path)])
                except Exception as e:
                    QMessageBox.critical(self, "Launch Failed", f"Could not open file:\n{e}")

    def open_containing_folder(self, path):
        # Use OS-native explorer to show the file
        folder = path.parent
        if not folder.exists():
            QMessageBox.warning(self, "Folder Missing", f"Containing folder does not exist:\n{folder}")
            return

        try:
            if sys.platform.startswith("win"):
                subprocess.run(["explorer", "/select,", str(path)])
            elif sys.platform.startswith("darwin"):
                subprocess.run(["open", "-R", str(path)])
            else:  # Linux
                subprocess.run(["xdg-open", str(folder)])
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Could not open folder:\n{e}")
            
    def get_selected_image_path(self):
        selected_items = self.table.selectedItems()
        if not selected_items:
            return None  # No selection

        item = selected_items[0]  # Assuming single selection
        path = Path(item.text())  # Assuming cell text is a file path
        return path

    def open_face_tagging_window(self):
        selected_path = self.get_selected_image_path()
        self.face_window = FaceTaggingWindow(self, image_path=selected_path)
        self.face_window.show()

    def shift_cells_left(self, row: int, start_col: int, exclude_col: int):
        col_count = self.table.columnCount()
        for col in range(start_col, col_count - 1):  # exclude last (hidden) column
            next_item = self.table.item(row, col + 1)
            if next_item is None or not next_item.text().strip():
                self.table.takeItem(row, col)  # Clear current cell
                break
            self.table.setItem(row, col, QTableWidgetItem(next_item.text()))
        self.table.takeItem(row, col_count - 2)  # Clear last visible cell

    def delete_file(self, row: int, col: int, path: Path):
        # Confirm, delete from disk, table, and self.master
        reply = QMessageBox.question(
            self,
            "Confirm Delete",
            f"Delete this file from disk, table, and dictionary?\n{path}",
            QMessageBox.Yes | QMessageBox.No
        )
        if reply != QMessageBox.Yes:
            return

        try:
            path.unlink()
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to delete file:\n{e}")
            return

        hash_key = self.get_hash_key_for_row(row)
        if hash_key and hash_key in self.active_dict:
            self.active_dict[hash_key] = [p for p in self.active_dict[hash_key] if p != path]
            if not self.active_dict[hash_key]:
                del self.active_dict[hash_key]

        self.table.takeItem(row, col)
        self.shift_cells_left( row, col, self.hidden_index)
        self.table.resizeColumnsToContents()

    
    def browse_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Folder")
        if folder:
            self.root_dir_input.setText(folder)


    def export_table(self):
        path, _ = QFileDialog.getSaveFileName(self, "Save CSV", "", "CSV Files (*.csv)")
        if path:
            self.export_table_to_csv(Path(path))

    def save_master_dict(self):
        path, _ = QFileDialog.getSaveFileName(self, "Save JSon", "", "JSon Files (*.json)")
        if path:
            filepath = Path(path)
            if filepath.suffix.lower() != ".json":
                filepath = filepath.with_suffix(".json")
            path = filepath.as_posix()
            result, err = save_dict_to_json(self.master, path)
            if not result:
                QMessageBox.information(
                    self,
                    "Error",
                    f"Can't save master dictionary\n{err}",
                    QMessageBox.Ok
                )
                
            
    def load_master_dict(self):
        path, _ = QFileDialog.getOpenFileName(self, "Load JSon", "", "JSon Files (*.json)")
        if path:
            try:
                self.master = load_dict_from_json( path)
                self.set_dict_mode(DictMode.MASTER) 
                #self.populate_table(self.master, ViewMode(self.view_group.checkedId()))
            except Exception as e:
                QMessageBox.information(
                    self,
                    "Error",
                    "Can't load master dictionary",
                    QMessageBox.Ok
                )

    def notinmaster_dict(self):
        for key in list(self.candidate.keys()):
            if key in self.master: 
                del self.candidate[key]
        self.set_dict_mode(DictMode.CANDIDATE)

    #multi threaded scanner
    def start_scan(self):
        self.cancel_flag.clear()
        self.progress_bar.setRange(0, 0)  # Indeterminate
        path = self.root_dir_input.text().strip()
        if not Path(path).is_dir():
            QMessageBox.warning(self, "Invalid Path", "Please enter a valid directory.")
            return

        self.set_progress_visibility(True)
        QApplication.processEvents()
        worker = ScannerWorker(path, self.cancel_flag, False)

        worker.signals.progress.connect(self.update_progress)
        worker.signals.finished.connect(self.scan_finished)
        worker.signals.error.connect(self.show_error)
        worker.signals.cancelled.connect(self.scan_cancelled)

        self.threadpool.start(worker)

    def update_progress(self, path):
        self.output.append(f"Scanning: {path}")

    def scan_finished(self, result):
        self.progress_bar.setRange(0, 1)
        self.output.append(f"Scan complete. Found {len(result)} files.")
        self.set_progress_visibility(False)
        self.active_dict = result
            
        self.populate_table(result, ViewMode(self.view_group.checkedId()))

    def show_error(self, msg):
        self.output.append(f"Error: {msg}")

    def scan_cancelled(self):
        self.progress_bar.setRange(0, 1)
        self.output.append("Scan cancelled.")

    def cancel_scan(self):
        self.cancel_flag.set()


    #unthreaded scanner
    def scan_for_duplicates(self):
        path = self.root_dir_input.text().strip()
        if not Path(path).is_dir():
            QMessageBox.warning(self, "Invalid Path", "Please enter a valid directory.")
            return

        self.set_progress_visibility(True)
        QApplication.processEvents()
        scanner = DuplicateScanner(path)
        self.master = scanner.scan()
        self.set_progress_visibility(False)
        self.populate_table(self.master)

    def get_hash_key_for_row(self, row: int) :
        item = self.table.item(row, self.hidden_index)
        return item.text() if item else None


    def handle_double_click(self, row, col):
        item = self.table.item(row, col)
        if item:
            path = Path(item.text())
            open_file(path)

