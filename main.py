import os
import sys

# Locate the plugin path inside the bundled app
if hasattr(sys, '_MEIPASS'):
    plugin_path = os.path.join(sys._MEIPASS, '_internal', 'PySide6', 'plugins')
    os.environ['QT_PLUGIN_PATH'] = plugin_path

from PySide6.QtWidgets import QApplication
from gui.main_window import DuplicateViewerWindow

if __name__ == "__main__":
    import sys
    app = QApplication(sys.argv)
    window = DuplicateViewerWindow()
    window.show()
    sys.exit(app.exec())
