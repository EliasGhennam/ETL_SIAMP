# main.py
from PyQt6.QtWidgets import QApplication
from gui.gui_main import MainWindow
import sys

if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())
