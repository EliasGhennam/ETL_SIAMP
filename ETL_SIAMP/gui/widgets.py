from PyQt6.QtWidgets import QListWidget
from PyQt6.QtGui import QPainter, QFont, QColor
from PyQt6.QtCore import Qt
from typing import List

class DropListWidget(QListWidget):
    def __init__(self, on_click_callback=None):
        super().__init__()
        self.setAcceptDrops(True)
        self.setSelectionMode(self.SelectionMode.ExtendedSelection)
        self.setMinimumHeight(150)
        self.on_click_callback = on_click_callback

    def paintEvent(self, event):
        super().paintEvent(event)
        if self.count() == 0:
            painter = QPainter(self.viewport())
            painter.setPen(QColor("#777"))
            font = QFont("Segoe UI", 10, QFont.Weight.Normal)
            font.setItalic(True)
            painter.setFont(font)
            text = "Glissez vos fichiers Excel ici ou cliquez pour les sélectionner"
            painter.drawText(self.rect(), Qt.AlignmentFlag.AlignCenter, text)

    def mousePressEvent(self, event):
        if self.count() == 0 and self.on_click_callback:
            self.on_click_callback()
        super().mousePressEvent(event)

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()

    def dragMoveEvent(self, event):
        event.acceptProposedAction()

    def dropEvent(self, event):
        for url in event.mimeData().urls():
            f = url.toLocalFile()
            if f.lower().endswith(".xlsx") and f not in self.files():
                self.addItem(f)
        event.acceptProposedAction()

    def files(self) -> List[str]:
        return [self.item(i).text() for i in range(self.count())]
