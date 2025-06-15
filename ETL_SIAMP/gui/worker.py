from PyQt6.QtCore import QThread, pyqtSignal
from etl.core import run_etl

class Worker(QThread):
    done = pyqtSignal(bool)

    def __init__(self, fichiers: list[str], chemin_sortie: str):
        super().__init__()
        self.fichiers = fichiers
        self.chemin_sortie = chemin_sortie

    def run(self):
        success = run_etl(self.fichiers, self.chemin_sortie)
        self.done.emit(success)
