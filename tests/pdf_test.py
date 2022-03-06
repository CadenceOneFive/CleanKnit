from PyQt5.QtWidgets import QApplication
import qpageview
from qpageview.rubberband import Rubberband
from time import sleep

app = QApplication([])

v = qpageview.View()
v.resize(900, 500)
v.loadPdf("C:/data/socrata/pluto_datadictionary.pdf")
v.show()
# r = Rubberband()
# v.setRubberband(r)
sleep(100)
# app.exec()
