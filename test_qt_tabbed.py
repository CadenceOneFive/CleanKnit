
import sys
from PySide6.QtWidgets import QWidget, QApplication, QVBoxLayout, QGridLayout, QLabel, QTabWidget
from PySide6.QtWidgets import QApplication



class Window(QWidget):
    def __init__(self):
        QWidget.__init__(self)
        layout = QGridLayout()
        self.setLayout(layout)
        label1 = QLabel("Widget in Tab 1.")
        label2 = QLabel("Widget in Tab 2.")
        tabwidget = QTabWidget()
        tabwidget.addTab(label1, "Tab 1")
        tabwidget.addTab(label2, "Tab 2")
        layout.addWidget(tabwidget, 0, 0)

app = QApplication(sys.argv)
screen = Window()
screen.show()
sys.exit(app.exec_())