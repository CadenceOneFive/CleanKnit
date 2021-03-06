import sys
from PySide6.QtWidgets import QWidget, QApplication, QVBoxLayout
from PySide6.QtWidgets import QApplication
from PySide6.QtWebEngineWidgets import QWebEngineView


class Example(QWidget):
    def __init__(self):
        super().__init__()

        self.initUI()

    def initUI(self):

        vbox = QVBoxLayout(self)

        self.webEngineView = QWebEngineView()
        self.loadPage()

        vbox.addWidget(self.webEngineView)

        self.setLayout(vbox)

        self.setGeometry(300, 300, 350, 250)
        self.setWindowTitle("QWebEngineView")
        self.show()

    def loadPage(self):

        with open("geopoly_example.html", "r") as f:

            html = f.read()
            self.webEngineView.setHtml(html)


def main():

    app = QApplication(sys.argv)
    ex = Example()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
