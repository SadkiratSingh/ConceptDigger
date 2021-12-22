from classes.Connector import Connector

class Main:
    def __init__(self, create_fresh: bool, datafolderprefix: str, language_code:str):
        Connector(create_fresh, datafolderprefix, language_code)



Main(False,
    "extractedttlfiles/",
    "en"
    )