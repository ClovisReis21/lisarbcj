import logging

class Notificador:
    def __init__(self, logLevel = "INFO"):
        logging.basicConfig(
            level=logLevel,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

    def lof_level(self, newLevel):
        logging.basicConfig(level=newLevel)
    
    def mostrar(self, nivel, mensagem):
        getattr(logging, nivel)(mensagem)

