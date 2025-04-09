import sys
from src.spark import Spark
from src.extrator import Extrator
from src.ingesta import Ingesta

sparkSession = Spark()

if sys.argv[1] == 'extrator':
    Extrator(sparkSession).Run()
elif sys.argv[1] == 'ingesta':
    Ingesta(sparkSession).Run()