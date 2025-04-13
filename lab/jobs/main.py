import sys
from src.spark import Spark
from src.extrator import Extrator
from src.ingesta import Ingesta
from src.batchViews import BatchViews

sparkSession = Spark(sys.argv[1])

if sys.argv[1] == 'extrator':
    Extrator(sparkSession).Run()
elif sys.argv[1] == 'ingesta':
    Ingesta(sparkSession).Run()
elif sys.argv[1] == 'batch':
    BatchViews(sparkSession).Run()