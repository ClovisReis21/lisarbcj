# entrypoint.py
import sys
from src.spark import Spark
from src.extrator import Extrator
from src.ingesta import Ingesta
from src.batch_views import BatchViews
from src.speed_views import SpeedViews

def main():
    if len(sys.argv) < 2:
        print("Uso: python entrypoint.py <comando>")
        sys.exit(1)

    command_name = sys.argv[1]
    spark_wrapper = Spark(command_name)
    spark_session = spark_wrapper.get()

    # Dispatcher de comandos
    commands = {
        'extrator': lambda: Extrator(spark_session).run(),
        'ingesta': lambda: Ingesta(spark_session).run(),
        'batch': lambda: BatchViews(spark_session).run(),
        'speed': lambda: SpeedViews(spark_session).run()
    }

    command = commands.get(command_name)
    if command:
        command()
    else:
        print(f"[ERRO] Comando inválido: {command_name}")
        print("Comandos disponíveis:", ', '.join(commands.keys()))
        sys.exit(1)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f'[ERRO] Execução principal falhou: {e}')
        try:
            Spark(sys.argv[1]).stop()
        except Exception as stop_error:
            print(f'[ERRO ao encerrar Spark]: {stop_error}')
        sys.exit(1)
