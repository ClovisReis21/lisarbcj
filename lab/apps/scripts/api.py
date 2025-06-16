from fastapi import FastAPI
import subprocess
from typing import Optional

app = FastAPI(title="Orquestrador de Pipelines", version="1.0")

@app.get("/")
def read_root():
    return {"message": "API está funcionando!"}

@app.post("/run/{command}")
def run_command(command: str):
    comandos_validos = ["extrator", "ingesta", "batch", "speed"]
    result = None

    if command not in comandos_validos:
        return {"error": f"Comando inválido. Use um destes: {comandos_validos}"}
    
    try:
        result = subprocess.run(
            ["python3", "main.py", command, "localhost", "big_data_importer", "big_data_importer", "vendas"],
            capture_output=True,
            text=True,
            check=True
        )
        return {
            "command": command,
            "status": "sucesso",
            "stdout": result.stdout,
            "stderr": result.stderr
        }
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar o comando: {e}")
        return {
            "command": command,
            "status": "erro",
            "stdout": e.stdout,
            "stderr": e.stderr
        }
