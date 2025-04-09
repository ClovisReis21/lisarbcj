datahora=$(date)

echo "****************************************************************"
echo "Iniciando ingesta - $datahora"

python3 /home/cj/lisarb_jc/lab/jobs/main.py ingesta

echo "Finalizando ingesta - $datahora"
echo "****************************************************************\n\n\n"
