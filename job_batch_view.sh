datahora=$(date)

echo "****************************************************************"
echo "Iniciando job_batch_view.sh - $datahora"

python3 /home/cj/lisarb_jc/lab/jobs/main.py batch

echo "Finalizando batch - $datahora"
echo "****************************************************************\n\n\n"
