datahora=$(date)

echo "****************************************************************"
echo "Iniciando extração - $datahora"

python3 /home/cj/lisarb_jc/lab/jobs/main.py extrator localhost big_data_importer big_data_importer vendas

echo "Finalizando extração - $datahora"
echo "****************************************************************\n\n\n"
