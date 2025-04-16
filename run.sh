echo "Iniciando containers..."
docker compose -f lab/apps/docker-compose.yml up --build  -d

echo "Iniciando streaming..."
sudo -u $SUDO_USER nohup python3 lab/jobs/src/speedViews.py > saida_speed_views.log 2>&1 &

echo "Iniciando gold view..."
sudo -u $SUDO_USER nohup python3 lab/jobs/src/goldView.py > saida_gold_view.log 2>&1 &

echo "Rodando!"
