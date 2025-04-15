echo "Parando gold view..."
pkill -f goldView.py

echo "Parando streaming..."
pkill -f speedViews.py

echo "Parando containers..."
docker compose -f lab/apps/docker-compose.yml down

echo "Finalizado!"
