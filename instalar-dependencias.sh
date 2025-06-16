echo "Iniciando atualizacoes..."

apt-get update && apt-get -y upgrade
echo "Atualizacoes finalizadas!"
echo "************************************************************"
echo "Instalando dependencias do projeto..."
echo "************************** Java ****************************"
apt install openjdk-11-jre-headless -y
echo "************************** MySql ****************************"
apt install mysql-client-8.0 -y
apt install python3-pip -y
apt install jupyter -y

sudo -u $SUDO_USER pip install findspark
sudo -u $SUDO_USER pip install pyspark==3.5.1
sudo -u $SUDO_USER pip install mysql-connector-python

echo "Atualizacoes concluidas!"

echo "Dounload dos drivers - jar"

sudo -u $SUDO_USER wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar -P /home/cj/lisarbcj/lab/apps/scripts/src/jars
sudo -u $SUDO_USER wget https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.103.3/nessie-spark-extensions-3.5_2.12-0.103.3.jar -P /home/cj/lisarbcj/lab/apps/scripts/src/jars
sudo -u $SUDO_USER wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-nessie/1.5.0/iceberg-nessie-1.5.0.jar -P /home/cj/lisarbcj/lab/apps/scripts/src/jars
sudo -u $SUDO_USER wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.11/mysql-connector-java-8.0.11.jar -P /home/cj/lisarbcj/lab/apps/scripts/src/jars

uvicorn lab.apps.scripts.api:app --host 0.0.0.0 --port 8000