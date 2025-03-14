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
