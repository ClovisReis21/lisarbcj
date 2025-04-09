echo "Adicionando job extracao..."

CRON_EXPRESSION="0 0-0,2-23 * * * /home/cj/lisarb_jc/job_extracao.sh >> /home/cj/lisarb_jc/saida_extracao.txt 2>&1"
# CRON_EXPRESSION="* * * * * /home/cj/lisarb_jc/job_extracao.sh >> /home/cj/lisarb_jc/saida_extracao.txt 2>&1"

(crontab -l ; echo "$CRON_EXPRESSION") | crontab -

echo "Adicionando job ingesta..."

CRON_EXPRESSION="* 1 * * * /home/cj/lisarb_jc/job_ingesta.sh >> /home/cj/lisarb_jc/saida_ingesta.txt 2>&1"
# CRON_EXPRESSION="* * * * * /home/cj/lisarb_jc/job_ingesta.sh >> /home/cj/lisarb_jc/saida_ingesta.txt 2>&1"

(crontab -l ; echo "$CRON_EXPRESSION") | crontab -

echo "Concluído!"
