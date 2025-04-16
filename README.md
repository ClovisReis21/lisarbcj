# lisarbcj

## Objetivo

Case engenharia de dados. Pipeline de ingestão de dados de uma loja fictícia chamada aqui de Loja Simulada.

## Problema

Aplicar a arquitetura lambda em um problema de engenharia de dados.

## Instalação

Faça o clone do projeto na pasta home *(~/)* de um usuário com privilégios administrativos (root)
```
$ git clone https://github.com/ClovisReis21/lisarbcj.git

$ cd lisarbcj
```

### pre requisitos

Sertifique-se de que as dependências abaixo estejam instaladas

* Docker -> engine community version 26.1.4 - **instale você mesmo**
* Linux Ubuntu 24.04.4 LTS
* Java -> openjdk-11-jre-headless   *
* mysql-client-8.0  *
* python3-pip   *
* jupyter   *

**O arquivo bash** *instalar-dependencias.sh* **pode auxiliar na instalação de algumas das dependências acima (marcadas com** * **) - porem, é importante se certificar se ele fez um bom trabalho antes de seguir adiante**
```
$ sudo sh instalar-dependencias.sh
```
### usuários e ACL
Certifique-se de que os executaveis possam ser acessados e executados pelo usuário logado.

### iniciando o projeto
Tendo dudo dado certo até aqui, tudo indica que o ambiente está montado, é hora de subir o projeto.
Digite o seguinte comando:
```
$ sudo sh run.sh
```
Aguarde até que os containers subam...

### utilizando
A aplicação loja_simulada realiza geração aleatória de *vendas* com uma fequencia de 3 vendas por minuto (padrão), mas isso pode ser alterado conforme se queira sempre respeitando os limites de infraestrutura na qual o projeto estiver rodando.
Para realizar a alteração da quantidade de vendas por minuto, utilize a URL *http://localhost:30001/update/<vendasPorMinuto>* com o verbo PUT.

Caso queira utilizar o comando curl para isto:
```
$ curl -X PUT http://localhost:30001/update/<vendasPorMinuto>
```
Onde 'vendasPorMinuto' é a quantidade de vendas por minuto que se quer gerar.

### acesso ao mysql - transacional
Caso queira acessar o banco de dados, utilize o exemplo abaixo:
```
$ sudo mysql -h127.0.0.1 -P3306 -uroot -proot
```

### observabilidade
Alguns arquivos de saída são gerados conforme os jobs são executados: saida_extração.log, saida_ingesta.log, saida_batch.log, saida_gold_view.log e saida_gold_view.log

### Recomendações
* VSCode
* Postman


### parando o projeto
Tendo dudo dado certo até aqui, é possível parar o projeto.
Digite o seguinte comando:
```
$ sudo sh stop.sh
```
Aguarde até que todos os containers estejam parados.