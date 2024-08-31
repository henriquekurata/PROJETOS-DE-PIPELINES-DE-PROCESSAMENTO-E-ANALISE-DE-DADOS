# ***Apache Cassandra para processamento de análise de dados***
  
  
 ## Ferramentas:
Apache Cassandra

## Passos: 

Já listados junto aos comandos


## Comandos:

## #Instalar Apache Cassandra para RHEL 7

#Criar uma VM com ambiente linux 

#Editar e criar o repositório: 

/etc/yum.repos.d/cassandra.repo

```
[cassandra]
name=Apache Cassandra
baseurl=https://redhat.cassandra.apache.org/41x/noboolean/
gpgcheck=1
repo_gpgcheck=1
gpgkey=https://downloads.apache.org/cassandra/KEYS
```

#Instale o Cassandra, aceitando os prompts de importação da chave gpg:

sudo yum install cassandra

#Inicie o Cassandra (não será iniciado automaticamente):

service cassandra start

#Faça o Cassandra iniciar automaticamente após a 
reinicialização:

chkconfig cassandra on

#Conferir o status

sudo systemctl status cassandra


### Instalar Python 

Fazer o download do arquivo Anaconda Pyhton > No terminal digitar: bash <Nome do arquivo de download> > navegar até cd ~ > source .bashrc (para atualizar as informações do Anaconda)

### Instalar o driver Cassandra

pip install cassandra-driver


### Dados de amostra utilizados 

#datasets (São 30 arquivos)

artist,auth,firstName,gender,itemInSession,lastName,length,level,location,method,page,registration,sessionId,song,status,ts,userId

Mudhoney,Logged In,Aleena,F,10,Kirby,231.57506,paid,"Waterloo-Cedar Falls, IA",PUT,NextSong,1.54102E+12,637,Get Into Yours,200,1.54233E+12,44

Carpenters,Logged In,Aleena,F,11,Kirby,238.39302,paid,"Waterloo-Cedar Falls, IA",PUT,NextSong,1.54102E+12,637,Yesterday Once More,200,1.54233E+12,44



### Script ETL para Extração e Transformação (Carga e Analytics está no outro script)

#etl_app.py

```
# Imports
import os
import glob
import csv
from pipeline import conecta_cluster

# Função para consolidar os arquivos de entrada em um único arquivo
def etl_processa_arquivos():

    print("\nIniciando a Etapa 1 do ETL...")

    # Pasta com os arquivos que serão processados
    current_path = "dados"
    print("\nOs arquivos que serão processados estão na pasta: " + current_path)

    # Lista para o caminho de cada arquivo
    lista_caminho_arquivos = []

    # Loop para extrair o caminho de cada arquivo
    print("\nExtraindo o caminho de cada arquivo.")
    for root, dirs, files in os.walk(current_path):
        lista_caminho_arquivos = glob.glob(os.path.join(root, '*'))

    # Lista para manipular as linhas de cada arquivo
    linhas_dados_all = list()

    # Loop por cada arquivo
    print("\nExtraindo as linhas de cada arquivo e consolidando em um novo arquivo.")
    for file in lista_caminho_arquivos:
        with open(file, 'r', encoding = 'utf8', newline = '') as fh:
            reader = csv.reader(fh)
            next(reader)

            # Append de cada linha de cada arquivo
            for line in reader:
                linhas_dados_all.append(line)

    print("\nEtapa 1 Finalizada. Extração concluída com sucesso.")

    return linhas_dados_all
```


### Função para extrair somente os dados relevantes do arquivo gerado pela função anterior

```
def etl_processa_dados(records):

    print("\nIniciando a Etapa 2 do ETL...")

    # Registra uma estrutura de dados
    csv.register_dialect('dadosGerais', quoting = csv.QUOTE_ALL, skipinitialspace = True)

    # Filtrando os dados relevantes que serão inseridos no Apache Cassandra
    print("\nFiltrando os dados relevantes que serão inseridos no Apache Cassandra.")
    with open('resultado/dataset_completo.csv', 'w', encoding = 'utf8', newline = '') as fh:

        # Cria o objeto
        writer = csv.writer(fh)

        # Executa
        writer.writerow(['artist', 'firstName', 'gender', 'itemInSession', 'lastName', 'length','level', 'location', 'sessionId', 'song', 'userId'])

        # Loop
        for row in records:
            # Se não tiver artista, não geramos a linha final
            if not row[0]:
                continue
            
            # Gravo a linha de dados
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

        print("\nEtapa 2 Finalizada. Transformação concluída com sucesso.")

# Bloco main
if __name__ == '__main__':
    
    # Etapa 1 do ETL
    records_list = etl_processa_arquivos()

    # Se a Etapa 1 foi executada com sucesso, seguimos para a próxima etapa
    if records_list:

        # Etapa 2 do ETL 
        etl_processa_dados(records_list)

        # Conectamos no cluster para a carga de dados no Cassandra
        conecta_cluster()
```



### Carga de dados e Analytics
#pipeline.py

```
# Imports
import csv
import pandas as pd
from cassandra.cluster import Cluster

# Este arquivo não deve ser executado diretamente. Colocamos uma mensagem para lembrar o Engenheiro de Dados.
if __name__ == '__main__':
    print("Este arquivo não deve ser executado diretamente. Execute: etl_app.py")

# Arquivo de dados que será carregado no Apache Cassandra
file_name = 'resultado/dataset_completo.csv'

# Função para criar o cluster Cassandra
def conecta_cluster():

    # Cria a conexão ao cluster Cassandra
    cluster = Cluster(['localhost'])

    # Estabelece a conexão
    session = cluster.connect()

    # Cria a keyspace
    session.execute("CREATE KEYSPACE IF NOT EXISTS projeto1 WITH REPLICATION = "
                    "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")

    # Define o tipo da keyspace
    session.set_keyspace('projeto1')

    # Executa os métodos de Analytics
    pipeline_analytics_1(session)
    pipeline_analytics_2(session)
    pipeline_analytics_3(session)

    # Deleta as tabelas após o Analytics
    # drop_tables(session)

    # Desliga os recursos
    session.shutdown()
    cluster.shutdown()

    print("\nPipeline Concluído com Sucesso. Obrigado!\n")

# Pipeline de Analytics 1 - Busca o artista e o comprimento (tempo) da música do sessionId = 436 e itemInSession = 12
def pipeline_analytics_1(session):

    print("\nIniciando o Pipeline de Analytics 1 (Carga e Análise de Dados)...")

    # Cria a tabela
    query = "CREATE TABLE IF NOT EXISTS tb_session_itemSession "
    
    # Cria as colunas na tabela
    query = query + "(sessionId text, itemInSession text, song text, artist text, length text, " \
                    "PRIMARY KEY (sessionId, itemInSession))"
    
    # Executa
    session.execute(query)

    # Abre o arquivo de entrada para a carga de dados
    with open(file_name, 'r', encoding = 'utf-8') as fh:

        # Leitura do arquivo
        reader = csv.reader(fh)
        next(reader)

        # Loop pelo arquivo e carga na tebal no Cassandra
        for line in reader:
            query = "INSERT INTO tb_session_itemSession (sessionId, itemInSession, song, artist, length)"
            query = query + " VALUES (%s, %s, %s, %s, %s)"
            session.execute(query, (line[8], line[3], line[9], line[0], line[5]))

    # Select nos dados
    query = "SELECT artist, song, length FROM tb_session_itemSession WHERE sessionId = '436' and itemInSession = '12'"
    
    # Converte o resultado em dataframe do Pandas e salva em disco
    df = pd.DataFrame(list(session.execute(query)))
    df.to_csv('resultado/pipeline1.csv', sep = ',', encoding = 'utf-8')
    
    # Print
    print("\nResultado do Pipeline de Analytics 1:\n")
    print(df)

# Pipeline de Analytics 2 - Busca o artista, o nome da música e o usuário do userid = 54 e sessionid = 616
def pipeline_analytics_2(session):

    print("\nIniciando o Pipeline de Analytics 2 (Carga e Análise de Dados)...")

    # Cria a tabela
    query = "CREATE TABLE IF NOT EXISTS tb_user_session "
    query = query + "(userId text, sessionId text, itemInSession text, artist text, song text, firstName text, " \
                    "lastName text, PRIMARY KEY ((userId, sessionId), itemInSession))"
    session.execute(query)

    # Carrega a tabela
    with open(file_name, 'r', encoding = 'utf8') as f:
        reader = csv.reader(f)
        next(reader) 
        for line in reader:
            query = "INSERT INTO tb_user_session (userId, sessionId, itemInSession, artist, song, firstName, lastName)"
            query = query + " VALUES (%s, %s, %s, %s, %s, %s, %s)"
            session.execute(query, (line[10], line[8], line[3], line[0], line[9], line[1], line[4]))

    # Analisa a tabela
    query = "SELECT artist, song, firstname, lastname FROM tb_user_session WHERE userId = '54' and sessionId = '616'"

    # Resultado
    df = pd.DataFrame(list(session.execute(query)))
    df.to_csv('resultado/pipeline2.csv', sep = ',', encoding = 'utf-8')
    print("\nResultado do Pipeline de Analytics 2:\n")
    print(df)

# Pipeline de Analytics 3 - Busca cada usuário que ouviu a música 'The Rhythm Of The Night'
def pipeline_analytics_3(session):

    print("\nIniciando o Pipeline de Analytics 3 (Carga e Análise de Dados)...")

    # Cria a tabela
    query = "CREATE TABLE IF NOT EXISTS tb_user_song "
    query = query + "(song text, userId text, firstName text, lastName text, PRIMARY KEY (song, userId))"
    session.execute(query)

    # Carrega a tabela
    with open(file_name, 'r', encoding = 'utf8') as f:
        reader = csv.reader(f)
        next(reader) 
        for line in reader:
            query = "INSERT INTO tb_user_song (song, userId, firstName, lastName)"
            query = query + " VALUES (%s, %s, %s, %s)"
            session.execute(query, (line[9], line[10], line[1], line[4]))

    # Analisa a tabela
    query = "SELECT firstname, lastname FROM tb_user_song WHERE song = 'The Rhythm Of The Night'"
    
    # Resultado
    df = pd.DataFrame(list(session.execute(query)))
    df.to_csv('resultado/pipeline3.csv', sep = ',', encoding = 'utf-8')
    print("\nResultado do Pipeline de Analytics 3:\n")
    print(df)

# Função para deletar as tabelas ao final do pipeline
def drop_tables(session):

    query = "DROP TABLE tb_session_itemSession"
    session.execute(query)

    query = "DROP TABLE tb_user_session"
    session.execute(query)

    query = "DROP TABLE tb_user_song"
    session.execute(query)

``` 

### Perguntas respondidas com esse pipeline:

#Qual o artista e o comprimento (tempo) da música do sessionId = 436 e itemInSession = 12?

#Quais músicas o usuário do userid = 54 e sessionid = 616 ouviu? Retorne o nome do artista, o nome da música e nome e sobrenome do usuário.

#Quais usuários ouviram a música 'The Rhythm Of The Night'?

#Executando o pipeline:
1. Crie a pasta Projeto1 e coloque lá dentro todos os arquivos do projeto.

2. No terminal, acesse a pasta e execute o pipeline: sudo python etl_app.py


### Trabalhando com CQL

#Instruções CQL

```
DESCRIBE keyspaces;

SELECT * FROM system_schema.keyspaces;

DESCRIBE tables;

DESCRIBE projeto1;

DESCRIBE projeto1.tb_user_session;

SELECT sessionid, COUNT(*) FROM projeto1.tb_user_session WHERE userid IN ('49', '50') GROUP BY sessionid;  

SELECT sessionid, COUNT(*) FROM projeto1.tb_user_session WHERE userid IN ('49', '50') GROUP BY sessionid ALLOW FILTERING;

SELECT sessionid, COUNT(*) FROM projeto1.tb_user_session WHERE userid = '49' GROUP BY sessionid ALLOW FILTERING;
```

#Sair do CQLSH e digitar os comandos abaixo para verificar a saíde do cluster

nodetool tablestats

nodetool describecluster



OBS: Realizado na vm datanode
