# 🚀 ***Construção de ambiente com MongoDB para análise de dados com Python***

## 📖 **Descrição do Projeto:**
Este projeto envolve a criação de um ambiente MongoDB para análise de dados com Python, incluindo a instalação do MongoDB, conexão com Python utilizando o pacote `pymongo`, e a construção de um pipeline ETL para ingestão de dados JSON e organização em coleções MongoDB.


## Principais Funcionalidades:
- Configuração de ambiente MongoDB.
- Conexão Python-MongoDB usando `pymongo`.
- Pipeline ETL para ingestão de dados JSON.
- Armazenamento de dados em coleções MongoDB.
- Visualização dos dados via MongoDB Compass.

## 🛠️ Ferramentas Utilizadas:
- **MongoDB:** Banco de dados NoSQL.
- **Python:** Linguagem de programação para construção do pipeline ETL.
- **MongoDB Compass:** Interface gráfica para visualização de dados.
- **pymongo:** Driver Python para MongoDB.


## 📋 **Descrição do Processo:**
1. **Instalação do MongoDB:**
   - Download da versão Community Edition e configuração das variáveis de ambiente.
   - Criação do banco de dados para administração e inicialização do MongoDB.
   - Instalação e inicialização do MongoDB Compass para gerenciamento visual.

2. **Conexão do Python ao MongoDB:**
   - Conexão ao MongoDB utilizando `pymongo`.
   - Implementação de um pipeline ETL que carrega dados JSON de capítulos e versos de um livro.
   - Criação de coleções no MongoDB e inserção dos dados.

3. **Pipeline ETL:**
   - Importação de dados JSON e transformação para estrutura MongoDB.
   - Criação de coleções para cada capítulo e inserção dos respectivos versos.



## 💻 **Comandos:** 

### Instalando o MongoDB

Fazer o download no mongodb community edition> tar -xvf <nome download> > sudo mv <nome do arquivo de download descompactado> /opt/mongodb

#### Variáveis de ambiente:

cd ~

vi .bashrc

export MONGO_HOME=/opt/mongodb

export PATH=$PATH:$MONGO_HOME/bin

#### Criar o banco de dados para administração do MongoDB:

cd ~

mkdir mongodb

#### Inicializando o banco de dados

mongod --dbpath /home/aluno/mongodb

OBS: Caso seja necessário, antes de inicializar o mongodb, pode ser preciso limpar o diretório cd /tmp e liberar o PID em uso anterior pelo mongodb (sudo kill <PID>)


---


### Instalando Mongodbcompass

Fazer o download para linux (Extensão .rpm)

wget https://downloads.mongodb.com/compass/mongodb-compass-1.43.5.x86_64.rpm

sudo yum install mongodb-compass-1.43.5.x86_64.rpm

#### Inicializando o monogodbcompass

mongodb-compass --no-sandbox

#### Conexão do Python ao banco de dados Mongodb

Para conectar um script Python em qualquer banco de dados precisamos de um driver, um pequeno software que possui as bibliotecas de conexão ao banco de dados. 

A conexão entre Python e MongoDB pode ser feita através do pacote pymongo, que traz o driver necessário para a conexão:

pip install pymongo

---

### Pipeline ETL para ingestão de dados
#pipeline.py

```py
# Imports
import os
import json
from pymongo import MongoClient
from encodings import utf_8

print("\nIniciando o Pipeline ETL..." )

# Conexão ao MongoDB
client = MongoClient('mongodb://localhost:27017')

print("\nConexão ao MongoDB Feita com Sucesso.")

# Deleta o banco de dados (se existir)
client.drop_database('projeto2')

# Define o banco de dados (será criado se não existir)
db = client.projeto2

print("Banco de Dados Criado com Sucesso.")

# Abre o arquivo com os nomes dos capítulos do livro
arquivo_capitulos = open('dados/capitulos.json', 'r', encoding = 'utf-8')

print("Carregando os Títulos de Cada Capítulo.")

# Carrega o dataset como formato JSON em uma variável Python
conteudo_arquivo_capitulos = json.load(arquivo_capitulos)

# Lista para receber os dados (títulos dos capítulos)
lista_titulos_capitulos = []

# Extrai cada título de cada capítulo e coloca em uma lista
for capitulo in conteudo_arquivo_capitulos:

    # Para cada linha do arquivo extrai duas colunas, nome do capítulo e id
    lista_titulos_capitulos.append((capitulo['transliteration'], capitulo['id']))

print("Criando Uma Coleção no MongoDB Para Cada Título.")

# Loop pela lista de capítulos
for capitulo in lista_titulos_capitulos:

    # Para cada título, cria uma coleção no banco de dados do MongoDB
    db.create_collection(capitulo[0])

    # Prepara o documento com os campos da coleção (número do verso e texto do verso)
    documento = {'number' : capitulo[1], 'translation': capitulo[0]}

    # Insere o documento
    db.summary.insert_one(documento)

print("Carregando o Arquivo com os Versos em Francês.")

# Define o arquivo com os versos em Francês
arquivo_versos = open('dados/versos_fr.json', 'r', encoding = 'utf-8')

# Carrega o arquivo no formato JSON
conteudo_arquivo_versos = json.load(arquivo_versos)

print("Para Cada Título (Coleção), Carregando os Versos Como Documento:\n")

# Para cada título (cada coleção), carrega os dados (documentos) dos versos do livro
for versos in conteudo_arquivo_versos:

    # Condicional que verifica se o título do capítulo no arquivo de versos é o mesmo na lista de títulos dos capítulos
    if versos['transliteration'] in [nome[0] for nome in lista_titulos_capitulos]:

        # Define o nome da Coleção
        collection = versos['transliteration']
        print(collection)

        # Extrai os versos daquele capítulo
        verses = versos['verses']

        # Lista de documentos (versos)
        documentos = []

        # Loop para carregar os versos em cada capítulo (coleção)
        for verse in verses:

            # ID do verso
            aya_number = verse['id']

            # Texto do verso
            translation = verse['translation']

            # Documento
            documento = {'aya_number' : aya_number, 'translation': translation}

            # Adiciona à lista de documentos (versos)
            documentos.append(documento)

        # Carrega os versos do capítulo corrente
        db.get_collection(collection).insert_many(documentos)

print("\nPipeline ETL Concluído com Sucesso.\n")
```

#### Amostra de dados utilizado

#dados

#capitulos.json

[{"id":1,"name":"الفاتحة","transliteration":"Al-Fatihah","type":"meccan","total_verses":7,"link":
"https://cdn.jsdelivr.net/npm/quran-json@3.1.2/dist/chapters/1.json"},

{"id":2,"name":"البقرة",
"transliteration":"Al-Baqarah","type":"medinan","total_verses":286,"link":"https://cdn.jsdelivr.net
/npm/quran-json@3.1.2/dist/chapters/2.json"},

{"id":3,"name":"آل عمران","transliteration":"Ali 'Imran",
"type":"medinan","total_verses":200,"link":"https://cdn.jsdelivr.net/npm/quran-json@3.1.2/dist/chapters/3.json"}



#Versos.json

[{"id":1,"name":"الفاتحة","transliteration":"Al-Fatihah","translation":"L'ouverture","type":"meccan","total_verses":7,"verses"
:[{"id":1,"text":"بِسۡمِ ٱللَّهِ ٱلرَّحۡمَٰنِ ٱلرَّحِيمِ","translation":"Au nom d'Allah, le Tout Miséricordieux, le Très Miséricordieux"},

{"id":2,
"text":"ٱلۡحَمۡدُ لِلَّهِ رَبِّ ٱلۡعَٰلَمِينَ","translation":"Louange à Allah, Seigneur de l'univers"},

{"id":3,"text":"ٱلرَّحۡمَٰنِ ٱلرَّحِيمِ","translation":"Le
Tout Miséricordieux, le Très Miséricordieux"}

---

Criar a pasta na máquina virtual "Projeto 2" e inserir o pipeline e os dados.

Executar: Python pipeline.py > Verificar o resultado com o mongodbcompass.

Obs: Alternativas para análise de dados com o Mongodb: Mongodb Charts (Apenas em nuvem) / Power BI / Bibliotecas Python para criação de gráficos


---
## 📞 **Contato**

Se tiver dúvidas ou sugestões sobre o projeto, entre em contato comigo:

- 💼 [LinkedIn](https://www.linkedin.com/in/henrique-k-32967a2b5/)
- 🐱 [GitHub](https://github.com/henriquekurata?tab=overview&from=2024-09-01&to=2024-09-01)