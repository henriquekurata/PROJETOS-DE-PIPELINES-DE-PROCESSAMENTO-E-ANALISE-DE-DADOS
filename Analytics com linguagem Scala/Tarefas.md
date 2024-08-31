# ***Processamento de dados com linguagem Scala no Apache Spark*** 

## Ferramentas: 

Java JDK, interpretador da linguagem Scala e Spark.

## Passos:

Já listados junto aos comandos.


## Comandos:

### Instalar Java 8 e Scala

Fazer o Download do Java 8 pela documentação Java 

Fazer o Download do interpretador da linguagem Scala

Para Instalar em máquina Windows: colocar o arquivo Scala e Java no drive C > Acessar variáveis de ambiente > Variáveis de usuário > New > Add "Scala" como o caminho do arquivo > Add "Path" com o caminho do JDK/bin

Obs: Os binários do Java devem estar acima dos binários Scala nas configurações de ambiente

## Instalar Apache Spark

Fazer o Download do Apache Spark direto da documentação
Para Instalar em máquina Windows: colocar o arquivo Spark no drive C > Acessar variáveis de ambiente > Variáveis de ambiente >  Add "Path" com o caminho do Spark (C:\Spark\spark-3.1.2-bin-hadoop2.7\bin) > 
Add "Path" com o caminho do Hadoop (C:\Hadoop\bin - esse diretório ficara vazio mesmo) 

### Projeto1 - Pipeline de ML com Scala e Apache Spark - Treinamento supervisionado

Navegar até a pasta onde estão os arquivos > iniciar o cluster com "spark-shell" > Para rodar o pipeline basta digitar :load <nome do arquivo>


#PipelineSeg.scala - Treinamento não supervisionado - 
Pipeline de Machine Learning Para Segmentação de Clientes

```
import org.apache.spark.sql.SparkSession

// Definindo o log de erro
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

// Criando sessão Spark
val spark = SparkSession.builder().getOrCreate()

// Importando o algoritmo K-Means
import org.apache.spark.ml.clustering.KMeans

// Carregando o dataset
val dataset = spark.read.option("header","true").option("inferSchema","true").csv("dados.csv")

// Selecionamos as seguintes colunas para o conjunto de treinamento:
// Fresh, Milk, Grocery, Frozen, Detergents_Paper, Delicatessen
val feature_data = dataset.select($"Fresh", $"Milk", $"Grocery", $"Frozen", $"Detergents_Paper", $"Delicatessen")
println(dataset.schema)

// Import VectorAssembler e Vectors
import org.apache.spark.ml.feature.{VectorAssembler,StringIndexer,VectorIndexer,OneHotEncoder}
import org.apache.spark.ml.linalg.Vectors

// Criamos um novo objeto VectorAssembler chamado assembler para a coluna de atributos
// Lembre-se de que não existe uma coluna de labels em problemas de aprendizado não supervisionado
val assembler = new VectorAssembler().setInputCols(Array("Fresh", "Milk", "Grocery", "Frozen", "Detergents_Paper", "Delicatessen")).setOutputCol("features")

// Usamos o objeto assembler para transformar o feature_data
// Chamamos este novo objeto de dataset
val dataset = assembler.transform(feature_data).select("features")

// Criando o modelo Kmeans com K = 3
val kmeans = new KMeans().setK(3).setSeed(1L)

// Fit do modelo
val model = kmeans.fit(dataset)

// Previsões
val previsoes = model.transform(dataset)

// Criamos o avaliador de cluster
import org.apache.spark.ml.evaluation.ClusteringEvaluator
val evaluator = new ClusteringEvaluator()

// Avaliaremos o modelo usando o Silhouette Score.
// O Silhouette Score é um coeficiente dentro do intervalo [-1, 1]. 
// Valor próximo de 1 significa que os clusters são muito densos e bem separados. 
// Valor próximo de 0 significa que os clusters estão sobrepostos. 
// Valor inferior a 0 significa que os dados pertencentes aos clusters podem estar errados/incorretos.
val silhouette = evaluator.evaluate(previsoes)
println(s"Silhouette Score = $silhouette")

// Mostra os resultados
println("Segmentos de Clientes (Clusters): ")
previsoes.collect().foreach(println)

// Salva o resultado em disco
import java.io._
val writer = new BufferedWriter(new FileWriter("previsoes.txt"))
writer.write("Fresh, Milk, Grocery, Frozen, Detergents_Paper, Delicatessen, Grupo\n")
previsoes.collect().foreach(x=>{writer.write(x.toString())})
writer.close()

```

### Dados de amostra utilizados 

#dados

Channel,Region,Fresh,Milk,Grocery,Frozen,Detergents_Paper,
Delicatessen

2,3,12669,9656,7561,214,2674,1338

2,3,7057,9810,9568,1762,3293,1776

2,3,6353,8808,7684,2405,3516,7844

1,3,13265,1196,4221,6404,507,1788




### Projeto 2 - Consolidate e remoção de  Stopwords

#Pipeline ETL Para Processamento de Linguagem Natural com Linguagem Scala e Apache Spark

```
// Imports
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature._

// Cria a sessão spark
val ss = SparkSession.builder().master("local").appName("MP3").getOrCreate()

// Para conversão implícita de RDD para DataFrame
import ss.implicits._  

// Carregando os arquivos individuais

// Diretório corrente
val currentDir = System.getProperty("user.dir")  

// Arquivos de entrada
val inputFile = "./attributes.csv + ./product_description.csv + ./train.csv"

println("Carregando os arquivos: " + inputFile)
println

// Dataframe com os dados de treino
val trainDF = ss.read.format("csv").option("header", "true").load("train.csv")
trainDF.printSchema()
    
// Dataframe com a descrição dos produtos
val descriptionDF = ss.read.format("csv").option("header", "true").load("product_descriptions.csv")
descriptionDF.printSchema()
    
// Dataframe com os atributos dos produtos
val attributesDF = ss.read.format("csv").option("header", "true").load("attributes.csv")
attributesDF.printSchema()

// Filtra os atributos por uma das brands
val newAttributesDF = attributesDF.filter(attributesDF("name")==="MFG Brand Name")
val newNewAttributesDF = newAttributesDF.select("product_uid","value")

// Consolida os dataframes
val consolidated = trainDF.join(descriptionDF, "product_uid").join(newNewAttributesDF, "product_uid")
      .select(trainDF("product_uid"), trainDF("product_title"), trainDF("search_term"),
      trainDF("relevance"), descriptionDF("product_description"), newNewAttributesDF("value"))
      .withColumn("product_description",lower(col("product_description"))).withColumn("product_title", lower(col("product_title")))
      .withColumn("search_term", lower(col("search_term"))).withColumn("value", lower(col("value")))

// Limpa os dataframes intermediários para liberar memória
trainDF.unpersist()
descriptionDF.unpersist()
attributesDF.unpersist()
newAttributesDF.unpersist()
newNewAttributesDF.unpersist()

// Visualiza
consolidated.show(10)

// Pré-Processamento

// Tokenização da variável product_title

// Cria o tokenizador
val tokenizerTitle = new Tokenizer().setInputCol("product_title").setOutputCol("product_title_words")

// Aplica o tokenizador
val tokenizedTitle = tokenizerTitle.transform(consolidated)
 
// Libera o dataframe intermediário
consolidated.unpersist()

// Seleciona as colunas
tokenizedTitle.select("product_title", "product_title_words")

// Função para remover stopwords
val removerTitle = new StopWordsRemover()
      .setInputCol("product_title_words")
      .setOutputCol("filtered_title_words")

// Une as sequências de palavras em um array de strings
val joinSeq = udf { (words: Seq[String]) => words.mkString(" ") }

// Remove as stopwords
val removedStopwordsTitle = removerTitle.transform(tokenizedTitle)

// Libera o dataframe intermediário
tokenizedTitle.unpersist()

// Junção das sequências como array de strings
val removedStopwordsTitleJoinedSeq = removedStopwordsTitle.withColumn("filtered_title_words", joinSeq($"filtered_title_words"))

// Libera o dataframe intermediário
removedStopwordsTitle.unpersist()

// Tokenização da variável product_description

val tokenizerDesc = new Tokenizer().setInputCol("product_description").setOutputCol("product_description_words")

val tokenizedDesc = tokenizerDesc.transform(removedStopwordsTitleJoinedSeq)

tokenizedDesc.select("product_description", "product_description_words")

val removerDesc = new StopWordsRemover()
      .setInputCol("product_description_words")
      .setOutputCol("filtered_description_words")

val removedStopwordsDesc = removerDesc.transform(tokenizedDesc)

tokenizedDesc.unpersist()

val removedStopwordsDescJoinedSeq = removedStopwordsDesc.withColumn("filtered_description_words", joinSeq($"filtered_description_words"))

removedStopwordsDesc.unpersist()

// Tokenização da variável search_term

val tokenizerSearch = new Tokenizer().setInputCol("search_term").setOutputCol("search_term_words")

val tokenizedSearch = tokenizerSearch.transform(removedStopwordsDescJoinedSeq)

removedStopwordsDescJoinedSeq.unpersist()

tokenizedSearch.select("search_term", "search_term_words")

val removerSearch = new StopWordsRemover()
      .setInputCol("search_term_words")
      .setOutputCol("filtered_search_words")


val removedStopwordsSearch = removerSearch.transform(tokenizedSearch)

tokenizedSearch.unpersist()

val removedStopwordsSearchJoinedSeq = removedStopwordsSearch.withColumn("filtered_search_words", joinSeq($"filtered_search_words"))

removedStopwordsSearch.unpersist()

// Dataframe final após a tokenização
removedStopwordsSearchJoinedSeq.show(10)
removedStopwordsSearchJoinedSeq.printSchema()

// Verificamos se o título contém alguma palavra que foi usada nos termos de busca
val commonterms_SearchVsTitle = udf((filtered_search_words: String, filtered_title_words:String) =>
      if (filtered_search_words.isEmpty || filtered_title_words.isEmpty){
        0
      }
      else{
        var tmp1 = filtered_search_words.split(" ")
        var tmp2 = filtered_title_words.split(" ")
        tmp1.intersect(tmp2).length
      })

// Verificamos se a descrição contém alguma palavra que foi usada nos termos de busca
val commonterms_SearchVsDescription = udf((filtered_search_words: String, filtered_description_words:String) =>
      if (filtered_search_words.isEmpty || filtered_description_words.isEmpty){
        0
      }
      else{
        var tmp1 = filtered_search_words.split(" ")
        var tmp2 = filtered_description_words.split(" ")
        tmp1.intersect(tmp2).length
      })

// Contamos se as descrições e títulos contém os termos usados para busca
val countTimesSearchWordsUsed = udf((filtered_search_words: String, filtered_title_words:String, filtered_description_words:String) =>
      if (filtered_search_words.isEmpty || filtered_title_words.isEmpty){
        0
      }
      else{
        var tmp1 = filtered_search_words
        var count = 0
        if (filtered_title_words.contains(filtered_search_words)){
          count += 1
        }
        if (filtered_description_words.contains(filtered_search_words)){
          count += 1
        }
        count
      })

// Concatenamos os resultados das variáveis após o pré-processamento

// Palavras comuns entre filtered_search_words e filtered_title_words
val results = removedStopwordsSearchJoinedSeq.withColumn("common_words_ST", commonterms_SearchVsTitle($"filtered_search_words", $"filtered_title_words"))
    results.select("common_words_ST").show()
    results.printSchema()

// Palavras comuns entre filtered_search_words e filtered_description_words
val results2 = removedStopwordsSearchJoinedSeq.withColumn("common_words_SD", commonterms_SearchVsDescription($"filtered_search_words", $"filtered_description_words"))
    results2.select("common_words_SD").show()
    results2.printSchema()

// Concatenamos os resultados
val results1and2 = results.withColumn("common_words_SD", commonterms_SearchVsDescription($"filtered_search_words", $"filtered_description_words"))
    results1and2.printSchema()
    results.unpersist()
    results2.unpersist()

// Removemos caracteres especiais e stopwords
val newConsolidated = results1and2
      .withColumn("search_term_len", size(split('filtered_search_words, " ")))
      .withColumn("product_description_len", size(split('filtered_description_words, " ")))
      .withColumn("ratio_desc_len_search_len", size(split('filtered_description_words, " "))/size(split('filtered_search_words, " ")))
      .withColumn("ratio_title_len_search_len", size(split('filtered_title_words, " "))/size(split('filtered_search_words, " ")))
      .withColumn("common_words_ST", $"common_words_ST")
      .withColumn("common_words_SD", $"common_words_SD")
    results.unpersist()

newConsolidated.show(10)

// Converte para dataframe
val df = newConsolidated.toDF()
df.printSchema()
df.show(10)

// Salva o resultado em disco
df.write.format("parquet").save("novo_dataset")

```

### Valores gerados pelo scripit do projeto 2

#attributes

product_uid,"name","value"												

100001,"Bullet01","Versatile connector for various 90Â° connections and home repair projects"												

100001,"Bullet02","Stronger than angled nailing or screw fastening alone"												

100001,"Bullet03","Help ensure joints are consistently straight and strong"												


#product_descriptions

product_uid,"product_description"																				
100001,"Not only do angles make joints stronger, they also provide more consistent, straight corners. Simpson Strong-Tie offers a wide variety of angles in various sizes and thicknesses to handle light-duty jobs or projects where a structural connection is needed. Some can be bent (skewed) to match the project. For outdoor projects or those where moisture is present, use our ZMAX zinc-coated connectors, which provide extra resistance against corrosion (look for a ""Z"" at the end of the model number).Versatile connector for various 90 connections and home repair projectsStronger than angled nailing or screw fastening aloneHelp ensure joints are consistently straight and strongDimensions: 3 in. x 3 in. x 1-1/2 in.Made from 12-Gauge steelGalvanized for extra corrosion resistanceInstall with 10d common nails or #9 x 1-1/2 in. Strong-Drive SD screws"																				
100002,"BEHR Premium Textured DECKOVER is an innovative solid color coating. It will bring your old, weathered wood or concrete back to life. The advanced 100% acrylic resin formula creates a durable coating for your tired and worn out deck, rejuvenating to a whole new look.  For the best results, be sure to properly prepare the surface using other applicable BEHR products displayed above.California residents: see&nbsp	Proposition 65 informationRevives wood and composite decks, railings, porches and boat docks, also great for concrete pool decks, patios and sidewalks100% acrylic solid color coatingResists cracking and peeling and conceals splinters and cracks up to 1/4 in.Provides a durable, mildew resistant finishCovers up to 75 sq. ft. in 2 coats per gallonCreates a textured, slip-resistant finishFor best results, prepare with the appropriate BEHR product for your wood or concrete surfaceActual paint colors may vary from on-screen and printer representationsColors available to be tinted in most storesOnline Price includes Paint Care fee in the following states: CA, CO, CT, ME, MN, OR, RI, VT"																			

#train

id,"product_uid","product_title","search_term","relevance"			

2,100001,"Simpson Strong-Tie 12-Gauge Angle","angle bracket",3			

3,100001,"Simpson Strong-Tie 12-Gauge Angle","l bracket",2.5			






### Projeto 3 -  Estruturação de dados - Criando um schema para os dados

```
// Imports
import spark.implicits._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.Row

// Função para carregar o arquivo
val readFile = (loc: String) => { val rdd = sc.textFile(loc) 
  | rdd 
  | }

// Executa a função e carrega o arquivo
val rdd = readFile("clientes.txt")

// Mostra os dados carregados
rdd.toDF.show()

// Define o schema dos dados
def dfSchema(columnNames: List[String]): StructType =
  StructType(
    Seq(
      StructField(name = columnNames(0), dataType = IntegerType, nullable = false),
      StructField(name = columnNames(1), dataType = StringType, nullable = false),
      StructField(name = columnNames(2), dataType = StringType, nullable = false),
      StructField(name = columnNames(3), dataType = StringType, nullable = false),
      StructField(name = columnNames(4), dataType = IntegerType, nullable = false)
    )
  )

// Extrai o schema dos dados
val schema = dfSchema(List("ID", "Nome", "Cidade", "Estado", "CEP"))

// Aplica o schema ao RDD e converte em dataframe
val rowRDD = rdd.map(_.split(", ")).map(p => Row(p(0).toInt, p(1), p(2), p(3), p(4).toInt))
val df = spark.createDataFrame(rowRDD, schema)

// Mostra os nomes dos clientes
df.select("Nome").show()

// Adiciona uma coluna ao dataframe com um valor padrão
val df2 = df.withColumn("Status", lit("Ativo"))

// Visualiza o resultado final
df2.show()

```

### Valores gerados pelo scripit do projeto 3

#clientes.txt
1000, Bob Silva, Fortaleza, CE, 78727900
2000, Ted Moreira, Pernambuco, RE, 75201900
3000, Mary Jones, Natal, RN, 77028900
4000, Ana Pereira, Fortaleza, CE, 78227900
5000, James Gordon, Fortaleza, CE, 78727900
