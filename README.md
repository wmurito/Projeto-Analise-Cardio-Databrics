
# Relatório Final do Projeto de Engenharia de Dados no Databricks Free Edition

## Introdução

Este relatório documenta o desenvolvimento de um projeto de engenharia de dados completo, utilizando o Databricks Free Edition e um dataset de doenças cardiovasculares fornecido pelo usuário. O objetivo principal foi construir um pipeline robusto para ingestão, limpeza, transformação e análise de dados, demonstrando as capacidades do Databricks para processamento de grandes volumes de dados e preparação para análises mais aprofundadas ou modelagem de Machine Learning.

O projeto seguiu uma abordagem estruturada, dividida em fases, desde a análise inicial do dataset até a criação de visualizações, culminando na documentação detalhada de cada etapa. A arquitetura do pipeline foi baseada no padrão Medallion Architecture (Bronze, Silver, Gold), garantindo a qualidade e a rastreabilidade dos dados em cada estágio do processamento.

## 1. Análise do Dataset e Planejamento do Projeto

Nesta fase, o dataset `cardio_train.csv` foi analisado para compreender sua estrutura, colunas e potenciais desafios de qualidade de dados. O dataset contém 70.000 registros com informações de pacientes, incluindo idade, gênero, altura, peso, pressão arterial, níveis de colesterol e glicose, hábitos como fumo e consumo de álcool, nível de atividade física, e uma variável alvo indicando a presença ou ausência de doença cardiovascular.

### Observações e Desafios Identificados:

*   **Delimitador**: O arquivo utiliza `;` como delimitador.
*   **Idade em Dias**: A coluna `age` é fornecida em dias, necessitando conversão para anos para melhor interpretabilidade.
*   **Variáveis Categóricas Codificadas**: `gender`, `cholesterol` e `gluc` são representadas por códigos numéricos, exigindo mapeamento para descrições textuais.
*   **Inconsistências na Pressão Arterial**: Potenciais outliers e valores ilógicos nas colunas `ap_hi` (sistólica) e `ap_lo` (diastólica), como `ap_lo` > `ap_hi` ou valores clinicamente implausíveis.
*   **Duplicatas e Valores Ausentes**: Necessidade de verificar e tratar duplicatas e valores ausentes (NaNs) em todo o dataset.

### Objetivos Definidos:

1.  **Ingestão de Dados**: Carregar o `cardio_train.csv` para o Databricks.
2.  **Armazenamento Otimizado**: Utilizar o formato Delta Lake para persistência dos dados.
3.  **Limpeza de Dados**: Tratar inconsistências, outliers e duplicatas.
4.  **Transformação de Dados**: Realizar engenharia de features (ex: idade em anos) e mapeamento de categorias.
5.  **Preparação para Análise/Modelagem**: Estruturar os dados para consumo por ferramentas de BI e modelos de ML.
6.  **Análise Exploratória (EDA)**: Obter insights iniciais sobre o dataset.

### Arquitetura do Pipeline (Medallion Architecture):

*   **Camada Bronze (Raw Data)**: Ingestão direta do CSV para Delta Lake, mantendo os dados brutos.
*   **Camada Silver (Cleaned and Conformed Data)**: Aplicação de limpeza e transformações básicas nos dados da camada Bronze, resultando em uma nova tabela Delta Lake.
*   **Camada Gold (Curated Data for Analytics and ML)**: Agregações e preparações finais para consumo por análises e modelos de ML (esta camada será abordada em futuras extensões do projeto, focando principalmente nas camadas Bronze e Silver neste relatório).

## 2. Configuração do Ambiente Databricks

Nesta fase, o ambiente do Databricks Free Edition foi configurado para hospedar o projeto. Os passos essenciais incluíram:

1.  **Criação de Conta**: O usuário confirmou a criação de uma conta no Databricks Free Edition.
2.  **Criação de Cluster**: Um cluster foi provisionado no Databricks. Para o Free Edition, um cluster no modo `Single Node` é adequado para a maioria das tarefas de desenvolvimento e teste.
3.  **Configuração de Acesso ao Armazenamento (Unity Catalog)**: O arquivo `cardio_train.csv` foi carregado para o Unity Catalog do Databricks, especificamente para o caminho `/Volumes/workspace/default/tables/cardio_train.csv`. O Unity Catalog oferece recursos avançados de governança de dados, como controle de acesso granular e linhagem, que são benéficos para projetos de engenharia de dados.

## 3. Desenvolvimento do Pipeline de Ingestão de Dados (Camada Bronze)

O pipeline de ingestão foi implementado em um notebook Databricks (`01_Ingestao_Dados_Cardio`). O principal objetivo desta fase foi ler o arquivo CSV e persistir os dados brutos no formato Delta Lake, formando a camada Bronze.

### Passos Executados:

1.  **Criação do Notebook**: Um novo notebook Python foi criado e anexado ao cluster configurado.
2.  **Leitura do CSV**: O arquivo `cardio_train.csv` foi lido utilizando o Spark DataFrame Reader, especificando o delimitador (`;`), a presença de cabeçalho (`header=true`) e a inferência de schema (`inferSchema=true`).
    ```python
    file_path = "/Volumes/workspace/default/tables/cardio_train.csv"
    df = spark.read \
      .format("csv") \
      .option("header", "true") \
      .option("sep", ";") \
      .option("inferSchema", "true") \
      .load(file_path)
    ```
3.  **Persistência em Delta Lake**: O DataFrame lido foi salvo no formato Delta Lake na camada Bronze, utilizando o modo `overwrite` para garantir que a tabela seja sempre atualizada com a versão mais recente dos dados brutos.
    ```python
    delta_table_path_bronze = "/Volumes/workspace/default/tables/cardio_bronze"
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .save(delta_table_path_bronze)
    ```

**Resultado**: Os dados brutos do `cardio_train.csv` foram ingestados com sucesso e armazenados como uma tabela Delta Lake em `/Volumes/workspace/default/tables/cardio_bronze`.



## 4. Implementação de Transformações e Limpeza de Dados (Camada Silver)

Nesta fase, os dados brutos da camada Bronze foram carregados e submetidos a processos de limpeza e transformação para melhorar sua qualidade e usabilidade. As operações foram realizadas em um novo notebook Databricks (`02_Transformacao_Limpeza_Dados_Cardio`), resultando na criação da camada Silver.

### Passos Executados:

1.  **Carregamento da Camada Bronze**: Os dados foram lidos da tabela Delta Lake da camada Bronze.
    ```python
    delta_table_path_bronze = "/Volumes/workspace/default/tables/cardio_bronze"
    df_bronze = spark.read.format("delta").load(delta_table_path_bronze)
    ```
2.  **Conversão da Idade para Anos**: A coluna `age` (em dias) foi convertida para `age_years` (em anos), arredondada para o inteiro mais próximo.
    ```python
    from pyspark.sql.functions import col, round
    df_silver = df_bronze.withColumn("age_years", round(col("age") / 365.25, 0).cast("integer"))
    ```
3.  **Mapeamento de Variáveis Categóricas**: As colunas `gender`, `cholesterol` e `gluc` foram mapeadas para descrições mais legíveis, utilizando `when` para atribuir rótulos textuais.
    ```python
    from pyspark.sql.functions import when

    df_silver = df_silver.withColumn("gender_mapped", \
        when(col("gender") == 1, "Feminino") \
        .when(col("gender") == 2, "Masculino") \
        .otherwise("Desconhecido")
    )

    df_silver = df_silver.withColumn("cholesterol_mapped", \
        when(col("cholesterol") == 1, "Normal") \
        .when(col("cholesterol") == 2, "Acima do Normal") \
        .when(col("cholesterol") == 3, "Bem Acima do Normal") \
        .otherwise("Desconhecido")
    )

    df_silver = df_silver.withColumn("gluc_mapped", \
        when(col("gluc") == 1, "Normal") \
        .when(col("gluc") == 2, "Acima do Normal") \
        .when(col("gluc") == 3, "Bem Acima do Normal") \
        .otherwise("Desconhecido")
    )
    ```
4.  **Limpeza de Pressão Arterial Inconsistente e Outliers**: Registros com valores inconsistentes ou clinicamente implausíveis para `ap_hi` e `ap_lo` foram filtrados. O número de registros após esta etapa foi de 68672.
    ```python
    df_silver = df_silver.filter(
        (col("ap_lo") <= col("ap_hi")) & \
        (col("ap_hi") >= 70) & (col("ap_hi") <= 250) & \
        (col("ap_lo") >= 40) & (col("ap_lo") <= 180)
    )
    ```
5.  **Remoção de Duplicatas**: Duplicatas foram removidas do dataset. Não foram encontradas duplicatas após a limpeza da pressão arterial, mantendo o número de registros em 68672.
    ```python
    df_silver = df_silver.dropDuplicates()
    ```
6.  **Persistência em Delta Lake (Camada Silver)**: O DataFrame limpo e transformado foi salvo como uma nova tabela Delta Lake, representando a camada Silver.
    ```python
    delta_table_path_silver = "/Volumes/workspace/default/tables/cardio_silver"
    df_silver.write \
      .format("delta") \
      .mode("overwrite") \
      .save(delta_table_path_silver)
    ```

**Resultado**: Os dados foram limpos e transformados, resultando em uma tabela Delta Lake de alta qualidade em `/Volumes/workspace/default/tables/cardio_silver`, pronta para análises e modelagem.



## 5. Criação de Análises e Visualizações (Camada Gold - Preparação para Análise)

Nesta fase, os dados limpos e transformados da camada Silver foram utilizados para realizar análises estatísticas descritivas e criar visualizações, fornecendo insights iniciais sobre o dataset. As operações foram realizadas em um novo notebook Databricks (`03_Analise_Visualizacao_Cardio`).

### Passos Executados:

1.  **Carregamento da Camada Silver**: Os dados foram lidos da tabela Delta Lake da camada Silver.
    ```python
    delta_table_path_silver = "/Volumes/workspace/default/tables/cardio_silver"
    df_silver = spark.read.format("delta").load(delta_table_path_silver)
    ```
2.  **Análise Estatística Descritiva**: Um resumo estatístico das colunas numéricas foi gerado usando o método `describe()` do Spark DataFrame, fornecendo métricas como contagem, média, desvio padrão, mínimo e máximo.
    ```python
    df_silver.describe().show()
    ```
    Esta análise confirmou a integridade dos dados após as transformações e limpezas, mostrando que os valores de idade, peso, altura e pressão arterial estão dentro de faixas esperadas e consistentes.

3.  **Visualização da Distribuição de Idade**: Um histograma foi criado para visualizar a distribuição da idade dos pacientes (`age_years`). Esta visualização revelou que a maioria dos pacientes no dataset está concentrada na faixa etária entre 40 e 65 anos, com picos notáveis em torno dos 50 e 60 anos.
    ```python
    import matplotlib.pyplot as plt
    import seaborn as sns

    df_pandas = df_silver.toPandas()

    plt.figure(figsize=(10, 6))
    sns.histplot(df_pandas["age_years"], bins=15, kde=True)
    plt.title("Distribuição de Idade dos Pacientes")
    plt.xlabel("Idade (Anos)")
    plt.ylabel("Número de Pacientes")
    plt.grid(axis=\'y\', alpha=0.75)
    plt.show()
    ```

4.  **Visualização: Doença Cardiovascular por Gênero**: Um gráfico de barras foi gerado para comparar a incidência de doenças cardiovasculares entre os gêneros (`gender_mapped`). Esta visualização é crucial para entender se há disparidades na prevalência da doença entre homens e mulheres no dataset.
    ```python
    plt.figure(figsize=(8, 5))
    sns.countplot(data=df_pandas, x="gender_mapped", hue="cardio")
    plt.title("Doença Cardiovascular por Gênero")
    plt.xlabel("Gênero")
    plt.ylabel("Número de Pacientes")
    plt.legend(title="Doença Cardiovascular", labels=["Não", "Sim"])
    plt.grid(axis=\'y\', alpha=0.75)
    plt.show()
    ```

**Resultado**: As análises e visualizações forneceram insights valiosos sobre as características demográficas e a prevalência de doenças cardiovasculares no dataset, preparando o terreno para análises mais aprofundadas ou modelagem preditiva.



## Conclusão

Este projeto demonstrou a construção de um pipeline de engenharia de dados no Databricks Free Edition, desde a ingestão de dados brutos até a preparação para análise e visualização. A utilização do Delta Lake e do Unity Catalog no Databricks Free Edition provou ser uma solução eficaz para gerenciar e processar dados de forma escalável e confiável.

As fases do projeto, seguindo a Medallion Architecture, garantiram que os dados fossem progressivamente limpos, transformados e enriquecidos, resultando em uma camada Silver de alta qualidade, pronta para consumo. As análises exploratórias e visualizações iniciais forneceram insights importantes sobre o dataset de doenças cardiovasculares, destacando a distribuição etária dos pacientes e a prevalência da doença por gênero.

### Próximos Passos e Extensões Futuras:

Para futuras extensões deste projeto, sugere-se:

*   **Camada Gold**: Desenvolver a camada Gold com agregações e features mais complexas, otimizadas para casos de uso específicos, como treinamento de modelos de Machine Learning.
*   **Modelagem Preditiva**: Utilizar os dados da camada Silver (ou Gold) para construir e treinar modelos de Machine Learning para prever a presença de doenças cardiovasculares.
*   **Dashboards Interativos**: Criar dashboards interativos utilizando ferramentas como Databricks SQL Analytics ou Power BI/Tableau, conectando-se diretamente à camada Gold.
*   **Automação do Pipeline**: Implementar a automação do pipeline utilizando Databricks Jobs para agendar a execução das transformações de dados.

Este projeto serve como um guia fundamental para o desenvolvimento de soluções de engenharia de dados no ambiente Databricks, fornecendo uma base sólida para projetos mais complexos e avançados.

---

