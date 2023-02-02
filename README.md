# TabNewsLake
Consumo da API pública do TabNews para criação de um Datalake.

- [Sobre](#sobre)
- [Etapas](#etapas)
    - [1. Ingestão](#1-ingestão)
    - [2. Consumo e criação de tabela em Bronze](#2-consumo-e-criação-de-tabela-em-bronze)
- [Próximos passos](#próximos-passos)
- [Bônus](#bônus)
    - [Estatísticas por data](#estatísticas-por-data)
    - [Estatísticas por usuário](#estatísticas-por-usuário)


<img src="https://github.com/TeoMeWhy/TabNewsLake/blob/main/img/tabnewslake_arch.png" alt="Arquitetura TabNews Lake" width="650">


## Sobre

Essa iniciativa tem como objetivo as seguintes ações em relação aos dados:

- Coletar
- Armazenar
- Organizar
- Analisar

Bem como compartilhar os resultados obtidos e fazer uso dos mesmos dados para treinamentos voltados à área de Engenharia, Ciência e Análise de dados.

## Etapas

### 1. Ingestão

No primeiro momento estamos coletando os dados diretamente da API fornecida pelo time da [TabNews](https://www.tabnews.com.br/). Utilizamos como referência este [post](https://www.tabnews.com.br/GabrielSozinho/documentacao-da-api-do-tabnews) do GabrielSozinho.

Para coletar os dados estamos utilizando um script Python com a biblioteca [requests](https://pypi.org/project/requests/) no *endpoint*  https://www.tabnews.com.br/api/v1/contents . Onde a busca é feita do *content* mais recente para o mais antigo. Vale dizer que por hora, fazemos uma varredura iterativa até que sejam coletados todos os *contents* com os devidos status atualizado. Isto é, com o valor atualizado de *TabCoins*, número de *contents* filhos, etc.

Foi aberta uma [issue](https://github.com/filipedeschamps/tabnews.com.br/issues/1241) sugerindo a possibilidade de receber os *contents* mais atulizados no lugar dos mais recentes e/ou relevantes, assim, não seria necessário passar por todos *contents* da plataforma para atualizar a base toda.

```python
def get_data(**kwargs):
    url = "https://tabnews.com.br/api/v1/contents"
    resp = requests.get(url, params=kwargs)
    return resp
```

O script Python criado, em sua primeira versão, realizaria a ingestão diretamente no **Datalake** utilizando *Apache Spark*, entretando, o custo envolvido para disponibilizar o *cluster* não faria sentido para o trabalho. Assim, como alternativa (e melhor opção na realidade), subimos um *delivery stream* no *Kinesis Data Firehose* na AWS. Desta forma, o script Python realiza um PUT diretamente no *Firehose*, onde o mesmo entrega o dado no S3 em formato JSON.

```python
def save_with_firehose(data, firehose_client):

    data = [[i] for i in data]

    d = json.dumps(data)[1:-1].replace("], [", "]\n[") + "\n"

    firehose_client.put_record(
        DeliveryStreamName="tabnews-contents",
        Record={"Data": d},
    )

    return None
```

Levou um tempo para descobrir o formato ideal de como enviar os dados para o *Firehose* de maneira que o *Apache Spark* fosse capaz de ler, mas deu certo!

Para executar o script python basta:

```bash
$ python src/raw/contents.py --help
usage: contents.py [-h] [--date DATE] [--save_type {firehose,spark}]

optional arguments:
  -h, --help            show this help message and exit
  --date DATE           Data limite para busca (mais antiga): YYYY-MM-DD
  --save_type {firehose,spark}
                        Modo de salvar os dados
```

Assim, caso deseje coletar dados do dia atual até 01/06/2022, enviando os dados para seu *delivery stream*:

```
$ python src/raw/contents.py --date 2022-06-01 --save_type firehose
```

### 2. Consumo e criação de tabela em Bronze

Estamos trabalhando com camadas de dados. Isto é, separando em níveis a qualidade de nosso dado. O S3 onde o dado é entregue pelo *Firehose* chamamos de **Raw**. Nesta camada, pode haver dados duplicados, com **muitos** arquivos (pois é onde chega tudo no formato mais cru) e com possíveis inconsistências.

Para criar tabelas que sejam mais fáceis de se trabalhar, e aplicar a delicinha do *SQL*, criamos a camada **Bronze**, utilizando *Apache Spark Structed Streaming* com *Auto Loader* do Databricks. Com esses carinhas, podemos ler os dados em Raw e gravá-los em **Bronze** no formato [Delta](https://delta.io/), facilitando e otimizando a leitura, bem como garantindo maior integridade dos dados.

Ainda que o cluster de *Apache Spark* não fique ligado 24/7 esperando os dados de Raw chegarem, utilizamos o *CloudFiles* com Stream, o que facilita nossa vida, pois o mesmo é capaz de gerenciar quais arquivos ele já leu e quais não, iteração por iteração. Ou seja, ao realizar o processamento dos dados em Raw, ele lê, processa e armazena em **Bronze** os dados, na sua próxima iteração (agendada), haverão dados novos (ou não) em **Raw**, assim, ele lerá apenas esses novos dados e fará seu processo normalmente (mesmo que estejam no mesmo *bucket://folder* que os dados antigos, ignorando-os).

```python
df_stream = (spark.readStream
                  .format("cloudFiles")
                  .schema(schema)
                  .option("cloudFiles.format", "json")
                  .load(raw_path))

stream = (df_stream.writeStream
                  .trigger(once=True)
                  .option("checkpointLocation", checkpoint_path)
                  .foreachBatch(lambda df, batchId: upsert(df, df_delta, table))
                  .start())
```

## Próximos passos

- [ ] Realizar estatísticas descritivas dos posts e usuários da plataforma

## Bônus

Algumas estatísticas a partir dos dados coletados:

### Estatísticas por data

Quantidade histórica de conteúdos postados e usuários distintos realizando posts:

<img src="https://github.com/TeoMeWhy/TabNewsLake/blob/main/img/hist_qtde.png" alt="Quantidade histórica diária de conteúdo postado e usuários" width="650">

Claro que há interesse em entender o tamanho atual desta plataforma, assim podemos considerar os valores acumulados:

<img src="https://github.com/TeoMeWhy/TabNewsLake/blob/main/img/hist_qtde_acum.png" alt="Quantidade histórica acumulada de conteúdo postado e usuários" width="650">

### Estatísticas por usuário

Bora acompanhar os top 10 usuários que mais realizando posts por aqui?

<img src="https://github.com/TeoMeWhy/TabNewsLake/blob/main/img/post_users.png" alt="Top 10 usuários de posts e seus respectivos números" width="650">

Podemos fazer o mesmo para TabCoins? Está na mão!

<img src="https://github.com/TeoMeWhy/TabNewsLake/blob/main/img/tabcoins.png" alt="Top 10 usuários de tabcoins em posts e seus respectivos números" width="650">


Por enquanto é isso.