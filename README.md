# TabNewsLake
Consumo da API pública do TabNews para criação de um Datalake.

- [Sobre](#sobre)
- [Etapas](#etapas)
    - [1. Ingestão](#1-ingestão)

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

Para coletar os dados estamos utilizando um script Python com a biblioteca [requests](https://pypi.org/project/requests/) no *endpoint*  https://www.tabnews.com.br/api/v1/contents . Onde a busca é feita do *content* mais recente para o mais antigo. Vale dizer que por hora, fazemos uma varredura iterativa até que seja coletado todos os *contents* com a os devidos status atualizado. Isto é, com o valor atualizado de *TabCoins*, número de *contents* filhos, etc.

Foi aberta uma [issue](https://github.com/filipedeschamps/tabnews.com.br/issues/1241) sugerindo a possibilidade de receber os *contents* mais atulizados no lugar dos mais recentes e/ou relevantes, assim, não seria necessário passar por todos *contents* da plataforma para atulizar a base toda.

O script Python criado, na sua primeira viersão, realizaria a ingestão diretamente no **Datalake** utilizando *Apache Spark*, entretando, o custo envolvido par dispobilizar o *cluster* nõ faria sentido para o trabalho. Assim, como alternativa (e melhor opção na realidade), subimos um *delivery stream* no *Kinesis Data Firehose* na AWS. Desta forma, o script Python realiza um PUT diretamente no *Firehose*, onde o mesmo entrega o dado no S3 em formato JSON.

Levou um tempo para descobrir o formato ideal de como enviar os dados para o *Firehose* de maneira que o *Apache Spark* fosse capaz de ler, mas deu certo!

