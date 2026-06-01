# # Laboratório: Stream de Clima com PySpark
# 
# ## Contexto
# Simular um fluxo de dados em tempo real (streaming) usando PySpark.
# 
# A cada micro-batch, coletamos clima de 10 cidades via API e processamos os dados com funções `lambda` no RDD.
# 


# ## 1) Setup do ambiente
# Nesta etapa importamos bibliotecas, criamos a sessão Spark e configuramos o nível de log.
# 


import json
import time
from datetime import datetime
from pathlib import Path

import requests
from pyspark.sql import SparkSession

# Sessão Spark apontando para o cluster (master + worker do docker-compose).
spark = (
    SparkSession.builder
    .appName('stream-clima')
    .master('spark://spark-master:7077')
    .getOrCreate()
)

# WARN reduz ruído e deixa o output mais didático para sala.
spark.sparkContext.setLogLevel('WARN')

print('Spark:', spark.version)



# ## 2) Catálogo fixo de cidades e parâmetros do laboratório
# Aqui definimos:
# - cidades que entram no lote;
# - frequência de disparo do stream;
# - caminho de saída dos arquivos.
# 


CIDADES_PR = [
    {'city': 'Porto Alegre', 'latitude': -30.0346, 'longitude': -51.2177},
    {'city': 'Sao Paulo', 'latitude': -23.5505, 'longitude': -46.6333},
    {'city': 'Rio de Janeiro', 'latitude': -22.9068, 'longitude': -43.1729},
    {'city': 'Belo Horizonte', 'latitude': -19.9167, 'longitude': -43.9345},
    {'city': 'Brasilia', 'latitude': -15.7939, 'longitude': -47.8828},
    {'city': 'Salvador', 'latitude': -12.9777, 'longitude': -38.5016},
    {'city': 'Recife', 'latitude': -8.0476, 'longitude': -34.8770},
    {'city': 'Fortaleza', 'latitude': -3.7319, 'longitude': -38.5267},
    {'city': 'Manaus', 'latitude': -3.1190, 'longitude': -60.0217},
    {'city': 'Belem', 'latitude': -1.4558, 'longitude': -48.4902},
]

TRIGGER_SEGUNDOS = 10

# Timeout de chamada HTTP para a API de clima.
TIMEOUT_API = 10

# Tempo total de observação antes de encerrar automaticamente.
TEMPO_OBS = 60

# Estrutura de saída dentro do notebook (persistida no host via volume).
DATA_DIR = Path('/home/jovyan/notebooks/data')
BASE_PATH = DATA_DIR / 'stream_clima'
CHECKPOINT_PATH = BASE_PATH / 'checkpoints' / 'didatico'
RAW_PATH = BASE_PATH / 'raw'
PROCESSADO_PATH = BASE_PATH / 'processado'
ERROS_PATH = BASE_PATH / 'erros'

BASE_PATH.mkdir(parents=True, exist_ok=True)
CHECKPOINT_PATH.mkdir(parents=True, exist_ok=True)
RAW_PATH.mkdir(parents=True, exist_ok=True)
PROCESSADO_PATH.mkdir(parents=True, exist_ok=True)
ERROS_PATH.mkdir(parents=True, exist_ok=True)

print('Cidades no lote:', len(CIDADES_PR))
print('Base de saída:', BASE_PATH)



# ## 3) Coleta da API e processamento com lambda (RDD)
# 
# Fluxo de cada batch:
# 1. Coletar clima de cada cidade.
# 2. Salvar `raw` (dados brutos da API).
# 3. Processar com `map` + `filter` + `reduceByKey`.
# 4. Salvar `processado`.
# 5. Se houver falha, salvar em `erros`.
# 


def buscar_clima(cidade):
    """Consulta API de clima para uma cidade e devolve 1 registro."""
    url = 'https://api.met.no/weatherapi/locationforecast/2.0/compact'
    params = {'lat': cidade['latitude'], 'lon': cidade['longitude']}

    try:
        r = requests.get(
            url,
            params=params,
            headers={'User-Agent': 'atividade-didatica-pyspark'},
            timeout=TIMEOUT_API,
        )
        r.raise_for_status()

        ts = r.json().get('properties', {}).get('timeseries', [])
        if not ts:
            return {'city': cidade['city'], 'erro': 'sem_timeseries'}

        ponto = ts[0]
        d = ponto.get('data', {}).get('instant', {}).get('details', {})
        return {
            'city': cidade['city'],
            'temperature_2m': float(d['air_temperature']) if d.get('air_temperature') is not None else None,
            'humidity_2m': float(d['relative_humidity']) if d.get('relative_humidity') is not None else None,
            'wind_speed_10m': float(d['wind_speed']) if d.get('wind_speed') is not None else None,
            'api_time': ponto.get('time'),
            'ingest_ts': datetime.utcnow().isoformat(),
        }
    except Exception as e:
        return {'city': cidade['city'], 'erro': str(e)}


def coletar_lote():
    """Monta um lote com as cidades e separa sucesso/erro."""
    ok, erros = [], []
    for cidade in CIDADES_PR:
        r = buscar_clima(cidade)
        if 'erro' in r:
            erros.append(r)
        else:
            ok.append(r)
    return ok, erros


def processar_batch_lambda(batch_id):
    """Processa 1 micro-batch: coleta, transforma com lambda e salva saídas."""
    registros_ok, erros = coletar_lote()

    if not registros_ok:
        print(f'[BATCH {batch_id}] sem registros. erros={len(erros)}')
        if erros:
            err_file = ERROS_PATH / f'erros_batch_{batch_id}_{int(time.time()*1000)}.jsonl'
            with open(err_file, 'w', encoding='utf-8') as f:
                for row in erros:
                    f.write(json.dumps(row, ensure_ascii=False) + '\n')
            print('exemplo_erro:', erros[0])
            print('arquivo_erros:', err_file.name)
        return

    # 1) Saída raw: resposta bruta da coleta (antes do lambda).
    raw_file = RAW_PATH / f'raw_batch_{batch_id}_{int(time.time()*1000)}.jsonl'
    with open(raw_file, 'w', encoding='utf-8') as f:
        for row in sorted(registros_ok, key=lambda x: x['city']):
            f.write(json.dumps(row, ensure_ascii=False) + '\n')

    # Se houver erro parcial de API, salva para auditoria.
    if erros:
        err_file = ERROS_PATH / f'erros_batch_{batch_id}_{int(time.time()*1000)}.jsonl'
        with open(err_file, 'w', encoding='utf-8') as f:
            for row in erros:
                f.write(json.dumps(row, ensure_ascii=False) + '\n')

    rdd = spark.sparkContext.parallelize(registros_ok)

    # 2) Transformações com lambda (pedido da atividade).
    enriquecido = (
        rdd
        .map(lambda d: {
            'cidade': d['city'],
            'temp_c': d['temperature_2m'],
            'umidade': d['humidity_2m'],
            'vento': d['wind_speed_10m'],
        })
        .filter(lambda d: d['temp_c'] is not None and d['umidade'] is not None)
        .map(lambda d: {
            **d,
            'faixa_temp': 'quente' if d['temp_c'] >= 22 else 'ameno',
            'umidade_alerta': 'alta' if d['umidade'] >= 80 else 'normal',
        })
    )

    # Agregação didática usando reduceByKey.
    por_faixa = (
        enriquecido
        .map(lambda d: (d['faixa_temp'], 1))
        .reduceByKey(lambda a, b: a + b)
        .collect()
    )

    amostra = enriquecido.sortBy(lambda d: d['cidade']).collect()

    # 3) Saída processada: já transformada com lambda.
    proc_file = PROCESSADO_PATH / f'processado_batch_{batch_id}_{int(time.time()*1000)}.jsonl'
    with open(proc_file, 'w', encoding='utf-8') as f:
        for row in amostra:
            f.write(json.dumps(row, ensure_ascii=False) + '\n')

    print(f'[BATCH {batch_id}] ok={len(registros_ok)} erro={len(erros)} total_impressos={len(amostra)}')
    print('contagem faixa_temp:', sorted(por_faixa))
    print('arquivos:', raw_file.name, proc_file.name)
    for item in amostra:
        print(item)



# ## 4) Iniciar stream simulado e conectar `foreachBatch`
# Usamos `rate` como relógio para disparar micro-batches e chamar `processar_batch_lambda`.
# 


def stop_query(name):
    """Encerra query antiga com o mesmo nome, se existir."""
    for q in spark.streams.active:
        if q.name == name:
            q.stop()
            print(f'[STOP] {name}')


stop_query('stream_clima')

# Fonte em stream usada apenas como gatilho temporal (clock).
clock_stream = (
    spark.readStream
    .format('rate')
    .option('rowsPerSecond', 1)
    .load()
)

query = (
    clock_stream.writeStream
    .queryName('stream_clima')
    .foreachBatch(lambda _df, batch_id: processar_batch_lambda(batch_id))
    .trigger(processingTime=f'{TRIGGER_SEGUNDOS} seconds')
    .option('checkpointLocation', str(CHECKPOINT_PATH))
    .start()
)

print('Query iniciada:', query.id)
print('Status inicial:', query.status)



# ## 5) Observar execução e encerrar
# A célula abaixo aguarda alguns segundos para acumular batches, mostra queries ativas e encerra a stream.
# 


time.sleep(TEMPO_OBS)

print('Queries ativas:', [q.name for q in spark.streams.active])

# Diagnóstico rápido (útil em laboratório).
for q in spark.streams.active:
    if q.name == 'stream_clima':
        print('status:', q.status)
        print('ultima excecao:', q.exception())

for q in spark.streams.active:
    if q.name == 'stream_clima':
        q.stop()
        print('[STOP] stream_clima')



# criar sessão Spark

from pyspark.sql import SparkSession 

spark = (
    SparkSession.builder 
    .appName("TransformacoesSpark")
    .getOrCreate()
    )

sc = spark.sparkContext


# O map() é utilizado para transformar os elementos de um RDD.
# Neste exemplo, iremos transformar cada dicionário da cidade apenas no nome da cidade

# Converte a lista Python em um RDD Spark

rdd_cidades = sc.parallelize(CIDADES_PR)

# Aplica uma transformação pegando somente o campo 'city'
nomes_cidades = rdd_cidades.map(
    lambda cidade: cidade['city']
)

#Exibe o resultado final no notebook
nomes_cidades.collect()


# O filter() é utilizado para selecionar apenas os elementos que atendem a uma determinada condição
# Neste exemplo, iremos manter apenas cidades com latitude maior que -10. 

# Converte a lista Python em um RDD Spark
rdd_cidades = sc.parallelize(CIDADES_PR)

# Filtra apenas cidades com latitudae maior que -10 
cidades_latitude_maior_que_menos_10 = rdd_cidades.filter(
    lambda cidade: cidade['latitude'] > - 10
)

# Exibe o resultado do filtro 
cidades_latitude_maior_que_menos_10.collect()


# o flatMap() transforma cada elemento em múltiplos elementos. 
# Neste exemplo, nomes compostos das cidades serão separados em palavras individuais

# Converte a lista Python em um RDD Spark 
rdd_cidades = sc.parallelize(CIDADES_PR)

# Divide o nome das cidades em palavras
palavras_dos_nomes = rdd_cidades.flatMap(
    lambda cidade: cidade['city'].split(" ")
)

# Exibe todas as palavras geradas 
palavras_dos_nomes.collect()


# O sample() cria uma amostra dos dados. 
# Neste exemplo, iremos selecionar aproximadamente 40% das cidades do RDD original 

# Converte a lista Python em RDD Spark
rdd_cidades = sc.parallelize(CIDADES_PR)

# Gera uma amostra aleatória: 
# False = sem repetição 
# 0.4 = aproximadamente 40% dos dados
# seed = mantém o mesmo resultado aleatório 
amostra_cidades = rdd_cidades.sample(
    False,
    0.4,
    seed=42
)

# Exibe a amostra gerada 
amostra_cidades.collect()


# O distinct() remove elementos repetidos 
# Neste exemplo, iremos obter apenas as letras iniciais únicas das cidades

# Converte a lista Python em um RDD Spark 
rdd_cidades = sc.parallelize(CIDADES_PR) 

# Pega a primeira letra do nome de cada cidade
iniciais_unicas = rdd_cidades.map(
    lambda cidade: cidade['city'][0]
).distinct()

# Exibe apenas as letras únicas
iniciais_unicas.collect()



# O groupBy() agrupa elementos com base em uma chave
# Neste exemplo, iremos agrupar as cidades pela primeira letra do nome 

# Converte a lista Python em um RDD Spark
rdd_cidades = sc.parallelize(CIDADES_PR)

# Agrupa as cidades pela primeira letra 
cidades_agrupadas_por_inicial = rdd_cidades.groupBy(
    lambda cidade: cidade['city'][0]
)

# Converte os grupos para listas
resultado = cidades_agrupadas_por_inicial.mapValues(list)

# Exibe os grupos criados
resultado.collect()


# O reduceByKey() combina valores que possuem a mesma chave
# Neste exemplo, iremos contar quantas cidades existem para cada letra inicial 

# Converte a lista Python em um RDD Spark 
rdd_cidades = sc.parallelize(CIDADES_PR)

# Cria pares no formato:
# (primeira_letra, 1) 
quantidade_por_inicial = rdd_cidades.map(
    lambda cidade: (cidade['city'][0], 1)
).reduceByKey(
    # Soma os valores de chaves iguais
    lambda a, b: a + b
)

# Exibe a contagem final 
quantidade_por_inicial.collect()





