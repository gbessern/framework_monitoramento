import json
from percentis import *
from performance import *


##################################################################
## Carrega parametros
##################################################################
def load_param_exec(params_exec_json):
    """
    Carrega os parâmetros de execução (params_exec) e retorna as variáveis extraídas.

    Parâmetros:
    - params_exec_json (str): JSON contendo os parâmetros de execução.

    Retorno:
    - dict com as variáveis extraídas.
    """
    params_exec = json.loads(params_exec_json)
    
    incluir_baseline = params_exec.get("incluir_baseline", False)
    data_inicio = params_exec["periodo_monitoramento"]["data_inicio"]
    data_fim = params_exec["periodo_monitoramento"]["data_fim"]
    
    return {
        "incluir_baseline": incluir_baseline,
        "data_inicio": data_inicio,
        "data_fim": data_fim
    }

def load_param_monit(params_monit_json):
    """
    Carrega os parâmetros de monitoramento (params_monit) e retorna as variáveis extraídas.

    Parâmetros:
    - params_monit_json (str): JSON contendo os parâmetros de monitoramento.

    Retorno:
    - dict com as variáveis extraídas.
    """
    params_monit = json.loads(params_monit_json)
    
    aberturas = params_monit.get("aberturas", [])
    variaveis = params_monit.get("variaveis", {})
    scores = params_monit.get("scores", {})
    
    baseline_table = "hive_metastore.default." + params_monit.get("tabela_baselien", "baseline")
    prod_prefix = params_monit.get("prefix_tabela_prod", "prod_")
    prod_tables = "hive_metastore.default." + prod_prefix
    percentis_baseline = "hive_metastore.default.percentis_baseline"
    percentis_prod = "hive_metastore.default.percentis_prod"
    psi_table = "hive_metastore.default.psi"
    ks_table = "hive_metastore.default.ks"
    pct_fraude_table = "hive_metastore.default.pct_fraude"
    
    return {
        "aberturas": aberturas,
        "variaveis": variaveis,
        "scores": scores,
        "baseline_table": baseline_table,
        "prod_tables": prod_tables,
        "percentis_baseline": percentis_baseline,
        "percentis_prod": percentis_prod,
        "psi_table": psi_table,
        "ks_table": ks_table,
        "pct_fraude_table": pct_fraude_table
    }











# Função para gerar uma lista de ano_mes no formato YYYYMM no intervalo especificado
def gerar_lista_ano_mes(inicio, fim):
    anos_meses = []
    ano, mes = divmod(inicio, 100)
    ano_fim, mes_fim = divmod(fim, 100)
    
    while (ano < ano_fim) or (ano == ano_fim and mes <= mes_fim):
        anos_meses.append(f"{ano}{str(mes).zfill(2)}")  # Formata como 'YYYYMM'
        mes += 1
        if mes > 12:
            mes = 1
            ano += 1
    return anos_meses


##################################################################
## Cálculo de Percentis
##################################################################

overwrite_percentis = True  # Flag para sobrescrever percentis se já existirem
spark.sql('drop table if exists hive_metastore.default.percentis_baseline')
dbutils.fs.rm("dbfs:/user/hive/warehouse/percentis_baseline", recurse=True)

if incluir_baseline:
    # Tentar carregar a tabela baseline, ou imprimir erro
    try:
        baseline_df = spark.table(baseline_table)
    except Exception as e:
        print(f"Erro: Tabela {baseline_table} não encontrada. Detalhes: {e}")
    else:
        percentis = get_percentis(baseline_df, 'baseline', scores, aberturas, overwrite_percentis, baseline_df=None)



spark.sql('drop table if exists hive_metastore.default.percentis_prod')
dbutils.fs.rm("dbfs:/user/hive/warehouse/percentis_prod", recurse=True)

for ano_mes in gerar_lista_ano_mes(data_inicio, data_fim):
    prod_table = f"{prod_prefix}{ano_mes}"  # Nome da tabela baseada no ano_mes

    # Tentar carregar a tabela correspondente ao ano_mes
    try:
        prod_df = spark.table(
            prod_table)
    except Exception as e:
        print(f"Erro ao carregar a tabela {prod_table}: {e}")
    else:
        # Chamar a função para atualizar percentis
        percentis = get_percentis(prod_df, ano_mes, scores, aberturas, overwrite_percentis, percentis_baseline)



##################################################################
## Cálculo de PSI
##################################################################
spark.sql('drop table if exists hive_metastore.default.psi')
dbutils.fs.rm("dbfs:/user/hive/warehouse/psi", recurse=True)

percentil_prod_df = spark.table("hive_metastore.default.percentis_prod")
percentis_baseline = spark.table('hive_metastore.default.percentis_baseline')

for ano_mes in gerar_lista_ano_mes(data_inicio, data_fim):
    print(ano_mes)

    percentis_prod = percentil_prod_df.where(f"safra = '{ano_mes}'")
    psi = calcula_psi(ano_mes, percentis_baseline, percentis_prod, aberturas)


##################################################################
## Cálculo de KS
##################################################################
spark.sql('drop table if exists hive_metastore.default.ks')
dbutils.fs.rm("dbfs:/user/hive/warehouse/ks", recurse=True)

for ano_mes in gerar_lista_ano_mes(data_inicio, data_fim):
    print(ano_mes)
    df = spark.table(f'hive_metastore.default.prod_{ano_mes}')
    ks = calcula_ks(df, ano_mes, scores, aberturas, 'flag_1')





##################################################################
## Cálculo de % Fraude nas Faixas
##################################################################
spark.sql('drop table if exists hive_metastore.default.pct_fraude')
dbutils.fs.rm("dbfs:/user/hive/warehouse/pct_fraude", recurse=True)

percentis_baseline = spark.table('hive_metastore.default.percentis_baseline')
percentil_prod_df = spark.table("hive_metastore.default.percentis_prod")
for ano_mes in ['202304']:#gerar_lista_ano_mes(data_inicio, data_fim):
    print(ano_mes)
    percentis_prod = percentil_prod_df.where(f"safra = '{ano_mes}'")

    df = spark.table(f'hive_metastore.default.prod_{ano_mes}')
    pct_fraude = calcula_percentual_fraudes(df, ano_mes, scores, percentis_baseline, percentis_prod, aberturas)