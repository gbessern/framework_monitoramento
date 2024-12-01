from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

import os
from utils import load_param_monit, load_param_exec, gerar_lista_ano_mes
from percentis import *
from performance import *

## Carrega Parâmtros
def monit_performance(params_exec, params_monit):
    print(15*'*', 'Carregando Parâmetros', 15*'*')

    with open(params_exec, 'r') as f:
        params_exec_json = f.read()
    params_exec = load_param_exec(params_exec_json)

    with open(params_monit, 'r') as f:
        params_monit_json = f.read()
    params_monit = load_param_monit(params_monit_json)

    
    data_inicio = params_exec['data_inicio']
    data_fim = params_exec['data_fim']
    overwrite_percentis = params_exec['overwrite_percentis']
    incluir_baseline = params_exec['incluir_baseline']

    data_folder = params_monit['data_folder']
    data_prefix = params_monit['data_prefix']
    scores = params_monit['scores']
    aberturas = params_monit['aberturas']
    result_folder = params_monit['result_folder']
  
    ###############################################################
    ## PERCENTIS
    ###############################################################
    print(15*'*', 'Calculando Percentis', 15*'*')

    if incluir_baseline:
        baseline_file_path = os.path.join(data_folder, f"{data_prefix}_baseline.csv")
        try:
            # Carregar o arquivo CSV como DataFrame Spark
            baseline_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(baseline_file_path)
        except Exception as e:
            print(f"Erro: Arquivo {baseline_file_path} não encontrado ou erro ao carregar. Detalhes: {e}")
        else:
            # Calcular os percentis usando a função get_percentis
            percentis_baseline = get_percentis(baseline_df, 'baseline', scores, aberturas, overwrite_percentis, None, result_folder)

    baseline_percentis_path = os.path.join(result_folder, f"percentis_baseline.csv")
    percentis_baseline = df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(baseline_percentis_path)
    for ano_mes in gerar_lista_ano_mes(data_inicio, data_fim):
        prod_file_path = os.path.join(data_folder, f"{data_prefix}_{ano_mes}.csv")

        # Tentar carregar a tabela correspondente ao ano_mes
        try:
            df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(prod_file_path)
        except Exception as e:
            print(f"Erro: Arquivo {prod_file_path} não encontrado ou erro ao carregar. Detalhes: {e}")
        else:
            # Calcular os percentis usando a função get_percentis
            percentis = get_percentis(df, ano_mes, scores, aberturas, overwrite_percentis, percentis_baseline, result_folder)


    ###############################################################
    ## PSI
    ###############################################################
    print(15*'*', 'Calculando PSI', 15*'*')

    percentis_baseline_path = os.path.join(result_folder, f"percentis_baseline.csv")
    percentis_baseline = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(percentis_baseline_path)

    for ano_mes in gerar_lista_ano_mes(data_inicio, data_fim):
        print(f"Processando safra: {ano_mes}")
        percentil_prod_df_path = os.path.join(result_folder, f"percentis_prod_{ano_mes}.csv")
        percentil_prod_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(percentil_prod_df_path)


        percentis_prod = percentil_prod_df.where(f"safra = '{ano_mes}'")
        calcula_psi(ano_mes, percentis_baseline, percentis_prod, aberturas, result_folder)



    ###############################################################
    ## KS
    ###############################################################
    print(15*'*', 'Calculando KS', 15*'*')
    for ano_mes in gerar_lista_ano_mes(data_inicio, data_fim):
        print(f"Processando safra: {ano_mes}")
        # Carregar os dados da produção para a safra específica
        prod_file_path = os.path.join(data_folder, f"{data_prefix}_{ano_mes}.csv")
        if not os.path.exists(prod_file_path):
            print(f"Arquivo de produção para {ano_mes} não encontrado: {prod_file}")
            continue
          
        df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(prod_file_path)

        # Calcular o KS e salvar os resultados
        calcula_ks(df, ano_mes, scores, aberturas, output_folder=result_folder)





    ##################################################################
    ## Cálculo de % Fraude nas Faixas
    ##################################################################
    print(15*'*', 'Cálculo de % Fraude nas Faixas', 15*'*')

    percentis_baseline_path = os.path.join(result_folder, f"percentis_baseline.csv")
    percentis_baseline = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(percentis_baseline_path)
    for ano_mes in gerar_lista_ano_mes(data_inicio, data_fim):
        print(ano_mes)

        percentil_prod_df_path = os.path.join(result_folder, f"percentis_prod_{ano_mes}.csv")
        percentil_prod_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(percentil_prod_df_path)

        prod_file_path = os.path.join(data_folder, f"{data_prefix}_{ano_mes}.csv")
        df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(prod_file_path)

        pct_fraude = calcula_percentual_fraudes(df, ano_mes, scores, percentis_baseline, percentil_prod_df, aberturas, result_folder)



