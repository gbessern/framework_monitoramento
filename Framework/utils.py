import json

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
    
    
    data_inicio = params_exec["periodo_monitoramento"]["data_inicio"]
    data_fim = params_exec["periodo_monitoramento"]["data_fim"]
    maturacao = params_exec["periodo_monitoramento"]["maturação"]
    periodo_maturacao = params_exec["periodo_monitoramento"]["periodo_maturacao"]
    overwrite_percentis = params_exec.get("overwrite_percentis", False)
    incluir_baseline = params_exec.get("incluir_baseline", False)

    return {
        "data_inicio": data_inicio,
        "data_fim": data_fim,
        "maturacao": maturacao,
        "periodo_maturacao": periodo_maturacao,
        "incluir_baseline": incluir_baseline,
        "overwrite_percentis": overwrite_percentis
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
    data_prefix = params_monit.get("data_prefix", "tbl")
    data_folder = params_monit.get("data_folder", "./")
    result_folder = params_monit.get("result_folder", "./results")

    percentis_baseline = "hive_metastore.default.percentis_baseline"
    percentis_prod = "hive_metastore.default.percentis_prod"
    psi_table = "hive_metastore.default.psi"
    ks_table = "hive_metastore.default.ks"
    pct_fraude_table = "hive_metastore.default.pct_fraude"
    
    return {
        "aberturas": aberturas,
        "variaveis": variaveis,
        "scores": scores,
        "data_folder": data_folder,
        "data_prefix": data_prefix,
        "percentis_baseline": percentis_baseline,
        "percentis_prod": percentis_prod,
        "psi_table": psi_table,
        "ks_table": ks_table,
        "pct_fraude_table": pct_fraude_table,
        "result_folder": result_folder
    }



def gerar_lista_ano_mes(inicio, fim):
    """
    Gera uma lista de tuplas (ano, mês) dentro do intervalo de datas fornecido.

    Args:
        data_inicio (str): Data de início no formato 'YYYY-MM-DD'.
        data_fim (str): Data de fim no formato 'YYYY-MM-DD'.

    Returns:
        list: Lista de tuplas (ano, mês).
    """
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