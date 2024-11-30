import os
import itertools
import pandas as pd
from functools import reduce, partial

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window


def calcula_percentis(df, coluna, acumula_maiores, percentis, aberturas, safra, baseline_df = None):
    """Calcula os percentis para uma coluna sem coletar o DataFrame, mantendo as operações no Spark."""    
    if baseline_df:
        # Realizar o pivot
        df_wide = baseline_df.groupBy(*aberturas).pivot("percentil").agg(F.first("score_percentil"))

        # Renomear as colunas para o formato desejado (percentil_X)
        if aberturas:
          perc_df = df_wide.select(*aberturas, *[F.col(f"`{col}`").alias(f"percentil_{col}") for col in df_wide.columns])
        else:
          perc_df = df_wide.select([F.col(col).alias(f"percentil_{col}") for col in df_wide.columns])

    else:
        # Criar expressões para os percentis desejados
        percentil_exprs = [F.percentile_approx(coluna, p / 100, 100).alias(f"percentil_{p}") for p in percentis]

        # Agregar pelos grupos de aberturas
        perc_df = df.groupBy(*aberturas).agg(*percentil_exprs)

    if aberturas:
        perc_df = df.join(perc_df, on=[*aberturas], how='left')
    else:
        perc_df = df.crossJoin(perc_df)

   # Calcular a contagem cumulativa para cada percentil dentro do grupo
    total_perc = [
        F.count(
            F.when(
                (F.col(coluna) >= F.col(f'percentil_{p}')) if acumula_maiores else (F.col(coluna) <= F.col(f'percentil_{p}')),
                1
            )
        ).alias(f"count_{p}")
        for p in percentis
    ]

    # Agregar a contagem para cada percentil e total de linhas por grupo
    result_df = perc_df.groupBy(*aberturas, *[f"percentil_{p}" for p in percentis]).agg(
        *total_perc,
        F.count(F.col(coluna)).alias("total_count")
    )

    # Adicionar colunas de porcentagem para cada percentil
    for p in percentis:
        result_df = result_df.withColumn(
            f"percent_{p}",
            (F.col(f"count_{p}") / F.col("total_count") * 100).cast("double")
        )

    # Reformatar o DataFrame para o formato longo, com `valor` e `percentil` em linhas separadas
    percentis = result_df.select(
        F.lit(coluna).alias('score'),
        F.lit(safra).alias('safra'),
        *aberturas,
        F.explode(F.array(
            *[F.struct(F.col(f"percentil_{p}").alias("score_percentil"), F.lit(p).alias("percentil"), F.col(f'count_{p}').alias('count'), F.col(f'percent_{p}').alias('percent')) for p in percentis]
        )).alias("percentil_info")
    ).select(
        'score',
        'safra',
        *aberturas,
        F.col("percentil_info.percentil"),
        F.col("percentil_info.score_percentil").alias('score_percentil' if not baseline_df else 'score_baseline'),
        F.col("percentil_info.count").alias('count' if not baseline_df else 'count_baseline'),
        F.col("percentil_info.percent").alias('percent' if not baseline_df else 'percent_baseline')
    )

    return percentis



def filtra_percentis_aberturas(baseline_df, combinacao, aberturas):
    """
    Filtra o baseline_df mantendo apenas as linhas onde as colunas de aberturas
    que não estão em group_by_cols sejam nulas.
    
    Parâmetros:
    - baseline_df: DataFrame contendo os dados da baseline.
    - group_by_cols: Lista de colunas pelas quais agrupar os percentis.
    - aberturas: Lista completa de colunas de aberturas disponíveis.
    
    Retorna:
    - DataFrame filtrado.
    """
    # Construir a condição de filtro: colunas contidas em group_by_cols não podem ser nulas, colunas não incluídas devem ser nulas
    condicoes = [
        F.col(col) == 'TODOS' if col not in combinacao else F.col(col) != 'TODOS'
        for col in aberturas
    ]
    
    # Aplicar o filtro no DataFrame
    if condicoes:
        filtro = reduce(lambda x, y: x & y, condicoes)  # Combinar todas as condições com AND
        baseline_filtrado = baseline_df.filter(filtro)
    else:
        baseline_filtrado = baseline_df  # Sem colunas para filtrar, retornar o original
    
    return baseline_filtrado



def salvar_percentis_novos(novos_percentis, table_dest):
    """Salva os percentis novos no arquivo CSV, criando ou acrescentando."""
    union_by_name = partial(DataFrame.unionByName, allowMissingColumns=True)
    novos_percentis_df = reduce(union_by_name, novos_percentis)
    
    novos_percentis_df.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable(table_dest)



def atualiza_percentis(df, scores, safra, aberturas=[], baseline_df=None):
    """
    Calcula os percentis para todos os scores fornecidos no parâmetro `scores` e salva no arquivo de percentis.
    
    Parâmetros:
    - df: DataFrame contendo os dados.
    - scores: Dicionário contendo os scores e os percentis a serem calculados.
    - safra: Data (ano e mês) para o qual calcular os percentis.
    - aberturas: Colunas adicionais para segmentação.
    - baseline_df: dataframe de percentis da baseline
    """    
    percentis_path = "percentis.csv"

    novos_percentis = []

    # Loop através de todos os scores no dicionário
    for score, dados_score in scores.items():
        percentis_ppv = dados_score.get("percentis_ppv", [])
        percentis_dist = dados_score.get("percentis_dist", [])
        acumula_maiores = dados_score.get("acumula_maiores ", True)

        # Ajustar o percentil com base no parâmetro
        percentis_ppv = [100 - p if acumula_maiores else p for p in percentis_ppv]

        # Combina os percentis ppv e dist para o cálculo
        percentis_totais = sorted(set(percentis_ppv + percentis_dist))  # Remove duplicados

        # Filtra os dados para considerar apenas as linhas com o status de "score válido"
        df_score_valido = df.filter(F.col(f"{score}_status") == "score válido")

        # Define combinações de aberturas (inclui a combinação vazia)
        aberturas_combinacoes = [()] + list(itertools.chain.from_iterable(
            itertools.combinations(aberturas, i) for i in range(1, len(aberturas) + 1)
        ))

        # Calcular percentis para cada combinação de aberturas
        for combinacao in aberturas_combinacoes:
            combinacao = list(combinacao)

            df_score_valido_ = df_score_valido
            baseline_df_ = baseline_df if baseline_df else None

            for abertura in aberturas:
                if abertura not in combinacao:
                    df_score_valido_ = df_score_valido_.withColumn(abertura, F.lit('TODOS'))
                    if baseline_df:
                        baseline_df_ = baseline_df_.withColumn(abertura, F.lit('TODOS'))

            if baseline_df:
                baseline_perc = filtra_percentis_aberturas(baseline_df_, combinacao, aberturas)
                percentil_prod_baseline = calcula_percentis(df_score_valido_, score, acumula_maiores, percentis_totais, aberturas, safra, baseline_perc)
                percentil_prod = calcula_percentis(df_score_valido_, score, acumula_maiores, percentis_totais, aberturas, safra, None)

                df_percs = percentil_prod.join(percentil_prod_baseline, on=['score', 'safra', 'percentil', *aberturas], how='left')
                novos_percentis.append(df_percs)        
            else:
                novos_percentis.append(calcula_percentis(df_score_valido_, score, acumula_maiores, percentis_totais, aberturas, safra, None))

    # Salvar percentis novos no arquivo
    if novos_percentis:
        salvar_percentis_novos(novos_percentis, "hive_metastore.default.percentis_baseline" if safra == 'baseline' else "hive_metastore.default.percentis_prod")

    # Carregar percentis atualizados e retorna
    return spark.table("hive_metastore.default.percentis_baseline" if safra == 'baseline' else "hive_metastore.default.percentis_prod")



def get_percentis(df, safra, scores, aberturas, overwrite, baseline_df = None ):
  percentis_table = "hive_metastore.default.percentis_baseline" if safra == 'baseline' else "hive_metastore.default.percentis_prod"

  if safra != 'baseline' and (not baseline_df):
    print('Necessário passar tabela de percentil da baseline')
  
  try:
  # Verificar se a tabela de percentis existe e se contém a safra 
    percentis_existentes = (
        spark.table(percentis_table)
        .filter(f"safra = '{safra}'")
        if spark.catalog.tableExists(percentis_table) else None
    )
  except Exception as e:
      print(f"Erro ao verificar ou processar a tabela de percentis: {e}")
      print("percentis existentes:", percentis_existentes)

  # Condições para recalcular percentis
  if percentis_existentes and percentis_existentes.count() > 0:
      if overwrite:
          # Deletar registros existentes e calcular novos percentis
          spark.sql(f"DELETE FROM {percentis_table} WHERE safra = '{safra}'")
          percentis = atualiza_percentis(df, scores, safra, aberturas, baseline_df)
          print(f"Percentis para {safra} recalculados e salvos com sucesso.")
      else:
          print(f"Percentis para a safra {safra} já calculados. Nenhuma ação realizada.")
  else:
      # Calcular e salvar novos percentis, caso a safra "baseline" não exista
      percentis = atualiza_percentis(df, scores, safra, aberturas, baseline_df)
      print(f"Percentis para {safra} calculados e salvos com sucesso.")