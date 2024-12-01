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


def get_percentis(df, safra, scores, aberturas, overwrite=False, baseline_df=None, output_folder="output_csv"):
    """
    Calcula ou carrega percentis e salva os resultados em arquivos CSV.

    Parâmetros:
    - df: DataFrame com os dados da safra.
    - safra: Safra do dataframe.
    - scores: Dicionário contendo os scores e os percentis para cálculo.
    - aberturas: Lista de colunas de aberturas.
    - overwrite: Flag para sobrescrever percentis existentes.
    - baseline_df: DataFrame da baseline, caso necessário.
    - output_folder: Diretório onde os CSVs serão salvos.
    """
    # Configuração do caminho do arquivo
    percentis_file = os.path.join(output_folder, f"percentis_baseline.csv" if safra == 'baseline' else f"percentis_prod_{safra}.csv")
    os.makedirs(output_folder, exist_ok=True)

    # Verificar se o arquivo já existe e sobrescrever se necessário
    if os.path.exists(percentis_file) and not overwrite:
        print(f"Percentis para a safra {safra} já existem no arquivo {percentis_file}. Nenhuma ação realizada.")
        return

    novos_percentis = []

    # Loop pelos scores para calcular os percentis
    for score, dados_score in scores.items():
        percentis_ppv = dados_score.get("percentis_ppv", [])
        percentis_dist = dados_score.get("percentis_dist", [])
        acumula_maiores = dados_score.get("acumula_maiores", True)

        percentis_ppv = [100 - p if acumula_maiores else p for p in percentis_ppv]
        percentis_totais = sorted(set(percentis_ppv + percentis_dist))

        df_score_valido = df.filter(F.col(f"{score}_status") == "score válido")

        aberturas_combinacoes = [()] + list(itertools.chain.from_iterable(
            itertools.combinations(aberturas, i) for i in range(1, len(aberturas) + 1)
        ))

        for combinacao in aberturas_combinacoes:
            combinacao = list(combinacao)

            df_score_valido_ = df_score_valido
            baseline_df_ = baseline_df

            for abertura in aberturas:
                if abertura not in combinacao:
                    df_score_valido_ = df_score_valido_.withColumn(abertura, F.lit('TODOS'))
                    if baseline_df:
                        baseline_df_ = baseline_df_.withColumn(abertura, F.lit('TODOS'))

            if baseline_df:
                baseline_perc = baseline_df_.filter(
                    reduce(lambda x, y: x & y, [(F.col(col) == 'TODOS') if col not in combinacao else (F.col(col) != 'TODOS') for col in aberturas])
                )
                percentil_prod_baseline = calcula_percentis(df_score_valido_, score, acumula_maiores, percentis_totais, aberturas, safra, baseline_perc)
                percentil_prod = calcula_percentis(df_score_valido_, score, acumula_maiores, percentis_totais, aberturas, safra, None)
                df_percs = percentil_prod.join(percentil_prod_baseline, on=['score', 'safra', 'percentil', *aberturas], how='left')
                novos_percentis.append(df_percs)
            else:
                novos_percentis.append(calcula_percentis(df_score_valido_, score, acumula_maiores, percentis_totais, aberturas, safra, None))

    if novos_percentis:
        union_by_name = partial(DataFrame.unionByName, allowMissingColumns=True)
        novos_percentis_df = reduce(union_by_name, novos_percentis)
        novos_percentis_df.write.mode("overwrite").csv(percentis_file, header=True)
        print(f"Percentis calculados e salvos para a safra {safra} em {percentis_file}.")
