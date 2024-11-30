import os
import itertools
import pandas as pd
from functools import reduce, partial

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

def calcula_psi(safra, baseline_df, prod_df, aberturas):
    """
    Calcula o PSI entre baseline_df e prod_df.
    """
    psi_df = (
        prod_df
        .select('score', 'safra',	'percentil',	'percent_baseline', *aberturas)
        .join(
            baseline_df.select("score", "percentil", 'percent', *aberturas),
            on=["score", "percentil", *aberturas],
            how="inner"
        )
        .withColumnRenamed("percent", "Q_baseline")
        .withColumnRenamed("percent_baseline", "P_prod")
    )

    epsilon = 1e-10
    psi_df = psi_df.withColumn("P_prod", F.col("P_prod") + epsilon)
    psi_df = psi_df.withColumn("Q_baseline", F.col("Q_baseline") + epsilon)

    psi_df = psi_df.withColumn(
        "psi_component",
        (F.col("P_prod") - F.col("Q_baseline")) * F.log(F.col("P_prod") / F.col("Q_baseline"))
    )

    psi_value = psi_df.groupBy('score', 'safra', *aberturas).agg(F.sum("psi_component").alias("psi_value"))
   
    psi_value.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable(psi_table)

    return spark.table(psi_table)


def calcula_ks(df, safra, scores, aberturas, resp_column='flag_1'):
    """
    Calcula o KS de uma safra para n scores.
    """
    ks_list = []

    for score, dados_score in scores.items():
        # Filtra os dados para considerar apenas as linhas com o status de "score válido"
        df_score_valido = df.filter(F.col(f"{score}_status") == "score válido")

        # Define combinações de aberturas (inclui a combinação vazia)
        aberturas_combinacoes = [()] + list(itertools.chain.from_iterable(
            itertools.combinations(aberturas, i) for i in range(1, len(aberturas) + 1)
        ))

        # Calcular KS para cada combinação de aberturas
        for combinacao in aberturas_combinacoes:
            df_score_valido_ = df_score_valido
            group_by_cols = list(combinacao)

            # Adicionar "TODOS" para as aberturas ausentes
            for abertura in aberturas:
                if abertura not in group_by_cols:
                    df_score_valido_ = df_score_valido_.withColumn(abertura, F.lit('TODOS'))

            # Definir a especificação de janela para ordenação
            window_spec = Window.partitionBy(aberturas).orderBy(F.col(score).desc())

            # Calcular totais de positivos e negativos
            total_positives = df_score_valido_.filter(F.col(resp_column) == 1).count()
            total_negatives = df_score_valido_.filter(F.col(resp_column) == 0).count()

            # Calcular somatórios cumulativos de positivos e negativos
            df_with_cumsum = df_score_valido_.withColumn(
                "cumulative_positives",
                F.sum(F.when(F.col(resp_column) == 1, 1).otherwise(0)).over(window_spec)
            ).withColumn(
                "cumulative_negatives",
                F.sum(F.when(F.col(resp_column) == 0, 1).otherwise(0)).over(window_spec)
            )

            # Calcular as taxas cumulativas
            df_with_rates = df_with_cumsum.withColumn(
                "cumulative_pos_rate",
                F.col("cumulative_positives") / total_positives
            ).withColumn(
                "cumulative_neg_rate",
                F.col("cumulative_negatives") / total_negatives
            )

            # Calcular o KS estatístico
            df_with_ks = df_with_rates.withColumn(
                "ks_statistic",
                F.abs(F.col("cumulative_pos_rate") - F.col("cumulative_neg_rate"))
            )

            # Encontrar o valor máximo de KS para a combinação atual
            ks = df_with_ks.groupBy(F.col('ano_mes').alias('safra'), F.lit(score).alias('score'), *aberturas).agg(F.max("ks_statistic").alias("ks_value"))

            # Salvar os resultados na tabela Hive
            ks.write.format("delta").mode("append").saveAsTable(ks_table)

    return spark.table(ks_table)


def calcula_percentual_fraudes(df, safra, scores, pctl_baseline, pctl_baseline_prod, aberturas):
    """
    Calcula o % do número total de fraudes dentro de cada percentil e o % de fraudes dentro da faixa utilizando baseline e produção.

    Parâmetros:
    - df: DataFrame com os dados da safra.
    - safra: Safra do dataframe.
    - scores: Dicionário contendo os scores e os percentis para cálculo.
    - pctl_baseline: DataFrame contendo os percentis da baseline.
    - pctl_baseline_prod: DataFrame contendo os percentis da baseline para produção.
    - aberturas: Lista de colunas de aberturas (e.g., 'group_ab', 'group_xy').

    Retorno:
    - DataFrame consolidado com os percentuais de fraudes por score, percentil e combinação de aberturas.
    """
    resultados = []

    for score, config in scores.items():
        print(f'{score}')
        # Filtra o DataFrame apenas com "score válido"
        acumula_maiores = config.get("acumula_maiores", True)
        percentis_ppv = config.get("percentis_ppv", [])

        percentis_ppv = [100 - p if acumula_maiores else p for p in percentis_ppv]
        df_score_valido = df.filter(F.col(f"{score}_status") == "score válido")
        
        # Gera todas as combinações possíveis de aberturas (inclui 'TODOS' quando necessário)
        aberturas_combinacoes = [()] + list(itertools.chain.from_iterable(
            itertools.combinations(aberturas, i) for i in range(1, len(aberturas) + 1)
        ))

        for combinacao in aberturas_combinacoes:
            print(f'\t{combinacao}')
            group_by_cols = list(combinacao)
            df_score_valido_ = df_score_valido

            # Adicionar "TODOS" para colunas ausentes
            for abertura in aberturas:
                if abertura not in group_by_cols:
                    df_score_valido_ = df_score_valido_.withColumn(abertura, F.lit("TODOS"))

            # Calcular o total de fraudes e total de casos segmentados pelas aberturas
            totais_safra = df_score_valido_.groupBy(aberturas).agg(
                F.count(F.when(F.col("flag_1") == 1, 1)).alias("total_fraudes_safra")
            )

            percentis_baseline_df = pctl_baseline.filter(
                (F.col("score") == score) & (F.col("percentil").isin(percentis_ppv))
            ).select(
                "percentil", *aberturas, "score_percentil"
            ).withColumnRenamed("score_percentil", "score_baseline_percentil")
            
            # Selecionar os percentis da produção
            percentis_producao_df = pctl_baseline_prod.filter(
                (F.col("score") == score) & (F.col("percentil").isin(percentis_ppv))
            ).select(
                "percentil", *aberturas, "score_percentil"
            ).withColumnRenamed("score_percentil", "score_producao_percentil")
            
            # Juntar os percentis da baseline e produção ao DataFrame principal
            df_com_percentis = df_score_valido_.join(
                percentis_baseline_df, on=aberturas, how="left"
            ).join(
                percentis_producao_df, on=aberturas + ["percentil"], how="left"
            )

            # Filtrar os registros para baseline e produção
            df_baseline = df_com_percentis.filter(
                (F.col(score) >= F.col("score_baseline_percentil")) if acumula_maiores else (F.col(score) <= F.col("score_baseline_percentil"))
            )

            df_producao = df_com_percentis.filter(
                (F.col(score) >= F.col("score_producao_percentil")) if acumula_maiores else (F.col(score) <= F.col("score_producao_percentil"))
            )

            # Calcular fraudes por percentil para baseline
            baseline_fraudes = df_baseline.groupBy("percentil", *aberturas).agg(
                F.count(F.when(F.col("flag_1") == 1, 1)).alias("total_fraudes_baseline")
            )

            # Calcular fraudes por percentil para produção
            producao_fraudes = df_producao.groupBy("percentil", *aberturas).agg(
                F.count(F.when(F.col("flag_1") == 1, 1)).alias("total_fraudes_producao")
            )

            # Juntar os resultados de baseline e produção
            fraudes_consolidadas = baseline_fraudes.join(
                producao_fraudes, on=["percentil"] + aberturas, how="outer"
            )

            # Juntar os totais da safra
            fraudes_consolidadas = fraudes_consolidadas.join(
                totais_safra, on=aberturas, how="left"
            )

            # Calcular os percentuais de fraudes
            fraudes_consolidadas = fraudes_consolidadas.withColumn(
                # Percentual de fraudes do total (fraudes na faixa / total de fraudes na safra)
                "percentual_fraudes_total_baseline",
                (F.col("total_fraudes_baseline") / F.col("total_fraudes_safra") * 100).cast("double")
            ).withColumn(
                "percentual_fraudes_total_producao",
                (F.col("total_fraudes_producao") / F.col("total_fraudes_safra") * 100).cast("double")
            ).withColumn("safra", F.lit(safra)).withColumn("score", F.lit(score))

            # Salvar nos resultados
            resultados.append(fraudes_consolidadas)

    # Consolidar todos os DataFrames em um único DataFrame
    resultados_consolidados = resultados[0]
    for df_resultado in resultados[1:]:
        resultados_consolidados = resultados_consolidados.union(df_resultado)

    # Salvar na tabela Hive
    resultados_consolidados.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable(pct_fraude_table)

    # Retornar o DataFrame consolidado
    return spark.table(pct_fraude_table)
