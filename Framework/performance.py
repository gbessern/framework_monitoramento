import os
import itertools
import pandas as pd
from functools import reduce, partial

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

def calcula_psi(safra, baseline_df, prod_df, aberturas, output_folder="output_csv"):
    """
    Calcula o PSI entre baseline_df e prod_df e salva em arquivos CSV.

    Parâmetros:
    - safra: Identificador da safra.
    - baseline_df: DataFrame contendo os percentis da baseline.
    - prod_df: DataFrame contendo os percentis da produção.
    - aberturas: Lista de colunas de aberturas para segmentação.
    - output_folder: Diretório onde os resultados serão salvos.
    """
    os.makedirs(output_folder, exist_ok=True)

    # Realizar o cálculo do PSI
    psi_df = (
        prod_df
        .select('score', 'safra', 'percentil', 'percent_baseline', *aberturas)
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

    # Salvar resultados em CSV
    psi_output_path = os.path.join(output_folder, f"psi_{safra}.csv")
    psi_value.write.mode("overwrite").csv(psi_output_path, header=True)
    print(f"PSI calculado para safra {safra} e salvo em: {psi_output_path}")

    return psi_output_path


def calcula_ks(df, safra, scores, aberturas, output_folder="output_csv", resp_column='flag_1'):
    """
    Calcula o KS de uma safra para n scores e salva os resultados em arquivos CSV.

    Parâmetros:
    - df: DataFrame com os dados da safra.
    - safra: Safra do dataframe.
    - scores: Dicionário contendo os scores.
    - aberturas: Lista de colunas de aberturas.
    - output_folder: Diretório onde os resultados serão salvos.
    - resp_column: Nome da coluna que indica a resposta (e.g., flag_1).
    """
    os.makedirs(output_folder, exist_ok=True)
    results = []

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
            window_spec = Window.partitionBy(*aberturas).orderBy(F.col(score).desc())

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
            ks = df_with_ks.groupBy(
                F.lit(safra).alias('safra'), F.lit(score).alias('score'), *aberturas
            ).agg(F.max("ks_statistic").alias("ks_value"))

            results.append(ks)


    if results:
        union_by_name = partial(DataFrame.unionByName, allowMissingColumns=True)
        ks_results = reduce(union_by_name, results)
        output_file = os.path.join(output_folder, f"ks_{safra}.csv")
        ks_results.write.mode("overwrite").csv(output_file, header=True)
        print(f"KS calculado para safra {safra} e salvo em: {output_file}")

    else:
        print(f"Nenhum resultado de KS gerado para a safra {safra}.")

    return output_folder

def calcula_percentual_fraudes(df, safra, scores, pctl_baseline, pctl_baseline_prod, aberturas, output_folder="pct_fraude_results"):
    """
    Calcula o % do número total de fraudes dentro de cada percentil e o % de fraudes dentro da faixa utilizando baseline e produção.
    Salva os resultados em arquivos CSV.

    Parâmetros:
    - df: DataFrame com os dados da safra.
    - safra: Safra do dataframe.
    - scores: Dicionário contendo os scores e os percentis para cálculo.
    - pctl_baseline: DataFrame contendo os percentis da baseline.
    - pctl_baseline_prod: DataFrame contendo os percentis da baseline para produção.
    - aberturas: Lista de colunas de aberturas (e.g., 'group_ab', 'group_xy').
    - output_folder: Diretório onde os resultados serão salvos.
    """
    resultados = []

    for score, config in scores.items():
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
                "percentual_fraudes_total_baseline",
                (F.col("total_fraudes_baseline") / F.col("total_fraudes_safra") * 100).cast("double")
            ).withColumn(
                "percentual_fraudes_total_producao",
                (F.col("total_fraudes_producao") / F.col("total_fraudes_safra") * 100).cast("double")
            ).withColumn("safra", F.lit(safra)).withColumn("score", F.lit(score))

            # Salvar nos resultados
            resultados.append(fraudes_consolidadas)

    # Consolidar todos os DataFrames em um único DataFrame
    if resultados:
        union_by_name = partial(DataFrame.unionByName, allowMissingColumns=True)
        resultados_consolidados = reduce(union_by_name, resultados)
        output_file = os.path.join(output_folder, f"pct_fraude_{safra}.csv")
        resultados_consolidados.write.mode("overwrite").csv(output_file, header=True)
        print(f"Percentuais de fraudes calculados e salvos em: {output_file}")

    else:
        print(f"Nenhum resultado gerado para a safra {safra}.")
