import pandas as pd
import numpy as np


np.random.seed(42)

rows_per_month = 3000

# Função para converter renda em valores numéricos
def renda_to_numeric(renda):
    mapping = {
        "<1k": 0.5,
        "1k~3k": 2,
        "3k~5k": 4,
        "5k~10k": 7.5,
        "10k~20k": 15,
        "20k~30k": 25,
        "30k+": 35
    }
    return mapping.get(renda, np.nan)

# Função para gerar dados simulados
def generate_monthly_data(month):
    n_rows = rows_per_month
    # Variáveis
    idade = np.random.randint(15, 106, size=n_rows)  # Idade entre 15 e 105, sem valores nulos
    renda = np.random.choice(
        ["<1k", "1k~3k", "3k~5k", "5k~10k", "10k~20k", "20k~30k", "30k+", None],
        size=n_rows,
        p=[0.1, 0.2, 0.2, 0.2, 0.1, 0.05, 0.05, 0.1]  # 20% de nulos
    )
    score_ext_1 = np.random.choice(
        list(range(-1, 1001)),
        size=n_rows,
        p=[0.01] + [0.99 / 1001] * 1001  # 1% de valores inválidos (-1)
    )
    score_ext_2 = np.random.choice(
        list(range(-2, 1001)),
        size=n_rows,
        p=[0.01, 0.01] + [0.98 / 1001] * 1001  # 2% de valores inválidos (-2, -1)
    )

    # Conversão de renda para valores numéricos
    renda_num = [renda_to_numeric(r) for r in renda]

    relacionamento = np.random.choice(
        [0, 1], 
        size=n_rows, 
        p=[0.7, 0.3] # 30% clientes antigos
    )
    
    # Coluna adicional de portfólio e segmento
    group_ab = np.random.choice(["A", "B"], size=n_rows, p=[0.5, 0.5])
    group_xy = np.random.choice(["X", "Y"], size=n_rows, p=[0.3, 0.7])
        
    # Gerar score_final com base em uma regressão
    score_final = (
        0.7 * np.clip(score_ext_1, 0, None) -       # Score externo 1 (positivo para fraude)
        0.4 * np.clip(score_ext_2, 0, None) +       # Score externo 2 (negativo para fraude)
        -0.2 * np.nan_to_num(renda_num, nan=0) +    # Faixa de renda (quanto maior, menor o risco)
        0.05 * idade +                              # Idade (ligeira influência positiva)
        100 * relacionamento +
        np.random.normal(0, 30, n_rows)            # Ruído
    )
    score_final = np.clip(score_final, 0, 1000).astype(int)  # Limita entre 0 e 1000

    # Gerar flag_1 (fraude ou não) com base em uma probabilidade ajustada
    prob_fraude = (score_final / 1000) * 0.1  # Probabilidade proporcional ao score_final (7% target)
    flag_1 = np.random.choice(
        [1, 0],
        size=n_rows,
        p=[0.99, 0.01] if prob_fraude.mean() < 0.08 else [0.05, 0.95]
    )

    # Montando o DataFrame
    return pd.DataFrame({
        "ano_mes": month,
        "group_ab": group_ab,
        "group_xy": group_xy,
        "idade": idade,
        "renda": renda,
        "relacionamento": relacionamento,
        "score_ext_1": score_ext_1,
        "score_ext_2": score_ext_2,
        "score_final": score_final,
        "flag_1": flag_1
    })


