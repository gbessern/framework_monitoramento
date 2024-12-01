import pandas as pd

# Função para classificar o status dos scores
def classify_score_status(row):
    # Score Status para score_ext_1
    if row['score_ext_1'] == -1:
        row['score_ext_1_status'] = 'score especial - óbito'
    else:
        row['score_ext_1_status'] = 'score válido'
    
    # Score Status para score_ext_2
    if row['score_ext_2'] == -2:
        row['score_ext_2_status'] = 'score especial - no hit'
    elif row['score_ext_2'] == -1:
        row['score_ext_2_status'] = 'score especial - sem cadastro na base'
    else:
        row['score_ext_2_status'] = 'score válido'
    
    if row['score_final'] >= 0 and row['score_final'] <= 1000:
        row['score_final_status'] = 'score válido'
    else:
        row['score_final_status'] = 'score inválido'

    return row

