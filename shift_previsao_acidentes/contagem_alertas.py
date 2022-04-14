import pandas as pd
import numpy as np
from datetime import timedelta, datetime as dt
# from datetime import datetime as dt
import warnings
from IPython.display import clear_output, display

# Ignore alerts
warnings.filterwarnings("ignore")

# Folder with files
folder = 'shift_previsao_acidentes/'

# Read recipe inputs - Telemetria
date_col_alertas = 'data_inicio'
gr_Alertas_Telemetria_prepared2_sem_manutencao_dummy = pd.read_csv(
    folder + "GR_Alertas_Telemetria_prepared2_sem_manutencao_dummy.csv", sep=";")
gr_Alertas_Telemetria_prepared2_sem_manutencao_dummy_df = pd.DataFrame(
    gr_Alertas_Telemetria_prepared2_sem_manutencao_dummy)
gr_Alertas_Telemetria_prepared2_sem_manutencao_dummy_df[
    'MATRÍCULA'] = gr_Alertas_Telemetria_prepared2_sem_manutencao_dummy_df['MATRÍCULA'].astype(str)
gr_Alertas_Telemetria_prepared2_sem_manutencao_dummy_df[
    date_col_alertas] = gr_Alertas_Telemetria_prepared2_sem_manutencao_dummy_df[date_col_alertas].astype(int)
gr_Alertas_Telemetria_prepared2_sem_manutencao_dummy_df.fillna(
    '', inplace=True)

# Read recipe inputs - Acidentes
date_col_acidentes = 'data_sinistro'
gr_Sinistros_Acidentes_arrumado = pd.read_csv(
    folder + "GR_Sinistros_Acidentes_arrumado.csv", sep=";")
gr_Sinistros_Acidentes_arrumado_df = pd.DataFrame(
    gr_Sinistros_Acidentes_arrumado)
gr_Sinistros_Acidentes_arrumado_df['MATRÍCULA'] = gr_Sinistros_Acidentes_arrumado_df['MATRÍCULA'].astype(
    str)
gr_Sinistros_Acidentes_arrumado_df[date_col_acidentes] = gr_Sinistros_Acidentes_arrumado_df[date_col_acidentes].astype(
    int)
gr_Sinistros_Acidentes_arrumado_df.fillna('', inplace=True)

# Functions


def get_drivers_windowed_events(df_info, end_date, driver_id_col, date_col_info, row_filter, window):
    init_date = int(dt.strftime(dt.strptime(
        str(end_date), '%Y%m%d') - timedelta(days=window), '%Y%m%d'))
    query_str = f"{init_date} <= {date_col_info} <= {end_date} and {driver_id_col} == '{row_filter}'"
    df = df_info.query(query_str)
    df.replace('', 0, inplace=True)
    return df


# Shorter variables
df_acidentes = gr_Sinistros_Acidentes_arrumado_df
df_alertas = gr_Alertas_Telemetria_prepared2_sem_manutencao_dummy_df

# Parameters
window = 360  # days

# Counting definitions
alertas_cols = list(df_alertas.columns)
prefixes_to_count = ['classe_', 'velocidade_']
cols_to_add_in_acidentes = [c for c in alertas_cols if any(
    c.startswith(p) for p in prefixes_to_count)]

df_count = pd.DataFrame('', index=df_acidentes.index, columns=[
                        'SIN'] + cols_to_add_in_acidentes)
df_count['SIN'] = df_acidentes['SIN']

index_list = df_acidentes.index
drivers_reg = 'MATRÍCULA'

# Counting alerts for each event
for index in index_list:
    end_date = df_acidentes[date_col_acidentes][index]
    driver_id = df_acidentes[drivers_reg][index]

    df_window = get_drivers_windowed_events(
        df_alertas, end_date, drivers_reg, date_col_alertas, driver_id, window)

    if df_window.empty:
        continue
    else:
        df_count_new = df_window[cols_to_add_in_acidentes].sum().astype(int)
        for col in cols_to_add_in_acidentes:
            df_count[col][index] = df_count_new[col]
