import duckdb
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import numpy as np

def load_table_duckdb(db_path, table_name):
    con = duckdb.connect(database=db_path, read_only=True)
    df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
    con.close()
    return df

def normalize_usina_meta(df):
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()
    df['id_da_usina'] = df['nome']
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    return df[['id_da_usina', 'latitude', 'longitude']]

def normalize_estacoes_meta(df):
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()
    if 'arquivo' not in df.columns:
        raise ValueError("Não foi possível encontrar coluna 'arquivo' para ID da estação")
    df['id_estacao'] = df['arquivo']
    lat_col, lon_col = df.columns[0], df.columns[1]
    df['latitude'] = pd.to_numeric(df[lat_col], errors='coerce')
    df['longitude'] = pd.to_numeric(df[lon_col], errors='coerce')
    return df[['id_estacao', 'latitude', 'longitude']]

def compute_distance_matrix(usinas, estacoes):
    u_coords = usinas[['latitude', 'longitude']].to_numpy()
    e_coords = estacoes[['latitude', 'longitude']].to_numpy()
    dist_matrix = np.sqrt(((u_coords[:, None, :] - e_coords[None, :, :])**2).sum(axis=2)) * 111
    return pd.DataFrame(dist_matrix, index=usinas['id_da_usina'], columns=estacoes['id_estacao'])

def compute_scores(df_energy, df_climate, time_col_energy="din_instante", time_col_climate="data_hora"):
    df_energy = df_energy.copy()
    df_climate = df_climate.copy()
    df_energy[time_col_energy] = pd.to_datetime(df_energy[time_col_energy])
    df_climate[time_col_climate] = pd.to_datetime(df_climate[time_col_climate])
    
    df_energy_daily = df_energy.groupby(df_energy[time_col_energy].dt.date).mean(numeric_only=True)
    df_climate_daily = df_climate.groupby(df_climate[time_col_climate].dt.date).mean(numeric_only=True)
    
    df_energy_daily.index = pd.to_datetime(df_energy_daily.index)
    df_climate_daily.index = pd.to_datetime(df_climate_daily.index)
    
    df_merged = df_energy_daily.join(df_climate_daily, how="inner", lsuffix="_energy", rsuffix="_climate")
    
    energy_cols = [c for c in df_energy_daily.columns if c in df_merged.columns]
    climate_cols = [c for c in df_climate_daily.columns if c in df_merged.columns]
    
    energy_vals = df_merged[energy_cols].to_numpy()
    climate_vals = df_merged[climate_cols].to_numpy()
    
    pearson_matrix = np.corrcoef(energy_vals.T, climate_vals.T)[:len(energy_cols), len(energy_cols):]
    
    return pearson_matrix, energy_cols, climate_cols, df_merged

def normalize_series(series):
    scaler = MinMaxScaler()
    values = series.values.reshape(-1, 1)
    norm = scaler.fit_transform(values).ravel()
    return pd.Series(norm, index=series.index)