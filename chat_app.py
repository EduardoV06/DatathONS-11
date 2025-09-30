import streamlit as st
import duckdb
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
import plotly.graph_objects as go

from interpreter_util import prepare_aggregated_anomaly_summary_v3, interpret_aggregated_anomaly_with_ollama_v3
from util import *

# ---------------- Configurações ----------------
ONS_DB_PATH = "ons_simple1.db"
CLIMATE_DB_PATH = "climate_simple1.db"
TOP_K = 5

st.title("Dashboard Energia x Clima — Correlação e Anomalias")

# ---------------- Carregamento dos dados ----------------
energy_table_options = [
    'balanco_energia_subsistema_2024',
    'dados_hidrologicos_res_2024',
    'curva_carga_2024',
    'ear_diario_reservatorios_2024'
]

selected_energy_table = st.selectbox("Selecione a tabela de energia ONS:", energy_table_options)

df_energy = load_table_duckdb(ONS_DB_PATH, selected_energy_table)
df_climate = load_table_duckdb(CLIMATE_DB_PATH, "clima")
usinas_meta_raw = load_table_duckdb(ONS_DB_PATH, "usinameta")
estacoes_meta_raw = load_table_duckdb(CLIMATE_DB_PATH, "metadados_estacoes")

usinas_meta = normalize_usina_meta(usinas_meta_raw)
estacoes_meta = normalize_estacoes_meta(estacoes_meta_raw)

# Calcular latitude e longitude central do Brasil
valid_usinas = usinas_meta.dropna(subset=['latitude', 'longitude'])
central_lat = valid_usinas['latitude'].mean() if not valid_usinas.empty else -14.235
central_lon = valid_usinas['longitude'].mean() if not valid_usinas.empty else -51.9253

# Ajustar coordenadas faltantes
usinas_meta['latitude'] = usinas_meta['latitude'].fillna(central_lat)
usinas_meta['longitude'] = usinas_meta['longitude'].fillna(central_lon)

# ---------------- Corrigir coordenadas invertidas ----------------
def is_out_of_brazil(lat, lon):
    return pd.isna(lat) or pd.isna(lon) or lat < -35 or lat > 5 or lon < -75 or lon > -34

for idx, usina in usinas_meta.iterrows():
    lat, lon = usina['latitude'], usina['longitude']
    if is_out_of_brazil(lat, lon):
        usinas_meta.at[idx, 'latitude'], usinas_meta.at[idx, 'longitude'] = lon, lat

dist_matrix = compute_distance_matrix(usinas_meta, estacoes_meta)

# ---------------- Calcular correlações ----------------
pearson_matrix, energy_cols, climate_cols, df_merged = compute_scores(df_energy, df_climate)

pairs = []
for i, e_col in enumerate(energy_cols):
    for j, c_col in enumerate(climate_cols):
        pairs.append({'energy_var': e_col, 'climate_var': c_col, 'pearson_r': pearson_matrix[i,j]})
df_pairs = pd.DataFrame(pairs)
df_pairs_top = df_pairs.sort_values('pearson_r', ascending=False).head(TOP_K)

st.subheader("Top correlações Energia x Clima")
st.dataframe(df_pairs_top)

# ---------------- Detectar anomalias com limiar adaptativo ----------------
usinas_meta['id_da_usina'] = usinas_meta['id_da_usina'].str.strip()
usinas_anomaly = {usina_id: False for usina_id in usinas_meta['id_da_usina']}

# Criar mapeamento de subsistema para usinas baseado em df_energy
if 'id_subsistema' in df_energy.columns and 'nome_usina' in df_energy.columns:
    subsistema_to_usinas = df_energy.groupby('id_subsistema')['nome_usina'].unique().to_dict()
else:
    subsistema_to_usinas = {}

# Mapear usina para subsistema para fácil acesso inverso
usina_to_subsistema = {}
for subsis, usinas_list in subsistema_to_usinas.items():
    for u in usinas_list:
        usina_to_subsistema[u] = subsis

for idx, row in df_pairs_top.iterrows():
    energy_series = df_merged[row['energy_var']]
    climate_series = df_merged[row['climate_var']]
    derived_feature = energy_series - climate_series

    combined_df = pd.concat([energy_series, derived_feature], axis=1).dropna()
    if combined_df.shape[0] < 10:
        continue

    energy_norm = normalize_series(energy_series)
    derived_norm = normalize_series(derived_feature)
    combined_norm_df = pd.concat([energy_norm, derived_norm], axis=1).dropna()

    clf = IsolationForest(contamination=0.01, random_state=42)
    clf.fit(combined_norm_df)
    anomaly_scores = -clf.score_samples(combined_norm_df)  
    threshold = np.percentile(anomaly_scores, 95) 
    strong_anomalies = anomaly_scores >= threshold

    if strong_anomalies.sum() >= 3:
        subsistema_key = row['energy_var']
        # Tentativa de obter subsistema a partir do energy_var (nome da coluna)
        # Como não sabemos a estrutura exata, tentamos encontrar subsistema que contém energy_var
        matched_subsistema = None
        for subsis in subsistema_to_usinas.keys():
            if subsis == subsistema_key or subsis in subsistema_key or subsistema_key in subsis:
                matched_subsistema = subsis
                break
        if matched_subsistema is None:
            matched_subsistema = subsistema_key  # fallback

        # Marcar todas as usinas do subsistema como True
        for u_id in subsistema_to_usinas.get(matched_subsistema, []):
            if u_id in usinas_anomaly:
                usinas_anomaly[u_id] = True

# ---------------- Mapa ----------------
st.subheader("Mapa de Usinas com Anomalias")

latitudes, longitudes, colors, symbols, sizes, texts = [], [], [], [], [], []

for idx, usina in usinas_meta.iterrows():
    usina_id = usina['id_da_usina']
    has_anomaly = usinas_anomaly.get(usina_id, False)

    latitudes.append(usina['latitude'])
    longitudes.append(usina['longitude'])
    colors.append("red" if has_anomaly else "blue")
    symbols.append("diamond" if has_anomaly else "circle")
    sizes.append(12 if has_anomaly else 8)
    hover_text = f"Usina: {usina_id}<br>Anomalia: {'Sim' if has_anomaly else 'Não'}"
    texts.append(hover_text)

fig_map = go.Figure(go.Scattergeo(
    lat=latitudes,
    lon=longitudes,
    mode='markers',
    marker=dict(symbol=symbols, color=colors, size=sizes,
                line=dict(width=2, color='black')),
    text=texts,
    hoverinfo='text'
))

fig_map.update_layout(
    geo=dict(
        scope='south america',
        projection_type='mercator',
        showland=True,
        landcolor="rgb(217, 217, 217)",
        showcountries=True,
        lataxis=dict(range=[-35,-5]),
        lonaxis=dict(range=[-75,-34])
    ),
    title="Usinas com Anomalias",
    height=700,
    margin=dict(l=0,r=0,t=40,b=0)
)

st.plotly_chart(fig_map, use_container_width=True, key="map_usinas_anomalias")

# ---------------- Séries temporais ----------------
st.subheader("Séries temporais roláveis")

time_col_mapping = {
    'balanco_energia_subsistema_2024': 'din_instante',
    'dados_hidrologicos_res_2024': 'din_instante',
    'curva_carga_2024': 'din_instante',
    'ear_diario_reservatorios_2024': 'ear_data'
}
energy_time_col = time_col_mapping.get(selected_energy_table, 'din_instante')

tabs = st.tabs([f"{row['energy_var']} x {row['climate_var']}" for _, row in df_pairs_top.iterrows()])

for idx, (tab, row) in enumerate(zip(tabs, df_pairs_top.itertuples())):
    with tab:
        energy_series = df_merged[row.energy_var]
        climate_series = df_merged[row.climate_var]
        derived_feature = energy_series - climate_series

        combined_df = pd.concat([energy_series, derived_feature], axis=1).dropna()
        if combined_df.shape[0] < 10:
            st.write("Dados insuficientes para análise.")
            continue

        energy_norm = normalize_series(energy_series)
        climate_norm = normalize_series(climate_series)
        derived_norm = normalize_series(derived_feature)

        clf = IsolationForest(contamination=0.01, random_state=42)
        anomalies = clf.fit_predict(pd.concat([energy_norm, derived_norm], axis=1)) == -1

        fig = go.Figure()
        fig.add_trace(go.Scatter(y=energy_norm, mode='lines', name=row.energy_var))
        fig.add_trace(go.Scatter(y=climate_norm, mode='lines', name=row.climate_var))
        fig.add_trace(go.Scatter(y=derived_norm, mode='lines', name="Feature derivada"))
        fig.add_trace(go.Scatter(y=derived_norm.where(anomalies), mode='markers',
                                 name="Anomalia",
                                 marker=dict(color='red', size=8, symbol='diamond')))
        fig.update_layout(height=400, margin=dict(l=0,r=0,t=20,b=0))
        st.plotly_chart(fig, use_container_width=True)

        interpret_button_key = f"interpret_button_{idx}"
        if st.button("Interpretar Anomalias", key=interpret_button_key):
            df_energy_pair = energy_series.to_frame(name=row.energy_var)
            df_climate_pair = climate_series.to_frame(name=row.climate_var)
            if energy_time_col in df_energy.columns:
                time_series_full = pd.to_datetime(df_energy[energy_time_col])
            else:
                time_series_full = pd.to_datetime(df_energy.index)
            # Alinhar timestamps ao índice das séries de energia e clima
            # Tentar usar o índice de energy_series convertido para datetime
            try:
                timestamps = pd.to_datetime(energy_series.index)
            except Exception:
                timestamps = energy_series.index
            # Garantir que timestamps tenha o mesmo tamanho que energy_series
            if len(timestamps) != len(energy_series):
                timestamps = pd.date_range(start=timestamps[0], periods=len(energy_series), freq='D')

            # Construir DataFrame de anomalias compatível com a função
            anomalies_df_for_pair = pd.DataFrame({
                'x': [row.energy_var] * len(energy_series),
                'y': [row.climate_var] * len(climate_series),
                'anomaly': derived_feature.values,
                'score': clf.decision_function(pd.concat([energy_norm, derived_norm], axis=1))
            }, index=energy_series.index)

            anomalies_df_for_pair['timestamp_col'] = timestamps

            # Preparar o resumo agregado para o par atual usando o argumento time_col='timestamp_col'
            summary = prepare_aggregated_anomaly_summary_v3(
                df_energy_pair,
                df_climate_pair,
                anomalies_df_for_pair,
                time_col='timestamp_col'
            )
            # Obter interpretação da LLM
            interpretation_df = interpret_aggregated_anomaly_with_ollama_v3(summary)
            # Extrair apenas o texto da coluna 'interpretation'
            interpretation_text = ""
            if isinstance(interpretation_df, pd.DataFrame) and 'interpretation' in interpretation_df.columns:
                interpretation_text = interpretation_df['interpretation'].iloc[0]
            else:
                interpretation_text = str(interpretation_df)

            output_area = st.empty()
            displayed_text = ""
            for char in interpretation_text:
                displayed_text += char
                output_area.markdown(f"**Interpretação da LLM:**  \n{displayed_text}")
