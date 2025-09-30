import pandas as pd
import numpy as np
from langchain_community.llms import Ollama
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain

# -------------------------
# 1️⃣ Preparar resumo agregado com estatísticas robustas e histórico por estação/ano
# -------------------------
def prepare_aggregated_anomaly_summary_v3(df_energy, df_clima, anomalies, time_col):
    summaries = []

    anomalies = anomalies.copy()
    anomalies['timestamp'] = anomalies[time_col]

    grouped = anomalies.groupby(['x', 'y'])
    for (x, y), group in grouped:
        ts_energy = df_energy[[x]].copy()
        ts_clima = df_clima[[y]].copy()

        ts_energy = ts_energy[~ts_energy.index.duplicated(keep='first')]
        ts_clima = ts_clima[~ts_clima.index.duplicated(keep='first')]

        common_index = ts_energy.index.intersection(ts_clima.index)
        ts_energy = ts_energy.loc[common_index]
        ts_clima = ts_clima.loc[common_index]

        # Estatísticas globais
        energy_stats = {
            'mean': ts_energy[x].mean(),
            'std': ts_energy[x].std(),
            'min': ts_energy[x].min(),
            'max': ts_energy[x].max(),
        }
        clima_stats = {
            'mean': ts_clima[y].mean(),
            'std': ts_clima[y].std(),
            'min': ts_clima[y].min(),
            'max': ts_clima[y].max(),
        }

        # Histórico por estação e ano
        df_energy_hist = ts_energy.copy()
        df_clima_hist = ts_clima.copy()
        df_energy_hist['year'] = df_energy_hist.index.year
        df_energy_hist['season'] = (df_energy_hist.index.month % 12 // 3 + 1)
        df_clima_hist['year'] = df_clima_hist.index.year
        df_clima_hist['season'] = (df_clima_hist.index.month % 12 // 3 + 1)
        season_map = {1: 'Verão', 2: 'Outono', 3: 'Inverno', 4: 'Primavera'}

        # Agrupa por estação/ano para comparar com anomalias
        hist_stats = {}
        for (year, season), _ in df_energy_hist.groupby(['year', 'season']):
            key = f"{year}-{season_map[season]}"
            hist_stats[key] = {
                'energy_mean': df_energy_hist.loc[(df_energy_hist.year==year) & (df_energy_hist.season==season), x].mean(),
                'energy_std': df_energy_hist.loc[(df_energy_hist.year==year) & (df_energy_hist.season==season), x].std(),
                'clima_mean': df_clima_hist.loc[(df_clima_hist.year==year) & (df_clima_hist.season==season), y].mean(),
                'clima_std': df_clima_hist.loc[(df_clima_hist.year==year) & (df_clima_hist.season==season), y].std(),
            }

        # Lista de anomalias com Z-scores
        anomaly_list = []
        for _, a in group.iterrows():
            ts_year = a['timestamp'].year
            ts_season = (a['timestamp'].month % 12 // 3 + 1)
            key = f"{ts_year}-{season_map[ts_season]}"
            e_mean = hist_stats[key]['energy_mean']
            e_std = hist_stats[key]['energy_std']
            c_mean = hist_stats[key]['clima_mean']
            c_std = hist_stats[key]['clima_std']
            anomaly_list.append({
                'timestamp': a['timestamp'],
                'value_energy': a['anomaly'],
                'score': a['score'],
                'z_energy': (a['anomaly'] - e_mean)/e_std if e_std else np.nan,
                'z_clima': (ts_clima.loc[a['timestamp'], y] - c_mean)/c_std if c_std else np.nan,
                'year': ts_year,
                'season': season_map[ts_season]
            })

        summaries.append({
            'x': x,
            'y': y,
            'energy_stats': energy_stats,
            'clima_stats': clima_stats,
            'historical_stats': hist_stats,
            'anomalies': anomaly_list
        })

    return pd.DataFrame(summaries)

# -------------------------
# 2️⃣ Flatten para LLM
# -------------------------
def flatten_aggregated_summary_v3(df_summary):
    rows = []
    for _, row in df_summary.iterrows():
        anomalies_text = "\n".join([
            f"{a['timestamp']} ({a['season']} {a['year']}): energy={a['value_energy']}, score={a['score']}, z_energy={a['z_energy']:.2f}, z_clima={a['z_clima']:.2f}"
            for a in row['anomalies']
        ])
        flat_row = {
            'x': row['x'],
            'y': row['y'],
            'energy_mean': row['energy_stats']['mean'],
            'energy_std': row['energy_stats']['std'],
            'energy_min': row['energy_stats']['min'],
            'energy_max': row['energy_stats']['max'],
            'clima_mean': row['clima_stats']['mean'],
            'clima_std': row['clima_stats']['std'],
            'clima_min': row['clima_stats']['min'],
            'clima_max': row['clima_stats']['max'],
            'anomalies_text': anomalies_text
        }
        rows.append(flat_row)
    return pd.DataFrame(rows)

# -------------------------
# 3️⃣ Interpretar agregado com Ollama + LangChain
# -------------------------
def interpret_aggregated_anomaly_with_ollama_v3(df_summary):
    df_flat = flatten_aggregated_summary_v3(df_summary)

    prompt_text = """
Você é um analista de dados especializado em energia e clima. Interprete o resumo agregado:

Variável de Energia: {x}
Variável de Clima: {y}

Estatísticas globais:
- Energia: média={energy_mean}, std={energy_std}, min={energy_min}, max={energy_max}
- Clima: média={clima_mean}, std={clima_std}, min={clima_min}, max={clima_max}

Anomalias detectadas:
{anomalies_text}

Para cada anomalia, discuta:
1. O impacto potencial na produção e consumo de energia.
2. Possíveis causas relacionadas à estação do ano, padrões climáticos ou extremos de temperatura.
3. Comparação com histórico similar (mesma estação e ano).
4. Relação explícita entre energia e clima (ex.: quedas de energia durante temperaturas anormais).
5. Recomendações de monitoramento, operação ou investigação.

Produza uma interpretação detalhada, estruturada e confiável.
    """

    prompt = PromptTemplate(
        template=prompt_text,
        input_variables=list(df_flat.columns)
    )

    llm = Ollama(model="qwen2:1.5b")
    chain = LLMChain(llm=llm, prompt=prompt)

    results = []
    for _, row in df_flat.iterrows():
        interpretation = chain.run(row.to_dict())
        results.append({
            'x': row['x'],
            'y': row['y'],
            'interpretation': interpretation
        })
    return pd.DataFrame(results)
