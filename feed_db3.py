import pandas as pd
from pathlib import Path
import sqlite3

CLIMATE_ROOT = Path(__file__).parent / "DatathONS-11" / "Climate"
DB_PATH = Path("climate_simple1.db")

# Colunas que vamos manter e seus nomes simples
KEEP_COLS = {
    "data": "data_hora",
    "hora_utc": "hora_utc",
    "precipitação total, horário (mm)": "precipitacao",
    "pressao atmosferica ao nivel da estacao, horaria (mB)": "pressao",
    "umidade relativa do ar, horaria (%)": "umidade",
    "vento, velocidade horaria (m/s)": "vento",
    "temperatura máxima na hora ant. (aut) (°c)": "temperatura_max",
    "temperatura mínima na hora ant. (aut) (°c)": "temperatura_min"
}

def simplify_column_name(s):
    s = s.lower().strip()
    s = s.replace(" ", "_").replace("ç", "c").replace("ã","a").replace("í","i").replace("ó","o")
    return s

def process_csv(file_path: Path):
    # ----------------------------
    # Captura dos metadados (8 primeiras linhas)
    # ----------------------------
    try:
        meta_df = pd.read_csv(file_path, sep=";", encoding="latin1", nrows=8, header=None)
        # Normaliza strings e elimina duplicados
        meta_df = meta_df.applymap(lambda x: str(x).strip() if pd.notna(x) else x).drop_duplicates()
        meta_df["arquivo"] = file_path.name
    except Exception:
        print(f"Falha ao ler metadados de {file_path.name}")
        meta_df = pd.DataFrame()

    # ----------------------------
    # Captura dos dados métricos (a partir da linha 9)
    # ----------------------------
    try:
        df = pd.read_csv(file_path, sep=";", encoding="latin1", decimal=",", skiprows=8)
    except Exception:
        print(f"Falha ao ler dados de {file_path.name}")
        return pd.DataFrame(), meta_df

    # Normalizar colunas
    df.columns = [simplify_column_name(c) for c in df.columns]

    # Manter apenas colunas desejadas
    df_simple = pd.DataFrame()
    for k, v in KEEP_COLS.items():
        k_simple = simplify_column_name(k)
        if k_simple in df.columns:
            df_simple[v] = df[k_simple]
        else:
            df_simple[v] = pd.NA

    # Criar data_hora
    if "hora_utc" in df_simple.columns:
        df_simple["hora_utc"] = df_simple["hora_utc"].astype(str).str.replace(" UTC","", regex=False).str.zfill(4)
        df_simple["hora_utc"] = df_simple["hora_utc"].str.replace(r"^(\d{2})(\d{2})$", r"\1:\2", regex=True)
        df_simple["data_hora"] = pd.to_datetime(df_simple["data_hora"].astype(str) + " " + df_simple["hora_utc"], errors="coerce")
    else:
        df_simple["data_hora"] = pd.to_datetime(df_simple["data_hora"], errors="coerce")

    df_simple = df_simple.drop(columns=["hora_utc"], errors="ignore")
    df_simple = df_simple.dropna(subset=["data_hora"])
    df_simple["arquivo"] = file_path.name

    return df_simple, meta_df

def process_year(year: int):
    year_folder = CLIMATE_ROOT / str(year)
    if not year_folder.exists():
        print(f"Pasta {year_folder} não existe.")
        return

    conn = sqlite3.connect(DB_PATH)

    csv_files = sorted([p for p in year_folder.iterdir() if p.suffix.lower() == ".csv"])
    all_data = []
    all_meta = []
    for f in csv_files:
        df, meta_df = process_csv(f)
        if not df.empty:
            all_data.append(df)
        if not meta_df.empty:
            all_meta.append(meta_df)

    if all_data:
        df_year = pd.concat(all_data, ignore_index=True)
        df_year.to_sql("clima", conn, if_exists="replace", index=False)
        print(f"{len(df_year)} linhas de dados métricos inseridas no banco para {year}.")

    if all_meta:
        meta_year = pd.concat(all_meta, ignore_index=True).drop_duplicates()
        meta_year.to_sql("metadados_estacoes", conn, if_exists="replace", index=False)
        print(f"{len(meta_year)} linhas de metadados de estações inseridas no banco para {year}.")

    conn.close()

if __name__ == "__main__":
    process_year(2024)
    print(DB_PATH)