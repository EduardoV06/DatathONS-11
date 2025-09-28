import pandas as pd
from pathlib import Path
import sqlite3
import fnmatch

CLIMATE_ROOT = Path(__file__).parent / "DatathONS-11" / "Climate"
DB_PATH = Path("climate.db")

def csvs_to_sqlite(year: int):
    year_folder = CLIMATE_ROOT / str(year)
    if not year_folder.exists():
        raise FileNotFoundError(f"Pasta Climate/{year} não encontrada.")

    csv_files = [f for f in year_folder.iterdir() if fnmatch.fnmatch(f.name.lower(), "*.csv")]
    if not csv_files:
        raise FileNotFoundError(f"Nenhum CSV encontrado para o ano {year}.")

    conn = sqlite3.connect(DB_PATH)

    for file in csv_files:
        try:
            meta_df = pd.read_csv(file, sep=";", encoding="latin1", nrows=8, header=None)
            meta_df = meta_df.T  # transpor para ter as variáveis como colunas
            meta_df.columns = ['regiao', 'uf', 'estacao', 'codigo_wmo', 'latitude', 'longitude', 'altitude', 'data_de_fundacao']
            meta_df['arquivo'] = file.name
            meta_df['ano'] = year

            meta_df.to_sql("estacoes", conn, if_exists="append", index=False)

            df = pd.read_csv(
                file,
                sep=";",
                encoding="latin1",
                decimal=",",
                skiprows=8,
                header=0
            )

            df.columns = [c.strip().lower().replace(" ", "_").replace("ç","c").replace("ã","a") for c in df.columns]

            df = df.loc[:, ~df.columns.duplicated()]

            df['arquivo'] = file.name

            existing_cols = pd.read_sql_query(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='climate'", conn
            )
            if existing_cols.empty:
                df.to_sql("climate", conn, if_exists="replace", index=False)
            else:
                df.to_sql("climate", conn, if_exists="append", index=False)

        except Exception as e:
            print(f"Falha ao processar {file.name}: {e}")
            continue

    conn.close()
    print("Banco SQLite atualizado com sucesso.")

# Exemplo
csvs_to_sqlite(2024)