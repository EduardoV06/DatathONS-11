import os
import sqlite3
import pandas as pd
from pathlib import Path
from glob import glob

# =========================
# Configurações de paths
# =========================
BASE_ROOT = Path("/Users/eduardo/Git/ProjectONS/DatathONS-11/ONS-Base")
DICT_ROOT = Path("/Users/eduardo/Git/ProjectONS/DatathONS-11/Dicts")
ONS_ROOT = Path("/Users/eduardo/Git/ProjectONS/DatathONS-11")
DB_PATH = Path("ons_simple1.db")

# =========================
# Funções auxiliares
# =========================
def simplify_column_name(s):
    s = s.lower().strip()
    s = s.replace(" ", "_").replace("ç", "c").replace("ã","a").replace("í","i").replace("ó","o")
    return s

# =========================
# Funções de carregamento
# =========================
def load_parquet_to_sqlite(parquet_path: Path, conn):
    """Carrega um parquet para uma tabela SQLite, mantendo nomes originais."""
    df = pd.read_parquet(parquet_path)
    table_name = parquet_path.stem.lower()

    # Converte datas para datetime
    if "din_instante" in df.columns:
        df["din_instante"] = pd.to_datetime(df["din_instante"], errors="coerce")

    # Salva no SQLite
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    print(f"Tabela '{table_name}' criada com {len(df)} registros.")

    return table_name, df.columns.tolist()

def load_dicts(conn):
    """Carrega dicionários de variáveis e cria tabela de metadados."""
    meta_all = []
    for dict_file in DICT_ROOT.glob("*.csv"):
        try:
            df_dict = pd.read_csv(dict_file, sep=None, engine="python", encoding="latin1")
            if "Código" in df_dict.columns and "Descrição" in df_dict.columns:
                for _, row in df_dict.iterrows():
                    if pd.notna(row["Código"]):
                        meta_all.append({
                            "variavel": row["Código"],
                            "descricao": row["Descrição"]
                        })
        except Exception as e:
            print(f"Erro ao ler dicionário {dict_file.name}: {e}")

    if meta_all:
        pd.DataFrame(meta_all).to_sql("metadata", conn, if_exists="replace", index=False)
        print(f"{len(meta_all)} registros de metadados inseridos.")

def process_usinameta(file_path: Path):
    """Processa metadados de usinas, separando tipo, latitude e longitude."""
    try:
        df = pd.read_csv(file_path, sep=";", encoding="latin1", decimal=",")
    except Exception:
        print(f"Falha ao ler {file_path.name}")
        return pd.DataFrame()

    df.columns = [simplify_column_name(c) for c in df.columns]

    if "tipo_de_usina,x,y" in df.columns:
        tipo_xy = df["tipo_de_usina,x,y"].str.split(",", n=2, expand=True)
        df["tipo_usina_descr"] = tipo_xy[0].str.strip()
        df["latitude"] = pd.to_numeric(tipo_xy[1], errors="coerce")
        df["longitude"] = pd.to_numeric(tipo_xy[2], errors="coerce")
        df = df.drop(columns=["tipo_de_usina,x,y"])

    return df

def process_subestacaometa(file_path: Path):
    """Processa metadados de subestações, separando latitude e longitude."""
    try:
        df = pd.read_csv(file_path, sep=";", encoding="latin1")
    except Exception:
        print(f"Falha ao ler {file_path.name}")
        return pd.DataFrame()

    df.columns = [simplify_column_name(c) for c in df.columns]

    if "data_entrada,x,y" in df.columns:
        xy = df["data_entrada,x,y"].str.split(",", n=1, expand=True)
        df["longitude"] = pd.to_numeric(xy[0], errors="coerce")
        df["latitude"] = pd.to_numeric(xy[1], errors="coerce")
        df = df.drop(columns=["data_entrada,x,y"])

    return df

def load_metadata(conn):
    """Carrega metadados de usinas e subestações diretamente para o banco."""
    meta_files = glob(str(ONS_ROOT / "*Meta.csv"))
    for file_path in meta_files:
        table_name = Path(file_path).stem.lower()
        if "usina" in table_name:
            df = process_usinameta(Path(file_path))
        elif "subestacao" in table_name:
            df = process_subestacaometa(Path(file_path))
        else:
            df = pd.read_csv(file_path, delimiter=";", encoding="latin1")

        if not df.empty:
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            print(f"Tabela '{table_name}' criada com {len(df)} registros.")

# =========================
# Função principal
# =========================
def main():
    conn = sqlite3.connect(DB_PATH)

    # Processa arquivos parquet da ONS
    for parquet_file in BASE_ROOT.glob("*.parquet"):
        try:
            load_parquet_to_sqlite(parquet_file, conn)
        except Exception as e:
            print(f"Erro ao processar {parquet_file.name}: {e}")

    # Carrega dicionários
    load_dicts(conn)

    # Carrega metadados
    load_metadata(conn)

    conn.close()
    print("Processamento ONS concluído.")

if __name__ == "__main__":
    main()