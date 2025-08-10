import requests
import pandas as pd
import json
import pyarrow as pa
import math
from datetime import datetime, timezone
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError

# Manejo de json

def get_last_update (file_path):

    try:
        #with es util porque permite simplificar el manejo de archivos, conexiones, db al asegurarse que se usan y liberan de forma correcta, incluso si hay errores.
        
        with open(file_path, "r") as file: 
            ultimoUpdate = json.load(file) # leo el archivo json
            return ultimoUpdate["last_updated"]

    except FileNotFoundError:
        raise FileNotFoundError(f"El archivo JSON en la ruta {file_path} no existe.") # propago el error y lo resuelvo en otro lado!
    
    except json.JSONDecodeError:
        raise json.JSONDecodeError(f"El archivo JSON en la ruta {file_path} no es válido.")

    except KeyError as k:
        raise KeyError(str(k))


def update_last_update (file_path, last_updated):

    try:
        with open(file_path, "w") as file:
            json.dump({"last_updated": last_updated.isoformat()}, file, indent=4)
    
    except FileNotFoundError:
        raise FileNotFoundError(f"El archivo JSON en la ruta {file_path} no existe.") # propago el error y lo resuelvo en otro lado!
    
# Extraccion de datos

def get_data_stations(url, params=None):
    
    try:
        response = requests.get(url,params=params, timeout = 10)
        response.raise_for_status() # excepcion que captura el except
        data = response.json()
        return create_data_frame(data)
        
    except requests.exceptions.RequestException as e:
        print(f"Error al obtener los datos. Código de error: {e}")
        return pd.DataFrame()

def create_data_frame(json_data):

    try:
        df = pd.json_normalize(json_data)
        return df
    
    except Exception as e:
        print(f"Se produjo un error en la construcción del DataFrame: {e}")   
        return pd.DataFrame() 
    
def incremental_extraction(url, file_path, params=None):

    try:

        ultimo_updateJson = get_last_update(file_path=file_path)
        ultimo_update = pd.to_datetime(ultimo_updateJson, utc=True)

        df = get_data_stations(url, params=params)
        if df is None:
            print("No se pudo construir el DataFrame.")
            return pd.DataFrame()
        
        fechas_convertidas = []
        for f in df["last_update"]:
            if f is not None and not math.isnan(f):
                ft = datetime.fromtimestamp(f / 1000, tz=timezone.utc)
                fechas_convertidas.append(ft)
            else:
                fechas_convertidas.append(pd.NaT) #fecha nula en caso de ser null
            
        df["last_update"] = fechas_convertidas

        df_incremental = df[df["last_update"] > ultimo_update]

        if df_incremental.empty:
            print("No hay nuevas actualizaciones desde la última consulta")
            return pd.DataFrame()

        max_timestamp = df["last_update"].max()
        update_last_update(file_path=file_path, last_updated=max_timestamp)
        return df_incremental

    except Exception as e:
        print(f"Se produjo un error en la extracción incremental {e}")
        return pd.DataFrame()


# Almacenamiento de datos

def save_data_as_delta(df, path, mode="overwrite", partition_cols=None):

    write_deltalake(path, df, mode = mode, partition_by = partition_cols)

def merge_new_data_as_delta(data, data_path, predicate):

    try:
        dt = DeltaTable(data_path)
        data_pa = pa.Table.from_pandas(data)
        dt.merge(
            source=data_pa,
            source_alias="source",
            target_alias="target",
            predicate=predicate
        ) \
        .when_matched_update_all() \
        .when_not_matched_insert_all() \
        .execute()
    except TableNotFoundError:
        save_data_as_delta(data, data_path)
    
def save_new_data_as_delta(new_data, data_path, predicate, partition_cols=None):
   
    try:
      dt = DeltaTable(data_path)
      new_data_pa = pa.Table.from_pandas(new_data)

      # Se insertan en target, datos de source que no existen en target
      dt.merge(
          source=new_data_pa,
          source_alias="source",
          target_alias="target",
          predicate=predicate
      ) \
      .when_not_matched_insert_all() \
      .execute()

    # Si no existe la tabla Delta Lake, se guarda como nueva
    except TableNotFoundError:
      save_data_as_delta(new_data, data_path, partition_cols=partition_cols)























