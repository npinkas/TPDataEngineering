import requests
import pandas as pd
import json

def obtenerDatosMonedas(url, params=None):
    
    try:
        response = requests.get(url,params=params, timeout = 10)
        response.raise_for_status() # excepcion que captura el except
        data = response.json()
        return crearDataFrame(data)
        
    except requests.exceptions.RequestException as e:
        print(f"Error al obtener los datos. Código de error: {e}")
        return None

def crearDataFrame(json_data):

    try:
        df = pd.json_normalize(json_data) #lo convierto en un dataframe
        return df
    
    except Exception as e:
        print(f"Se produjo un error en la construcción del DataFrame: {e}")   
        return None 


# Extracción incremental
"""
1. Leer el json y obtener el last update
2. Transformar el last update a datetime para poder comparar 
3. Obtener el lastupdate del json de la api y transformarlo a datetime
4. Traer todo lo que sea mayor al last update
5. Actualizar el last update
"""

def getLastUpdate (file_path):

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


def updateLastUpdate (file_path, last_updated):

    try:
        with open(file_path, "w") as file:
            json.dump({"last_updated": str(last_updated)}, file, default=str, indent=4)
    
    except FileNotFoundError:
        raise FileNotFoundError(f"El archivo JSON en la ruta {file_path} no existe.") # propago el error y lo resuelvo en otro lado!
    
    




def extraccionIncremental(url, file_path, params=None):

    try:

        ultimo_updateJson = getLastUpdate(file_path=file_path)
        ultimo_update = pd.to_datetime(ultimo_updateJson).tz_convert("UTC")

        df = obtenerDatosMonedas(url, params=params)
        if df is None:
            print("No se pudo construir el DataFrame.")
            return

        df["last_updated"] = pd.to_datetime(df["last_updated"])

        df_incremental = df[df["last_updated"] > ultimo_update]
        
        if not df_incremental.empty:

            updateLastUpdate(file_path=file_path, last_updated=max(df["last_updated"]))

            return df_incremental
        
        return pd.DataFrame()

    except Exception as e:

        print(f"Se produjo un error en la extracción incremental {e}")






























