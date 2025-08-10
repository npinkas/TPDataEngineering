import utils
from configparser import ConfigParser

def main ():

    # obtengo api-key del archivo config

    parser = ConfigParser()
    parser.read("pipeline.conf")
    api_key = parser["api-credentials"]["api-key"]         
    
    urlBase = "https://api.jcdecaux.com/vls/v1"

    bronze_dir = "datalake/bronze/luchtmeetnet_api"

    # ---------------- extraccion full ----------------

    paramsEstatico = {
        "apiKey" : api_key
    }

    endpointEstatico = "contracts"

    contracts_dir = f"{bronze_dir}/{endpointEstatico}"

    urlEstatica = f"{urlBase}/{endpointEstatico}"
        
    df_estatico = utils.obtenerDatosEstaciones(urlEstatica, paramsEstatico)
    
    utils.merge_new_data_as_delta(df_estatico, contracts_dir, "target.name = source.name")


    # ---------------- extraccion incremental ----------------
    
    endpointDinamico = "stations"

    paramsDinamico = {
        "contract": "lyon",
        "apiKey" : api_key
    }
    
    urlDinamica= f"{urlBase}/{endpointDinamico}"

    df_dinamico = utils.extraccionIncremental(urlDinamica, "metadata/metadata.json", paramsDinamico)
    
    if not df_dinamico.empty:

        df_dinamico["last_update"] = utils.pd.to_datetime(df_dinamico.last_update)
        df_dinamico["fecha"] = df_dinamico.last_update.dt.date
        df_dinamico["hora"] = df_dinamico.last_update.dt.hour

        stations_dir = f"{bronze_dir}/{endpointDinamico}"

        utils.save_new_data_as_delta(df_dinamico, stations_dir, predicate= "target.number = source.number AND target.last_update = source.last_update", partition_cols=["fecha", "hora"])
 


if __name__ == "__main__":
    main()