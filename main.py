import utils
from configparser import ConfigParser

def main ():

    parser = ConfigParser()
    parser.read("pipeline.conf")
    api_key = parser["api-credentials"]["api-key"]         
    
    urlBase = "https://api.jcdecaux.com/vls/v1"

    endpointDinamico = "stations"
    
    paramsDinamico = {
        "contract": "lyon",
        "apiKey" : api_key
    }

    paramsEstatico = {
        "apiKey" : api_key
    }
    endpointEstatico = "contracts"

    urlDinamica= f"{urlBase}/{endpointDinamico}"
    urlEstatica = f"{urlBase}/{endpointEstatico}"

    data_dinamico = utils.extraccionIncremental(urlDinamica, "metadata/metadata.json", paramsDinamico)
    print(data_dinamico.head())
    
    print(utils.obtenerDatosEstaciones(urlEstatica, paramsEstatico).head())


if __name__ == "__main__":
    main()