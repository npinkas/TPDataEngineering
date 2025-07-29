import utils

def main ():

       
    urlBase = "https://api.coingecko.com/api/v3"

    endpointDinamico = "coins/markets"
    paramsDinamico = {
        "vs_currency": "usd"
    }
    endpointEstatico = "coins/list"

    urlDinamica= f"{urlBase}/{endpointDinamico}"
    urlEstatica = f"{urlBase}/{endpointEstatico}"

    data_dinamico = utils.extraccionIncremental(urlDinamica, "metadata/metadata.json", paramsDinamico)
    print(data_dinamico.head())
    
    print(utils.obtenerDatosMonedas(urlEstatica).head())


if __name__ == "__main__":
    main()