import requests

def fetch_breweries():
    """
    Fetches brewery data from the Open Brewery DB API.
    Returns:
        str: The response text from the API.
    """
    url = "https://api.openbrewerydb.org/v1/breweries"
    headers = {
        'Cookie': '__cf_bm=CA6m2pSyBzf.552wfnIOZDOwHHI3ycLLiInGec1iEZU-1771249761-1.0.1.1-.XE1xVeKjZJKk_kUErN8vPgoes.5L_pevFJezpRQTcGtPKYgiYOi3IZCl3NDwDvvuJqpGpcejVBLXU4xfBmOml4gFx34mfQ2g4vW_u73D3w'
    }
    response = requests.get(url, headers=headers)
    return response.text

if __name__ == "__main__":
    data = fetch_breweries()
    print(data)