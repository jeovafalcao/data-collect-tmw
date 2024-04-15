# %%
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd
# %%

cookies = {
    '_gid': 'GA1.2.1089039515.1712004035',
    '_ga_DJLCSW50SC': 'GS1.1.1712004033.1.1.1712004073.20.0.0',
    '_ga_D6NF5QC4QT': 'GS1.1.1712004034.1.1.1712004073.21.0.0',
    '_ga': 'GA1.1.1178755841.1712004033',
    'FCNEC': '%5B%5B%22AKsRol8t37lJMnOgV_xCk4nMUYMo7u_OvL4okXyGMbcsRs5e6Z1zLpD17zhZW5GplFpYgONiEB4XJ3OW96IGedsDoqsiNoMLgs30n-uyNvvoTL_PzqKnwkGx9-5ZqM1fGTnyp6C5lWX8INA2WANlwxF9COm_n_Hnxg%3D%3D%22%5D%5D',
    }

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'pt-PT,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'max-age=0',
    # 'cookie': '_gid=GA1.2.1089039515.1712004035; _ga_DJLCSW50SC=GS1.1.1712004033.1.1.1712004073.20.0.0; _ga_D6NF5QC4QT=GS1.1.1712004034.1.1.1712004073.21.0.0; _ga=GA1.1.1178755841.1712004033; FCNEC=%5B%5B%22AKsRol8t37lJMnOgV_xCk4nMUYMo7u_OvL4okXyGMbcsRs5e6Z1zLpD17zhZW5GplFpYgONiEB4XJ3OW96IGedsDoqsiNoMLgs30n-uyNvvoTL_PzqKnwkGx9-5ZqM1fGTnyp6C5lWX8INA2WANlwxF9COm_n_Hnxg%3D%3D%22%5D%5D',
    'referer': 'https://www.residentevildatabase.com/personagens/',
    'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    }

def get_content(url):
    resp = requests.get(url, headers=headers)
    return resp


def get_basic_infos(soup):
    div_page = soup.find("div", class_ = "td-page-content")
    paragrafo = div_page.find_all('p')[1]
    ems = paragrafo.find_all('em')
    data = {}
    for i in ems:
        chave, valor, *_ = i.text.split(":")
        chave = chave.strip(" ") ## tirar o espaço antes da chave
        data[chave] = valor.strip(" ")

    return data

def get_aparicoes(soup):
    #procurou a div para em seguida procurar o h4 que abaixo dele tinham todas as infos necessárias
    lis = (soup.find("div", class_ = "td-page-content")
           .find("h4")
           .find_next()
           .find_all("li"))

    aparicoes = [i.text for i in lis]
    return aparicoes


def get_personagem_infos(url):
    resp = get_content(url)

    if resp.status_code != 200:
        print('Não foi possível obter os dados')
        return {}
    
    else:
        soup = BeautifulSoup(resp.text)
        data = get_basic_infos(soup)
        data["Aparicoes"] = get_aparicoes(soup)
        return data



def get_links():
    url = 'https://www.residentevildatabase.com/personagens/'
    resp = requests.get(url, headers=headers)

    soup_personagens = BeautifulSoup(resp.text)
    ancoras = (soup_personagens.find("div", class_="td-page-content")
                            .find_all("a"))

    links = [i["href"] for i in ancoras]
    return links

#%%

links = get_links()
data = []
for i in tqdm(links):
    d = get_personagem_infos(i)
    d["link"] = i
    nome = i.strip("/").split("/")[-1].replace("-", " ").title()
    d["Nome"] = nome
    data.append(d)

# %%

url = "https://www.residentevildatabase.com/personagens/ark-thompson/"
thompson = get_content(url)
soup = BeautifulSoup(thompson.text)
get_basic_infos(soup)
# %%
df = pd.DataFrame(data)
df[~df['de nascimento'].isna()]
# %%

#df.to_csv("dados_re.csv", index = False, sep= ';')
# %%
df.to_parquet("dados_re.parquet", index = False)
# %%
df_new = pd.read_parquet("dados_re.parquet")
df_new
# %%
df.to_pickle("dados_re.pkl")
# %%
