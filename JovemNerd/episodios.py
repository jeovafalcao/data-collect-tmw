#%%
import requests
import datetime
import json
import pandas as pd
import time

#%%

class Collector:
    
    # instanciar a classe com a url e o nome da instancia que vai salvar
    def __init__(self, url, instance_name):
        self.url = url
        self.instance_name = instance_name
    # funcao para buscar os dados da url
    def get_content(self, **kwargs):
        resp = requests.get(self.url, params=kwargs)
        return resp
    # duas funcoes para salvar arquivos
    def save_parquet(self, data):
        now = datetime.datetime.now().strftime("%Y-%m-%d %H %M %S")
        df = pd.DataFrame(data)
        df.to_parquet(f"data/{self.instance_name}/parquet/{now}.parquet", index=False)

    def save_json(self, data):
        now = datetime.datetime.now().strftime("%Y-%m-%d %H %M %S")
        with open(f"data/{self.instance_name}/json/{now}.json", "w") as open_file:
            json.dump(data, open_file)
    # funcao para invocar uma das duas anteriores
    def save_data(self, data, format = 'json'):
        if format == 'json':
            self.save_json(data)

        elif format == 'parquet':
            self.save_parquet(data)
    # funcao que define como vamos salvar os arquivos e os argumentos
    def get_and_save(self, save_format='json', **kwargs):
        #busca os dados
        resp = self.get_content(**kwargs)
        #verifica se a resposta foi 200
        if resp.status_code == 200:
            # se sim converte para json
            data = resp.json()
            #salva os dados
            self.save_data(data, save_format)
        # se nao retorna um data nulo
        else:
            data = None
            print(f"Request sem sucesso: {resp.status_code}", resp.json())
        return data
    # auto exec, salvando em json e coletando dados ate 2000-01-01
    def auto_exec(self, save_format='json', date_stop='2000-01-01'):
        page = 1
        #vai percorrendo as paginas
        while True:
            print(page)
            data = self.get_and_save(save_format=save_format,
                                      page=page,
                                      per_page=1000)
            if data == None:
                print("Erro ao coletar dados... aguardando")
                time.sleep(60*5)
            else:
                # vai pegar a data da ultima publicacao
                date_last = pd.to_datetime(data[-1]["published_at"]).date()
                # verifica se a data é menor do que a que tem que parar
                if date_last < pd.to_datetime(date_stop).date():
                    # se sim para
                    break
                # se nao verifica o tamanho dos dados
                elif len(data) < 1000:
                    # se for menor do que 1000 para
                    break
                page += 1
                time.sleep(5)

#%%
url = "https://api.jovemnerd.com.br/wp-json/jovemnerd/v1/nerdcasts/"
collect = Collector(url, "episodios")
collect.auto_exec()

# %%
