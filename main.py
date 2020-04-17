import requests
from urllib import parse
import re

import pandas as pd


item_data = pd.load_csv()
url_base = "https://celulares.mercadolivre.com.br/"

item_list = [
    "Galaxy TabS 10.5",
    "Galaxy TabS 8.4",
    "Galaxy TabS3",
    "Galaxy Trend Lite  GT-S7390L",
    "Galaxy Trend Lite GT-S7392L DUOS",
    "Galaxy Trend Plus GT-S7580",
    "Galaxy Win 2 Duos TV",
    "Galaxy WinDuos GT-I8552B",
    "Galaxy Y Duos TV GT-S6313T",
    "Galaxy Young 2",
    "Galaxy Young Plus (GT-S6293T)",
    "Galaxy Young2",
    "Galaxy Young2 Pro",
    "Gigaset GS370_Plus"]

url_list = [url_base + parse.quote(item_name) for item_name in item_list]
item_list_tuple = []

for url, item_original_name in zip(url_list, item_list):
    print(url)
    r = requests.get(url)
    text_content = str(r.text)
    item_name = re.search(r'class="main-title">([\s\S]+?)<\/span>', text_content)
    item_price = re.search(r'price__fraction">(\S+)<\/span>', text_content)
    if item_name:
        print(item_name.groups())
        item_name = item_name.groups()[0]
    if item_price:
        print(item_price.groups())
        item_price = item_price.groups()[0]
    print('\n')
    item_list_tuple.append((item_original_name, item_name, item_price))


extracted_data = pd.DataFrame(data=item_list_tuple, columns=['item_original_name','item_extracted_name', 'item_price'])
print(extracted_data.info())
extracted_data.to_csv('extracted_cellphone_data.csv', index=False)
