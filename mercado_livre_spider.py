import os
from pathlib import Path

from urllib import parse
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd
import scrapy
import pandas
import numpy
from lxml import html
import re
import os.path
import shutil
from scrapy.crawler import CrawlerProcess
from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging

class MercadoLivreSpider(scrapy.Spider):
    name = 'mercado_livre'
    novo_filter_xpath = '/html/body/main/div[2]/div/aside/section[2]/dl[@id="id_ITEM_CONDITION"]/dd[1]/a/@href'
    products_articles_criteria = '@class="results-item highlighted article stack product "'
    product_information_criteria = '@class="item__info-container highlighted "'
    item_prices_criteria = '@class="price__container"'
    webproducts_names_xpath = '/html/body/main/div[2]/div/section/ol/li[' + products_articles_criteria + ']/div/div[' + product_information_criteria + ']/div/h2/a/span/text()'
    webproducts_prices_xpath = '/html/body/main/div[2]/div/section/ol/li[' + products_articles_criteria +\
        ']/div/div['+ product_information_criteria + ']/div/div[' + item_prices_criteria + ']/div/span[2]/text()'
    
    def parse(self, response):
        url = response.request.url
        device_item_name = url[url.rfind('/')+1:]

        if response and response.body:
            response_content_str = response.body
            webpage_tree = html.fromstring(response_content_str)

            webproducts_names_found = webpage_tree.xpath(
                self.webproducts_names_xpath)
            webproducts_prices_found = webpage_tree.xpath(
                self.webproducts_prices_xpath)
            
            if len(webproducts_names_found) > len(webproducts_prices_found):
                webproducts_names_found = webproducts_names_found[
                    :len(webproducts_prices_found)]
            elif len(webproducts_names_found) < len(webproducts_prices_found):
                webproducts_prices_found = webproducts_prices_found[
                    :len(webproducts_names_found)]
            
            # creating and editing Tabela_municipios.txt.
            for device_raw_name, device_raw_price in zip(
                    webproducts_names_found, webproducts_prices_found):
                device_name = device_raw_name.upper()
                if '.' in device_raw_price:
                    # In this case there are more than one dot symbol
                    device_price = 0
                    raw_price_numbers_list = device_raw_price.split('.')
                    for i, number in enumerate(raw_price_numbers_list):
                        device_price += int(number)*10**((len(raw_price_numbers_list)-i-1)*3)
                    device_price = int(device_price)
                else:
                    device_price = int(device_raw_price)
                found_elements.append(
                    [device_item_name, url, [], device_name, device_price])
        else:
            found_elements.append(
                [device_item_name, url, [], [], []])


found_elements = []
parent_folder = Path(os.path.dirname(__file__))
print("rugoso ", parent_folder)

RELATIVE_PATH_TO_DATA_FOLDER = os.path.join(
    parent_folder, "data/")
filename = 'devices_using_ribonapp_data'
filename_with_path = RELATIVE_PATH_TO_DATA_FOLDER + filename + '.csv'
devices_df = pd.read_csv(
    filename_with_path, skiprows=1, names=['model_name', 'old_price_range'])

devices_df.drop_duplicates(['model_name'], inplace=True)
devices_df.dropna(axis=0, inplace=True)

informative_names_mask = devices_df['model_name'].str.len() > 1
devices_df = devices_df[informative_names_mask]

url_base = "https://celulares.mercadolivre.com.br/"
item_list = devices_df['model_name'].to_list()

process = CrawlerProcess()
process.crawl(
    MercadoLivreSpider, start_urls=[url_base + parse.quote(item_name) for item_name in item_list])

process.start()

extracted_data_df = pd.DataFrame(
    data=found_elements, columns=['item_original_name', 'url', 'improved_url', 'device_name', 'device_price'])
extracted_filename = 'scrapy_found_devices'
extracted_filename_with_path = RELATIVE_PATH_TO_DATA_FOLDER + extracted_filename + '.csv'
extracted_data_df.to_csv(extracted_filename_with_path, index=False)
