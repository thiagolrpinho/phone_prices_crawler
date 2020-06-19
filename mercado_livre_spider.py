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
    ROBOTSTXT_OBEY = True
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
        device_item_url = parse.unquote(url)

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
                        device_price += int(number)*10**(
                            (len(raw_price_numbers_list)-i-1)*3)
                    device_price = int(device_price)
                else:
                    device_price = int(device_raw_price)
                FOUND_ELEMENTS.append(
                    [device_item_url, device_name, device_price])
        else:
            FOUND_ELEMENTS.append(
                [device_item_url, [], []])


FOUND_ELEMENTS = []
