from metaflow import FlowSpec, step, Parameter


def join_relative_folder_path(filename: str) -> str:
    """
    A convenience function to get the absolute path to a file in this
    data's directory. This allows the file to be launched from any
    directory.

    """
    import os

    path_to_data_folder = os.path.join(os.path.dirname(__file__), "data/")
    return os.path.join(path_to_data_folder, filename)
from metaflow import FlowSpec, step, Parameter


def join_relative_folder_path(filename: str) -> str:
    """
    A convenience function to get the absolute path to a file in this
    data's directory. This allows the file to be launched from any
    directory.

    """
    import os

    path_to_data_folder = os.path.join(os.path.dirname(__file__), "data/")
    return os.path.join(path_to_data_folder, filename)

class DevicesPriceRangesFlow(FlowSpec):
    """
    Flow created to look online for devices
    prices available in a csv dataset and
    based on online prices update their
    market prices and create price ranges
    as very low, low, medium, high and very
    high prices.
    """
    device_reference_csv_path = Parameter(
        "device_reference_csv_path",
        help="The path to reference dataset available as a csv.",
        default=join_relative_folder_path('devices_using_ribonapp_data'))

    @step
    def start(self):
        """
            Loading the reference csv
            and treating the data by deleting
            duplicates and keeping  only informative device names.
        """
        import pandas as pd
        from datetime import datetime

        self.start_time = datetime.now()
        found_elements = []
        devices_df = pd.read_csv(
            self.device_reference_csv_path + '.csv',
            skiprows=1, names=['model_name', 'old_price_range'])

        devices_df.drop_duplicates(['model_name'], inplace=True)
        devices_df.dropna(axis=0, inplace=True)

        informative_names_mask = devices_df['model_name'].str.len() > 1
        self.devices_df = devices_df[informative_names_mask]
        self.next(self.crawling_for_prices)

    @step
    def crawling_for_prices(self):
        """ 
            Crawling for multiple prices at online webstores
            to update values.
        """
        from scrapy.crawler import CrawlerProcess
        from mercado_livre_spider import MercadoLivreSpider, FOUND_ELEMENTS
        from urllib import parse
        import pandas as pd

        devices_df = self.devices_df

        url_base = "https://celulares.mercadolivre.com.br/"
        item_list = devices_df['model_name'].to_list()
        url_list_unquoted = [
            url_base + item_name
            for item_name in item_list]
        url_list = [
            url_base + parse.quote(item_name)
            for item_name in item_list]

        process = CrawlerProcess()
        process.crawl(
            MercadoLivreSpider,
            start_urls=url_list)

        url_list_treated = [
            "-".join(url_unquoted.split(" "))
            for url_unquoted in url_list_unquoted]
        devices_df['url'] = url_list_treated
        process.start()

        self.extracted_data_df = pd.DataFrame(
            data=FOUND_ELEMENTS,
            columns=[
                'url',
                'device_name',
                'device_price'])

        self.next(self.treating_prices)

    @step
    def treating_prices(self):
        """
            Removing outliers and non related prices.
            And choosing the median value for each device.
        """
        import pandas as pd
        import numpy as np

        device_raw_extracted_data_df = self.extracted_data_df

        device_raw_extracted_data_df.dropna(
            axis=0, inplace=True)
        device_raw_extracted_data_df['device_price'] = pd.to_numeric(
            device_raw_extracted_data_df['device_price'],
            errors='coerce').fillna(0).astype(np.int64)

        INVALID_WORDS = set([
            'DEFEITO', 'TROCO', 'QUEBRADO', 'CABO', 'CONECTOR', 'PEÇA'])
        valid_rows_mask = ~device_raw_extracted_data_df[
            'device_name'].str.contains('|'.join(INVALID_WORDS), na=False)
        device_extracted_data_df = device_raw_extracted_data_df[
            valid_rows_mask]
        self.extracted_data_df = device_extracted_data_df.groupby(
            "url", as_index=False).median()

        self.next(self.creating_price_ranges)

    @step
    def creating_price_ranges(self):
        """
            Using prices found to create
            price ranges based on ibge 
            salary ranges
        """
        import pandas as pd

        device_extracted_data_df = self.extracted_data_df
        devices_df = self.devices_df

        ibge_price_ranges_labels = [
            'very_low', 'low', 'medium', 'high', 'very_high']
        salaries_ibge_ranges = [
            (0, 1908), (1909, 2862), (2863, 5724), (5725, 8566)]
        ibge_price_ranges = [0]

        for i, salarie_range in enumerate(salaries_ibge_ranges):
            threshold_price = (salarie_range[0] + salarie_range[1])//3
            # As the values are so low, we have to increment them with the
            # previous threshold
            threshold_price += ibge_price_ranges[i-1]
            ibge_price_ranges.append(threshold_price)

        ibge_price_ranges.append(float('inf'))
        device_extracted_data_df['ibge_price_range'] = pd.cut(
            device_extracted_data_df['device_price'],
            bins=ibge_price_ranges,
            labels=ibge_price_ranges_labels)
        device_extracted_data_df['url'] = device_extracted_data_df[
            'url'].str.upper()
        devices_df['url'] = devices_df['url'].str.upper()
        self.extracted_data_df = device_extracted_data_df.merge(
            right=devices_df,
            left_on='url',
            right_on='url', how='inner')

        self.next(self.end)

    @step
    def end(self):
        """
        End the flow.
        Output data available on
        Flow variable:
            extracted_data_df
        Inside Data Folder:
            extracted_device_data_2.csv
        """
        from datetime import datetime
        import pandas as pd

        extracted_filename = 'extracted_device_data_2.csv'

        self.extracted_data_df.to_csv(
            join_relative_folder_path(extracted_filename), index=False)

        self.end_time = datetime.now()
        self.total_time = self.end_time - self.start_time

if __name__ == '__main__':
    DevicesPriceRangesFlow()
