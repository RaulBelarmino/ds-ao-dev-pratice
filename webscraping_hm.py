#Imports
import re
import os
import logging
import sqlite3
import requests
import pandas   as pd
import numpy    as np
from sqlalchemy import create_engine
from bs4        import BeautifulSoup
from datetime   import datetime

def data_collection(url, headers):

    # create request and BeautifulSoup
    page = requests.get( url, headers=headers )
    soup = BeautifulSoup(page.text, 'html.parser')

    # get pagesize max to extract all items
    total_item = soup.find('h2', class_='load-more-heading')
    total_item = total_item.get('data-total')
    page_number = np.round(int(total_item)/36)

    # create new request to extract all items
    url01 = url + '?sort=stock&image-size=small&image=model&offset=0&page-size=' + str(int(page_number*36))
    page = requests.get( url01, headers=headers )
    soup = BeautifulSoup(page.text, 'html.parser')

    # idxing product list
    products = soup.find('ul', class_='products-listing small')
    product_list = products.find_all('article', class_='hm-product-item')

    # collect all products id
    product_id = [p.get('data-articlecode') for p in product_list]

    # collect all products category
    product_category = [p.get('data-category') for p in product_list]

    # collect all products name
    product_list = products.find_all('a', class_='link')
    product_name = [p.get_text() for p in product_list]

    # collect all products price
    product_list = products.find_all('span',class_='price regular')
    product_price = [p.get_text() for p in product_list]

    # transform all collects in dataframe
    data = pd.DataFrame([product_id, product_name, product_category,product_price]).T
    data.columns = ['product_id', 'product_name', 'product_category','product_price']

    # created style id
    data['style_id'] = data['product_id'].apply(lambda x: x[:-3])

    logger.info('Total showroom shape: %s', data.shape)

    return data

def data_collect_by_product(data, headers):
    # empty dataframe
    df_compositions = pd.DataFrame()

    # unique columns for all products composition
    aux = []
    cols = ['product_name', 'price', 'Art. No.', 'Composition', 'Fit', 'Size']

    df_pattern = pd.DataFrame(columns=cols)

    for i in range(len(data)):
        # API request
        url = 'https://www2.hm.com/en_us/productpage.' + data.loc[i, 'product_id'] + '.html'
        logger.debug('Product: %s', url)

        page = requests.get(url, headers=headers)

        # BeautifulSoup Object
        soup = BeautifulSoup(page.text, 'html.parser')

        # =======================color name====================#
        product_list = soup.find_all('a', class_=['filter-option miniature active', 'filter-option miniature'])
        color_name = [p.get('data-color') for p in product_list]

        # color id
        product_list = soup.find_all('a', class_=['filter-option miniature active', 'filter-option miniature'])
        color_id = [p.get('data-articlecode') for p in product_list]

        df_color = pd.DataFrame([color_id, color_name]).T
        df_color.columns = ['product_id', 'color_name']

        # generate style id + color id
        df_color['style_id'] = df_color['product_id'].apply(lambda x: x[:-3])
        df_color['color_id'] = df_color['product_id'].apply(lambda x: x[-3:])

        # ======================= composition ====================#
        for j in range(len(df_color)):
            url = 'https://www2.hm.com/en_us/productpage.' + df_color.loc[j, 'product_id'] + '.html'
            logger.debug('Color: %s', url)

            page = requests.get(url, headers=headers)

            # BeautifulSoup Object
            soup = BeautifulSoup(page.text, 'html.parser')

            # product name
            product_name = soup.find_all('h1', class_='primary product-item-headline')
            product_name = re.findall(r'\w+\s\w+', product_name[0].get_text())[0]
            product_name = ['product_name', product_name]

            # product price
            product_price = soup.find_all('div', class_='primary-row product-item-price')
            product_price = re.findall(r'\d+\.?\d+', product_price[0].get_text())[0]
            product_price = ['price', product_price]

            # dataframe name and price
            df_aux = pd.DataFrame([product_name, product_price]).T
            df_aux.columns = df_aux.iloc[0]
            df_aux = df_aux.iloc[1:].fillna(method='ffill')

            # collect composition
            product_composistion_list = soup.find_all('div', class_='pdp-description-list-item')
            product_composition = [list(filter(None, p.get_text().split('\n'))) for p in product_composistion_list]

            # rename data
            df_composition = pd.DataFrame(product_composition).T
            df_composition.columns = df_composition.iloc[0]

            # delete first row
            df_composition = df_composition.iloc[1:].fillna(method='ffill')

            # remove pocket and lining
            df_composition = df_composition[~df_composition['Composition'].str.contains('Pocket lining:', na=False)]
            df_composition = df_composition[~df_composition['Composition'].str.contains('Lining:', na=False)]
            df_composition['Composition'] = df_composition['Composition'].apply(lambda x: x.replace('Shell:', '').lower())

            # garantee the same number of columns
            df_aux = pd.merge(df_aux, df_composition, how='cross')

            df_comp = pd.concat([df_pattern, df_aux], axis=0)
            df_comp = df_comp[['product_name', 'price', 'Art. No.', 'Fit', 'Size', 'Composition']]

            # rename columns
            df_comp.columns = ['product_name', 'price', 'product_id', 'fit', 'size', 'composition']

            # keep new columns if it show up
            aux = aux + df_composition.columns.tolist()

            # merge data color + composition
            df_comp = pd.merge(df_comp, df_color, how='left', on='product_id')

            # all product
            df_compositions = pd.concat([df_compositions, df_comp], axis=0)

    # Join showroom data + details
    df_compositions['style_id'] = df_compositions['product_id'].apply(lambda x: x[:-3])
    df_compositions['color_id'] = df_compositions['product_id'].apply(lambda x: x[-3:])

    # scrapy datetime
    df_compositions['scrapy_datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # merge with data showroom to get category
    data_details = pd.merge(df_compositions.reset_index(drop=True),data[['product_id', 'product_category']], how='left',on='product_id')

    logger.info('Data details shape: %s', df_compositions.shape)
    logger.info('Data details shape: %s', data_details.shape)

    return data_details

def data_cleaning(data_details):
    #product_id
    df = data_details.dropna(subset=['product_id'])

    # reindex
    df = df[['product_id','product_name','product_category','price','fit','style_id','color_name','color_id','composition','size','scrapy_datetime']]

    # product_price
    df['price'] = df['price'].astype(float)

    # product name
    df['product_name'] = df['product_name'].str.replace('\n','')
    df['product_name'] = df['product_name'].str.replace('\t','')
    df['product_name'] = df['product_name'].str.replace('®','')
    df['product_name'] = df['product_name'].str.replace(' ','_').str.lower()

    # color_name
    df['color_name'] = df['color_name'].apply(lambda x: x.replace(' ', '_').lower() if type(x) == str else x)

    # fit
    df['fit'] = df['fit'].apply(lambda x: x.replace(' ', '_').lower() if type(x) == str else x)

    # size number
    df['size_number'] = df['size'].apply(lambda x: re.search('\d{3}cm', x).group(0) if pd.notnull(x) else x)
    df['size_number'] = df['size_number'].apply(lambda x: re.search('\d{3}', x).group(0) if pd.notnull(x) else x)

    # size model
    df['size_model'] = df['size'].str.extract('(\d+/\\d+)')

    # break composition comma
    df['composition'] = df['composition'].apply(lambda x: x.replace(' ',''))
    df1 = df['composition'].str.split(',', expand=True).reset_index(drop=True)

    df_ref = pd.DataFrame(index=np.arange(len(df)), columns=['cotton', 'polyester', 'elastane','elasterell'])

    #============= Break Composition Comma
    # cotton
    df_cotton_0 = df1.loc[df1[0].str.contains('cotton', na=True), 0 ]
    df_cotton_0.name = 'cotton'

    df_cotton_1 = df1.loc[df1[1].str.contains('cotton', na=True), 1]
    df_cotton_1.name = 'cotton'

    # combine
    df_cotton = df_cotton_0.combine_first(df_cotton_1)

    # concat cotton
    df_ref = pd.concat([df_ref, df_cotton], axis=1)
    df_ref = df_ref.iloc[:, ~df_ref.columns.duplicated(keep='last')]

    # polyester
    df_polyester = df1.loc[df1[1].str.contains('polyester', na=True), 1 ]
    df_polyester.name = 'polyester'

    # concat polyester
    df_ref = pd.concat([df_ref, df_polyester], axis=1)
    df_ref = df_ref.iloc[:, ~df_ref.columns.duplicated(keep='last')]

    # elastane
    df_elastane_1 = df1.loc[df1[1].str.contains('elastane', na=True), 1]
    df_elastane_1.name = 'elastane'

    df_elastane_2 = df1.loc[df1[2].str.contains('elastane', na=True), 2]
    df_elastane_2.name = 'elastane'

    # combine
    df_elastane = df_elastane_1.combine_first(df_elastane_2)

    # concat elastane
    df_ref = pd.concat([df_ref, df_elastane], axis=1)
    df_ref = df_ref.iloc[:, ~df_ref.columns.duplicated(keep='last')]

    # elasterell
    df_elasterell = df1.loc[df1[1].str.contains('elasterell', na=True), 1 ]
    df_elasterell.name = 'elasterell'

    # concat elasterell
    df_ref = pd.concat([df_ref, df_elasterell], axis=1)
    df_ref = df_ref.iloc[:, ~df_ref.columns.duplicated(keep='last')]

    df_aux = pd.concat([df['product_id'], df_ref], axis=1)

    # format composition data
    df_aux['cotton'] = df_aux['cotton'].apply(lambda x: int( re.search('\d+',x).group(0)) / 100 if pd.notnull(x) else x)
    df_aux['polyester'] = df_aux['polyester'].apply(lambda x: int( re.search('\d+',x).group(0)) / 100 if pd.notnull(x) else x)
    df_aux['elastane'] = df_aux['elastane'].apply(lambda x: int( re.search('\d+',x).group(0)) / 100 if pd.notnull(x) else x)
    df_aux['elasterell'] = df_aux['elasterell'].apply(lambda x: int( re.search('\d+',x).group(0)) / 100 if pd.notnull(x) else x)

    # final join
    df_aux = df_aux.fillna(0)
    df = pd.merge(df, df_aux, on='product_id',how='left')

    # drop columns
    df = df.drop(columns=['size','composition'], axis=1)

    # drop duplicates
    data_cleaned = df.drop_duplicates().reset_index(drop=True)

    logger.info('Data cleaned shape: %s', data_cleaned.shape)

    return data_cleaned

def data_insert_db(data_cleaned):
    data_insert = data_cleaned[['product_id', 'style_id', 'color_id', 'product_name', 'product_category', 'color_name', 'fit', 'price', 'size_number', 'size_model', 'cotton', 'polyester', 'elastane', 'elasterell', 'scrapy_datetime']]

    # connect to db and execute query's
    conn = create_engine('sqlite:///database_hm.sqlite', echo=False)

    #insert data
    data_insert.to_sql('showroom', con=conn, if_exists='append', index=False)

    return None

if __name__== '__main__':
    #logging
    path = 'C:/Users/rauul/repos/ds-ao-dev-pratice/'

    if not os.path.exists(path + 'Logs'):
        os.makedirs(path + 'Logs')

    logging.basicConfig(
        filename= path + 'Logs/webscraping_hm.log',
        level= logging.DEBUG,
        format= '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt= '%Y-%m-%d %H:%M:%S')

    logger = logging.getLogger('webscraping_hm')

    # parameters and constants
    url = 'https://www2.hm.com/en_us/men/products/jeans.html'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5),AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

    # data collection
    data = data_collection(url, headers)
    logger.info('Data Collect Done!')

    # data collection by product
    data_details = data_collect_by_product(data, headers)
    logger.info('Data Collect by Product Done!')

    # data cleaning
    data_cleaned = data_cleaning(data_details)
    logger.info('Data Cleaned Done!')

    # data insertion
    data_insert_db(data_cleaned)
    logger.info('Data Insertion Done!')