import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import argparse
import subprocess
import warnings
warnings.filterwarnings('ignore')

def setup_args():
    parser = argparse.ArgumentParser(
        description = 'Basket Lenta(c)White Dramatic Monkeys',
        epilog='''
    
    Example usage
    !lenta.py -date "Saturday, Sunday" -summ 1000 -ta 18-30 -gender M -city Moscow -social Yes
    
    ''',
        formatter_class = argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        '-date',
        help = 'day of the week(separated by comma and space with capital letter) or specific days(format 2016-11-01) in ".."',
        type = str,
        required = True
    )
    parser.add_argument(
        '-summ',
        help = 'basket amount',
        type = int,
        required = True
    )
    parser.add_argument(
        '-ta',
        help = 'target audience',
        type = str,
        required = True
    )
    parser.add_argument(
        '-gender',
        help='gender',
        type = str,
        required = True
    )    
    parser.add_argument(
        '-city',
        help = 'city',
        type = str,
        required = True
    )
    parser.add_argument(
        '-social',
        help = 'social (Yes or No)',
        type = str,
        default = 'No'
    )
 
 
    args = parser.parse_args()
 
    return args


def max_number(df_max_number,sum_top_number):
    x_top=0
    max_num=0
    while x_top <= sum_top_number and max_num<=40:
        x_top=x_top+df_max_number['sum'].iloc[max_num]
        max_num=max_num+1
    return max_num


def basket(day_top,sum_top,ta,gender,city,sale,transactions,clients,materials,plants):
    if day_top[0].isalpha():
        transaction_day=transactions.copy()
        transaction_day['weekday'] = transaction_day['chq_date'].dt.weekday_name
        transaction_day=transaction_day[transaction_day['weekday'].isin(day_top)]
    else:
        transaction_day=transactions[transactions['chq_date'].isin(day_top)]
    
    basket_plants = plants.loc[plants['city']==city]
    transaction_day=transaction_day.loc[transaction_day['plant'].isin(basket_plants['plant'].to_list())]
    clients['year']=2020-clients['birthyear']
    basket_clients = clients.loc[(clients['gender']==gender)&(clients['year']>=int(ta.split('-')[0]))&(clients['year']<=int(ta.split('-')[1]))]
    transaction_day=transaction_day.loc[transaction_day['client_id'].isin(basket_clients['client_id'].to_list())]
    
    transaction_day['sum']=transaction_day['sales_sum']/transaction_day['sales_count']
    transaction_day=transaction_day.reset_index(drop=True)
    
    df_basket=pd.DataFrame()
    
    if sale=='Yes':
        df_basket['material']=transaction_day['material'].unique()
        replace_dict = dict(materials[['material','hier_level_4']].to_dict('split')['data'])
        df_basket['hier_level_4'] = df_basket['material'].map(replace_dict)
        transaction_day_i=transaction_day.loc[transaction_day['material'].isin(df_basket['material'].to_list())]
        transaction_day_i=transaction_day_i.reset_index(drop=True)
        df_basket['sum'] = df_basket['material'].map(transaction_day_i.groupby('material')['sum'].mean().to_dict())
        df_basket.sort_values(by=['sum'], inplace=True)
        df_basket=df_basket.reset_index(drop=True)
        df_basket=df_basket.drop_duplicates(subset=['hier_level_4'])
        df_basket=df_basket.reset_index(drop=True)
    else:
        df_basket['material']=transaction_day['material'].value_counts().index
        replace_dict = dict(materials[['material','hier_level_4']].to_dict('split')['data'])
        df_basket['hier_level_4'] = df_basket['material'].map(replace_dict)
        df_basket=df_basket.drop_duplicates(subset=['hier_level_4'])
        df_basket=df_basket.reset_index(drop=True)
    
    transaction_day_i=transaction_day.loc[transaction_day['material'].isin(df_basket['material'].to_list())]
    transaction_day_i=transaction_day_i.reset_index(drop=True)
    df_basket['sum'] = df_basket['material'].map(transaction_day_i.groupby('material')['sum'].mean().to_dict())
    basket_num=max_number(df_basket,sum_top)
    df_basket=df_basket[:basket_num]
    return df_basket


def main():
    args = setup_args()
    
    day_basket = args.date.split(', ')
    sum_basket = args.summ
    ta_basket = args.ta
    gender_basket = args.gender
    city_basket = args.city
    sale_basket = args.social
    
    data_basket=pd.DataFrame()
    transactions_basket = pd.read_parquet('transactions.parquet')
    clients_basket = pd.read_csv('clients.csv')
    materials_basket = pd.read_csv('materials.csv')
    plants_basket = pd.read_csv('plants.csv')
    data_basket=basket(day_basket,sum_basket,ta_basket,gender_basket,city_basket,sale_basket,transactions_basket,clients_basket,materials_basket,plants_basket)
    for i in range(len(data_basket)):
        print('material: '+data_basket['material'].iloc[i]+', sum: '+ str(data_basket['sum'].iloc[i].round(2)))

 
 
if __name__ == '__main__':
    main()