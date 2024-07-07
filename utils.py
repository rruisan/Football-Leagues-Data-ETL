import pandas as pd
import time
from datetime import datetime
import uuid

# Fetches and processes league data from a given URL
def get_data(url, league):

    time.sleep(1)
    
    # Read HTML tables from the URL and concatenate the first two tables
    df_web = pd.read_html(url)
    df_web_full = pd.concat([df_web[0],df_web[1]], ignore_index=True, axis=1)
    
    # Rename columns and clean team names
    df_web_full = df_web_full.rename(columns={0: 'EQUIPO', 1: 'J', 2:'G', 3:'E', 4:'P', 5:'GF', 6:'GC', 7:'DIF', 8:'PTS'})
    df_web_full['EQUIPO'] = df_web_full['EQUIPO'].apply(lambda x: x[5:] if x[:2].isnumeric()== True else x[4:])
    df_web_full['LIGA'] = league
    
    date = datetime.now().strftime("%d-%m-%Y")
    df_web_full['CREATED_AT'] = date
        
    return df_web_full


# Processes data for multiple leagues and concatenates the results
def data_processing(df):
    df_spain = get_data(df['URL'][0], df['LIGA'][0])
    df_premier= get_data(df['URL'][1], df['LIGA'][1])
    df_italy = get_data(df['URL'][2], df['LIGA'][2])
    df_germany = get_data(df['URL'][3], df['LIGA'][3])
    df_france = get_data(df['URL'][4], df['LIGA'][4])
    df_portugal = get_data(df['URL'][5], df['LIGA'][5])
    df_nederland = get_data(df['URL'][6], df['LIGA'][6])
    
    df_final = pd.concat([df_spain, df_premier, df_italy, df_germany, df_france, df_portugal, df_nederland], ignore_index=False)
    
    return df_final
