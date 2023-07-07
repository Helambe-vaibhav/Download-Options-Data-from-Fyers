import time

# from functionsList import *
from fyers_api import fyersModel
from fyers_api import accessToken
import warnings
import datetime as dt
import pandas as pd
import os
import numpy as np
warnings.filterwarnings("ignore")
next_coming_thursday = dt.date.today() + dt.timedelta(days=(3- dt.date.today().weekday()+7)%7)
import shutil
import requests
TOKEN ='6382966457:AAG7flHUVlhz-Arqajy8HagOlYa7094xXFw'
id  = '5217068980'


class investing():
    def __init__(self):
        self.client_id='8NIKVBPVPZ-100'
        self.base_access_token = self.get_access_token()
        self.fyers = fyersModel.FyersModel(client_id=self.client_id, token=self.base_access_token ,log_path='logfiles')
        self.download_data(symbol='NSE:NIFTY50-INDEX',Nifty50=True)

    def get_access_token(self):
        # Variables
        secret_key ='1K2F1PLRNU'
        redirect_uri = 'https://www.google.com/'
        response_type = 'code'

        today_date = str(dt.datetime.now().date())
        if not os.path.exists(f'accessToken/access_token{today_date}.txt'):
            for file in os.listdir('accessToken'):
                os.remove(f'accessToken/{file}')
            session= accessToken.SessionModel(client_id=self.client_id,secret_key=secret_key,redirect_uri=redirect_uri, response_type=response_type, grant_type='authorization_code')
            response = session.generate_authcode()
            print('Login Url:',response)
            # send_message('Login Url:'+response)
            auth_url = input('Enter the URL: ')
            auth_code = auth_url.split('auth_code=')[1].split('&state')[0]
            session.set_token(auth_code)
            access_token = session.generate_token()['access_token']
            with open(f'accessToken/access_token{today_date}.txt', 'w') as f:
                f.write(access_token)
        else:
            with open(f'accessToken/access_token{today_date}.txt', 'r') as f:
                access_token = f.read()
        return access_token

    def download_data(self,symbol,Nifty50=False,underlying='NIFTY'):
        next_coming_thursday = dt.date.today() + dt.timedelta(days=(3- dt.date.today().weekday()+7)%7)
        till_today = str(dt.datetime.today().date())
        from_date = str(dt.datetime.today().date()- dt.timedelta(days=40))
        data  = {"symbol":symbol,"resolution":"1","date_format":"1","range_from":from_date,"range_to":till_today}
        df= pd.DataFrame(self.fyers.history(data)['candles'],columns=['Datetime','Open','High','Low','Close','Volume'])
        if df.shape[0]==0:
            print('No Data')
            return
        df['Datetime'] = pd.to_datetime(df['Datetime'],unit='s').dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')
        if Nifty50:
            self.date_range = df['Datetime']
        # if not Nifty50 then the df['Datetime'] is == self.date_range and we can merge the data
        if not Nifty50:
            df = df.merge(self.date_range,how='right',on='Datetime')
            df = df.fillna(method='ffill')
        df['Date'] = df['Datetime'].dt.date
        df['Time'] = df['Datetime'].dt.time
        # convert to 3min data and save
        df = df.set_index('Datetime')
        df = df.resample('3T').agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'})
        df['Date'] = df.index.date
        df['Time'] = df.index.time
        df = df[['Date','Time','Open','High','Low','Close','Volume']]
        df = df.dropna(thresh=4)
        df.to_csv(f'./data/{next_coming_thursday}/{underlying}_{next_coming_thursday}/{symbol[4:]}.csv',index=False)
        print(f'Data Downloaded for {underlying}, {symbol[4:]}')
        df = df[df['Date']>=dt.datetime.today().date()- dt.timedelta(days=7)]
        # drop rows if 3 values are null in a row
        # print(df)
        print('Data Downloaded for',symbol[4:],len(df))

def This_week():
    print(f"{dt.datetime.now()} Next Thursday: {next_coming_thursday} ")
    cur_path =f'datafiles/{next_coming_thursday}'
    if not os.path.exists(cur_path):
        os.mkdir(cur_path)
        fyers_fo = "https://public.fyers.in/sym_details/NSE_FO.csv"
        df_symbols = pd.read_csv(fyers_fo, index_col=False, header=None)
        df_symbols.drop([5,14,17,18],axis=1,inplace=True)
        df_symbols.columns=['Fytoken', 'Symbol Details', 'Exchange Instrument type', 'Minimum lot size', 'Tick size', 'ISIN', 'Last update date', 'Expiry date', 'Symbol ticker', 'Exchange', 'Segment', 'Scrip code', 'Underlying scrip code', 'Strike price', 'Option type']
        df_symbols.reset_index(drop=True, inplace=True)
        df_symbols['Expiry date'] = pd.to_datetime(df_symbols['Expiry date'],unit='s').dt.date
        expiry_list = sorted(df_symbols['Expiry date'].unique())
        for expiry in expiry_list:
            if expiry.weekday() == 3:
                next_expiry = expiry
                break
        print(f"{dt.datetime.now()} Expiry: {next_expiry} ")
        df_symbols = df_symbols[df_symbols['Expiry date']==next_expiry]
        df_symbols = df_symbols.sort_values(by=['Strike price'])
        df_symbols.reset_index(drop=True, inplace=True)
        df_symbols.set_index('Fytoken',inplace=True)
        for fils in ['data']:
            os.mkdir(f'{fils}/{next_coming_thursday}')
            for underlying in df_symbols['Underlying scrip code'].unique():
                os.mkdir(f'{fils}/{next_coming_thursday}/{underlying}_{next_coming_thursday}')
        with open(f'datafiles/{next_coming_thursday}/ExpirySymbols{next_coming_thursday}.csv','w') as f:
            df_symbols.to_csv(f)


This_week()

trading = investing()
main_df = pd.read_csv(f'datafiles/{next_coming_thursday}/ExpirySymbols{next_coming_thursday}.csv')
print(main_df['Underlying scrip code'].unique() )
for i in range(0,len(main_df.index)):
    try:
        trading.download_data(symbol=str(main_df.loc[i,'Symbol ticker']),underlying=main_df.loc[i,'Underlying scrip code'])
    except Exception as e:
        print(f"Error in {main_df.loc[i,'Symbol ticker']} : {e}")
        pass

# create zip and send to telegram
shutil.make_archive(f'data/{next_coming_thursday}', 'zip', f'datafiles/{next_coming_thursday}')

def send_message(message):
    print(message)
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={id}&text={message}"
    requests.get(url)

def send_document(message):
    with open(message, 'rb') as f:
        url = f"https://api.telegram.org/bot{TOKEN}/sendDocument?chat_id={id}"
        requests.post(url, files={'document': f})

send_document(f'datafiles/{next_coming_thursday}.zip')
send_message(f"Data Downloaded for {next_coming_thursday}")




