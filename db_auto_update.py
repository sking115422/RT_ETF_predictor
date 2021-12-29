
import private_info.db_info as dbi
import mysql.connector
import pandas_datareader as web
import pandas as pd 
from sklearn.preprocessing import MinMaxScaler
import requests as req
import datetime as dt
import pytz 
import csv


class FedFundsError(Exception):
    """Raised when there is an issue with the fed funds db table auto update"""
    pass


# CONNECTING TO DB


def connect_to_db ():
    db = mysql.connector.connect(
        host = dbi.hostname,  
        user = dbi.username,    
        password = dbi.password,
        database = dbi.database,
        auth_plugin='mysql_native_password'
    )

    print ()
    print (db)

    mycursor = db.cursor()
    
    return db, mycursor


db_con, cur = connect_to_db ()


# GETTING CURRENT DATE


est = pytz.timezone('US/Eastern')
now = dt.datetime.now(tz=est)

time_parts = str(now).split(" ")
current_date = time_parts[0]
print(current_date)


start_date = current_date
end_date = current_date


# GETTING ALL DB TABLE EXCEPT FED FUNDS IN LIST


cur.execute('USE etf_pred;')
cur.execute('SHOW tables;')
res = cur.fetchall()


name_list = []
for each in res:
    if each[0] != 'fed_funds':
         name_list.append(each[0])


for i in range (0, len(name_list)):
    if name_list[i] == 'fvx':
        name_list[i] = '^fvx'
    if name_list[i] == 'tnx':
        name_list[i] = '^tnx'
    if name_list[i] == 'vix':
        name_list[i] = '^vix'


print(name_list)


# GETTING TODAYS INFORMATION FOR EACH TABLE


asset_df_list = []

for each in name_list:
    
    asset_df_list.append( web.DataReader(each, data_source='yahoo', start = start_date, end = end_date) )


for i in range (0, len(name_list)):
    if name_list[i] == '^fvx':
        name_list[i] = 'fvx'
    if name_list[i] == '^tnx':
        name_list[i] = 'tnx'
    if name_list[i] == '^vix':
        name_list[i] = 'vix'


print(name_list)


# ADDING THIS NEW INFROMATION AS A NEW ROW IN EACH TABLE


inc = 0
for each in asset_df_list:
    each['Date'] = each.index
    each.reset_index(drop=True, inplace=True)
    each = each[['Date', 'High', 'Low', 'Open', 'Close', 'Volume', 'Adj Close']]
    if each.shape[0] > 1:
        each.drop(each.tail(1).index,inplace=True)
        
    date_dt = each.iloc[0]['Date']
    
    cur.execute(f"SELECT Date FROM {name_list[inc]} ORDER BY Date DESC LIMIT 1;")
    prev_date = cur.fetchall()[0][0]
    prev_date_dt = dt.datetime.strptime(prev_date, '%Y-%m-%d')
    
    
    if date_dt > prev_date_dt:
    
        date = str(date_dt)[0:10]
        print(date)
        high = each.iloc[0]['High']
        low = each.iloc[0]['Low']
        open = each.iloc[0]['Open']
        close = each.iloc[0]['Close']
        volume = each.iloc[0]['Volume']
        adj_close = each.iloc[0]['Adj Close']
        
        cmd1 = f"INSERT INTO {name_list[inc]} (`Date`, `High`, `Low`, `Open`, `Close`, `Volume`, `Adj Close`) "
        cmd2 = f"VALUES ('{date}', {high}, {low}, {open}, {close}, {volume}, {adj_close})"
        cmd = cmd1 + cmd2
        
        print (cmd)
        
        cur.execute(cmd)
        db_con.commit()
    
    inc = inc + 1


fed_fund_url = f"https://markets.newyorkfed.org/read?productCode=50&eventCodes=500&limit=5&startPosition=0&sort=postDt:-1&format=csv"


fed_funds_data = req.get(fed_fund_url)
decoded_content = fed_funds_data.content.decode('utf-8')

cr = csv.reader(decoded_content.splitlines(), delimiter=',')

ff_df = pd.DataFrame(cr)

ff_df.drop(ff_df.columns[1], axis=1, inplace=True)
ff_df.drop(ff_df.columns[2:18], axis=1, inplace=True)
ff_df.drop(index=ff_df.index[0], axis=0, inplace=True)
ff_df.reset_index(drop=True, inplace=True)
ff_df.rename(columns={0: 'Date', 2: 'DFF'}, inplace=True)

ff_df.head()


ff_new_date = ff_df['Date'].iloc[0]
ff_new_date = dt.datetime.strptime(ff_new_date, '%m/%d/%Y')
ff_new_date = dt.datetime.strftime(ff_new_date, '%Y-%m-%d')


print (ff_new_date)


ff_new_val = ff_df['DFF'].loc[0]


cur.execute(f"SELECT * FROM fed_funds ORDER BY Date DESC LIMIT 2;")
ff_db_list = cur.fetchall()


cur.execute(f"SELECT * FROM tnx ORDER BY Date DESC LIMIT 1;")
tnx_db_list = cur.fetchall()


tnx_date = tnx_db_list[0][1]


print(tnx_date)


ff_db_lr_index = ff_db_list[0][0]
ff_db_lr_date = ff_db_list[0][1]
ff_db_lr_val = ff_db_list[0][2]


if ff_db_lr_date == tnx_date and ff_db_lr_val == None:
    pass
else:
    if tnx_date == ff_new_date:
        cur.execute(f"UPDATE fed_funds SET Date = \'{ff_new_date}\' WHERE `index` = {ff_db_lr_index}")
        cur.execute(f"UPDATE fed_funds SET DFF = \'{ff_new_val}\' WHERE `index` = {ff_db_lr_index}")
        cur.execute(f"INSERT INTO fed_funds (`Date`, `DFF`) VALUES (\'{current_date}\', NULL);")
        db_con.commit()
    else:
        raise FedFundsError


