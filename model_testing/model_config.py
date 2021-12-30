

import pytz
import datetime as dt



def get_current_date ():
    est = pytz.timezone('US/Eastern')
    now = dt.datetime.now(tz=est)

    time_parts = str(now).split(" ")
    current_date = time_parts[0]
    return current_date



asset_pred_list = ['SPY', 'QQQ', 'IWM']

#Earliest Useable Date: 2001-03-19

train_start = '2000-01-01'
train_end = '2018-01-01'

test_start = '2020-01-01'
test_end = get_current_date()

