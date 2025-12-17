import os
import pandas as pd

# Unified columns including optional derivatives support
columns = [
    'DhanOrderType','Symbol','Exchange','TransactionType','Quantity','OrderType','ProductType',
    'Price','TargetPrice','StopLoss','TrailingStopLoss','TriggerPrice','OrderFlag','Validity',
    'DisclosedQuantity','Price1','TriggerPrice1','Quantity1','Tag',
    'StrikePrice','ExpiryDate','OptionType'
]

# Sample rows: SUPER, FOREVER, and derivatives (NIFTY & SENSEX via BSXOPT)
rows = [
    # Super order example (equity)
    ['SUPER','SBIN','NSE','BUY',10,'LIMIT','INTRADAY',610,650,590,0,'','','','', '', '', '', '', '', '', ''],
    # Forever SINGLE example (equity)
    ['FOREVER','SBIN','NSE','BUY',5,'LIMIT','CNC',1428,'','','',1427,'SINGLE','DAY',1,'','','','my_strategy','', '', ''],
    # Forever OCO example (equity)
    ['FOREVER','RELIANCE','NSE','BUY',5,'LIMIT','CNC',2428,'','','',2427,'OCO','DAY',1,2420,2419,10,'my_strategy_oco','', '', ''],
    # NIFTY option (derivative) - SUPER flow with option fields filled
    ['SUPER','NIFTY','NFO','BUY',50,'LIMIT','INTRADAY',100.5,120.0,80.0,0,'','','','', '', '', '', '', 24000,'2025-12-18','CE'],
    # SENSEX option via BSXOPT (derivative) - FOREVER flow with option fields filled
    ['FOREVER','BSXOPT','BFO','BUY',10,'LIMIT','CNC',250.0,'','','',245.0,'SINGLE','DAY',0,'','','','sensex_forever',50000,'2025-12-18','PE'],
]

df = pd.DataFrame(rows, columns=columns)

os.makedirs('static', exist_ok=True)

xlsx_path = os.path.join('static', 'sample_orders.xlsx')
print('Writing Excel to:', xlsx_path)
df.to_excel(xlsx_path, index=False, sheet_name='Orders')
print('Done')
