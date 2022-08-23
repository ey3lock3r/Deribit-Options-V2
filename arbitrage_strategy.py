import pandas as pd

put_label = ['bid_p', 'ask_p']
call_label = ['bid_c', 'ask_c']
csv_label = ['strike', 'buyOpt', 'sellOpt']
df_initcols = ['strike', 'instrument_name', 'option_type', 'settlement_period']

def check_riskfree_trade(put_options, call_options, price):

    df_put_data = pd.DataFrame(put_options.values())
    df_put_data.set_index('strike', inplace=True, drop=False)
    df_put_data.columns = df_initcols + ['date'] + put_label

    # Convert bids and asks to $
    put_bid_ask = df_put_data[put_label]
    put_bid_ask = put_bid_ask * price

    df_call_data = pd.DataFrame(call_options.values())
    df_call_data.set_index('strike', inplace=True, drop=False)
    df_call_data.columns = df_initcols + ['date'] + call_label

    call_bid_ask = df_call_data[call_label]
    call_bid_ask = call_bid_ask * price

    df_data = pd.concat([df_put_data[['strike']], put_bid_ask, call_bid_ask], axis=1)

    df_arbi_buy_c = pd.DataFrame(df_data[['strike', 'ask_c', 'bid_p']].values)
    df_arbi_buy_c.columns = csv_label
    df_arbi_buy_c['Side'] = 'Buy Call'
    # df_arbi_buy_c['PnL'] = price - df_arbi_buy_c['buyOpt'].values + df_arbi_buy_c['sellOpt'].values - df_arbi_buy_c['strike'].values

    df_arbi_buy_p = pd.DataFrame(df_data[['strike', 'ask_p', 'bid_c']].values) # <<<< todo recomm
    # df_arbi_buy_p = pd.DataFrame(df_data[['strike', 'bid_c', 'ask_p']].values)
    df_arbi_buy_p.columns = csv_label
    df_arbi_buy_p['Side'] = 'Buy Put'
    # df_arbi_buy_p['PnL'] = df_arbi_buy_p['strike'].values - df_arbi_buy_p['buyOpt'].values + df_arbi_buy_p['sellOpt'].values - price

    df_arbi = pd.concat([df_arbi_buy_c, df_arbi_buy_p])

    # additional check buy < sell
    df_arbi = df_arbi[df_arbi['buyOpt'] < df_arbi['sellOpt']]  # <<<< todo enable

    # f - c + p - k = 0
    df_arbi['PnL'] = price - df_arbi['buyOpt'].values + df_arbi['sellOpt'].values - df_arbi['strike'].values
    df_arbi['Price'] = price
    df_arbi = df_arbi[df_arbi['PnL'] > 0]
    
    return df_arbi


csv_label = ['strike', 'Call', 'Put']
def check_riskfree_trade_v2(put_options, call_options, price):

    df_put_data = pd.DataFrame(put_options.values())
    df_put_data.set_index('strike', inplace=True, drop=False)
    df_put_data.columns = df_initcols + ['date'] + put_label

    # Convert bids and asks to $
    put_bid_ask = df_put_data[put_label]
    put_bid_ask = put_bid_ask * price

    df_call_data = pd.DataFrame(call_options.values())
    df_call_data.set_index('strike', inplace=True, drop=False)
    df_call_data.columns = df_initcols + ['date'] + call_label

    call_bid_ask = df_call_data[call_label]
    call_bid_ask = call_bid_ask * price

    df_data = pd.concat([df_put_data[['strike']], put_bid_ask, call_bid_ask], axis=1)

    df_arbi_buy_c = pd.DataFrame(df_data[['strike', 'ask_c', 'bid_p']].values)
    df_arbi_buy_c.columns = csv_label
    df_arbi_buy_c['Side'] = 'Buy Call'
    df_arbi_buy_c['Cost'] = df_arbi_buy_c['strike'].values + df_arbi_buy_c['Call'].values - price - df_arbi_buy_c['Put'].values

    df_arbi_buy_p = pd.DataFrame(df_data[['strike', 'bid_c', 'ask_p']].values) 
    df_arbi_buy_p.columns = csv_label
    df_arbi_buy_p['Side'] = 'Buy Put'
    df_arbi_buy_p['Cost'] = price + df_arbi_buy_p['Put'].values - df_arbi_buy_p['strike'].values - df_arbi_buy_p['Call'].values

    df_arbi = pd.concat([df_arbi_buy_c, df_arbi_buy_p])

    df_arbi['Price'] = price
    df_arbi = df_arbi[df_arbi['Cost'] < 0]
    
    return df_arbi

