import pandas as pd
import numpy as np

put_label = ['bid_p', 'ask_p']
call_label = ['bid_c', 'ask_c']
csv_label = ['strike', 'Call', 'Put']
df_initcols = ['strike', 'instrument_name', 'option_type', 'settlement_period']

def dist_1500(data, put_options, call_options, price):

    # 1500 distance
    l_price = price - price % 250
    h_price = l_price + 250
    l_strike = 0
    h_strike = 0

    if price > l_price + 125:
        l_strike = h_price - 1000.0
        h_strike = h_price + 1000.0

    else:
        l_strike = l_price - 1000.0
        h_strike = l_price + 1000.0

    while l_strike not in put_options:
        l_strike -= 250
    while h_strike not in call_options:
        h_strike += 250

    # if put_options[l_strike]['bid'] + call_options[h_strike]['bid'] >= 0.008:
    p_data = [price, put_options[l_strike]['instrument_name'], 
        put_options[l_strike]['strike'], 
        put_options[l_strike]['bid'], 
        put_options[l_strike]['delta'], 
        put_options[l_strike]['gamma'], 
        put_options[l_strike]['vega'], 
        put_options[l_strike]['rho']]
    c_data = [call_options[h_strike]['instrument_name'], 
        call_options[h_strike]['strike'], 
        call_options[h_strike]['bid'], 
        call_options[h_strike]['delta'], 
        call_options[h_strike]['gamma'], 
        call_options[h_strike]['vega'], 
        call_options[h_strike]['rho']]

    if not np.isnan(put_options[l_strike]['bid']) and not np.isnan(call_options[h_strike]['bid']):
        data.append(p_data + c_data + ['1.5k Dist Strategy'])

    return data

def delta_10_20(data, put_options, call_options, price):

    activated = False
    # create put/call dataframes and check if empty
    df_put_bk = pd.DataFrame(put_options.values())
    df_put_bk.set_index('strike', inplace=True, drop=False)
    df_put = df_put_bk[(df_put_bk['delta'] <= -0.1) & (df_put_bk['delta'] >= -0.2)]

    df_call_bk = pd.DataFrame(call_options.values())
    df_call_bk.set_index('strike', inplace=True, drop=False)
    df_call = df_call_bk[(df_call_bk['delta'] >= 0.1) & (df_call_bk['delta'] <= 0.2)]

    if not df_put.empty and not df_call.empty:
        pmin = df_put['delta'].values.argmin()
        cmax = df_call['delta'].values.argmax()

        df_put = df_put.iloc[pmin]
        df_call = df_call.iloc[cmax]

        # if df_put['bid'] + df_call['bid'] >= 0.008:
        p_data = [price, df_put['instrument_name'], df_put['strike'], df_put['bid'], df_put['delta'], df_put['gamma'], df_put['vega'], df_put['rho']]
        c_data = [df_call['instrument_name'], df_call['strike'], df_call['bid'], df_call['delta'], df_call['gamma'], df_call['vega'], df_call['rho']]

        data.append(p_data + c_data + ['10-20% Delta Strategy'])
        activated = True

    return activated, data

def delta_2nd_max(data, put_options, call_options, price):

    df_put_bk = pd.DataFrame(put_options.values())
    df_put_bk.set_index('strike', inplace=True, drop=False)

    df_call_bk = pd.DataFrame(call_options.values())
    df_call_bk.set_index('strike', inplace=True, drop=False)

    df_put = df_put_bk[df_put_bk['delta'] >= -0.2]
    df_call = df_call_bk[df_call_bk['delta'] <= 0.2]

    if not df_put.empty and not df_call.empty:
        pmin = df_put['delta'].values.argmin()
        cmax = df_call['delta'].values.argmax()

        if df_put.iloc[pmin]['bid'] > df_call.iloc[cmax]['bid']:
            df_put =  df_put.drop(df_put.iloc[pmin]['strike'])
        else:
            df_call = df_call.drop(df_call.iloc[cmax]['strike'])
            
        if not df_put.empty and not df_call.empty:
            pmin = df_put['delta'].values.argmin()
            cmax = df_call['delta'].values.argmax()

        df_put = df_put.iloc[pmin]
        df_call = df_call.iloc[cmax]

        # if df_put['bid'] + df_call['bid'] >= 0.008:
        if not np.isnan(df_put['bid']) and not np.isnan(df_call['bid']):
            p_data = [price, df_put['instrument_name'], df_put['strike'], df_put['bid'], df_put['delta'], df_put['gamma'], df_put['vega'], df_put['rho']]
            c_data = [df_call['instrument_name'], df_call['strike'], df_call['bid'], df_call['delta'], df_call['gamma'], df_call['vega'], df_call['rho']]

            data.append(p_data + c_data + ['2nd Max Delta Strategy'])

    return data

def selling_premiums(put_options, call_options, price):
    data = []

    data = dist_1500(data, put_options, call_options, price)
    active, data = delta_10_20(data, put_options, call_options, price)
    
    if active:
        data = delta_2nd_max(data, put_options, call_options, price)

    return np.array(data, dtype=str)

def sell_008_premium_2k_dist(put_options, call_options, price, min_prem, strike_dist):
    data = []
    sum_premium = 0

    # create put/call dataframes and check if empty
    df_put_bk = pd.DataFrame(put_options.values())
    df_put_bk.set_index('strike', inplace=True, drop=False)
    df_put = df_put_bk[(df_put_bk['delta'] <= -0.1) & (df_put_bk['delta'] >= -0.2)]

    df_call_bk = pd.DataFrame(call_options.values())
    df_call_bk.set_index('strike', inplace=True, drop=False)
    df_call = df_call_bk[(df_call_bk['delta'] >= 0.1) & (df_call_bk['delta'] <= 0.2)]

    if not df_put.empty and not df_call.empty:
        pmin = df_put['delta'].values.argmin()
        cmax = df_call['delta'].values.argmax()

        df_put = df_put.iloc[pmin]
        df_call = df_call.iloc[cmax]

        sum_premium = df_put['bid'] + df_call['bid']
        strk_dist = abs(df_call['strike'] - df_put['strike'])
        if sum_premium >= min_prem and strk_dist >= strike_dist:
            data.append({
                'instrument': put_options[float(df_put['strike'])],
                'bid': df_put['bid'],
                'sum_prem': sum_premium,
                'strike_dist': strk_dist
            })
            data.append({
                'instrument': call_options[float(df_call['strike'])],
                'bid': df_call['bid'],
                'sum_prem': sum_premium,
                'strike_dist': strk_dist
            })

    return data

def test(put_options, call_options, price):
    data = []
    sum_premium = 0

    # create put/call dataframes and check if empty
    df_put_bk = pd.DataFrame(put_options.values())
    df_put_bk.set_index('strike', inplace=True, drop=False)
    df_put = df_put_bk[df_put_bk['delta'] >= -0.2]

    df_call_bk = pd.DataFrame(call_options.values())
    df_call_bk.set_index('strike', inplace=True, drop=False)
    df_call = df_call_bk[df_call_bk['delta'] <= 0.2]

    if not df_put.empty and not df_call.empty:
        pmin = df_put['delta'].values.argmin()
        cmax = df_call['delta'].values.argmax()

        df_put = df_put.iloc[pmin]
        df_call = df_call.iloc[cmax]

        sum_premium = df_put['bid'] + df_call['bid']
        # if sum_premium >= 0.008 and abs(df_call['strike'] - df_put['strike']) >= 2000:
        data.append({
            'instrument': put_options[float(df_put['strike'])],
            'bid': df_put['bid']
        })
        data.append({
            'instrument': call_options[float(df_call['strike'])],
            'bid': df_call['bid']
        })

    return (data, str(sum_premium))

def collar_strategy(put_options, call_options, price):
    styk_interval = 500
    prob = 0.5

    l_strike = float(price - price % styk_interval)
    h_strike = float(l_strike) + styk_interval

    data = [['bullish', call_options[h_strike]['bid'], put_options[l_strike]['ask']],
            ['bearish', put_options[l_strike]['bid'], call_options[h_strike]['ask']]]
    df = pd.DataFrame(data, columns=['Direction', 'Sell Premium', 'Buy Premium'])

    df['Premium Payout'] = df['Sell Premium'] - df['Buy Premium']
    df['Premium Payout'] *= price                                       # convert to price
    df['Max Profit'] = df['Premium Payout'] - abs(h_strike - price)
    df['Max Loss']   = df['Premium Payout'] - abs(l_strike - price)

    df = df[df['Max Profit'] > 0]
    df['Risk Reward'] = df['Max Profit'] / abs(df['Max Loss'])
    df['Kelly'] = (prob * df['Risk Reward'] + prob - 1) / df['Risk Reward']

    # Bullish
    # prem_payout = call_options[h_strike]['bid'] - put_options[l_strike]['ask']
    # max_profit  = prem_payout - abs(h_strike - price)
    # max_loss    = prem_payout - abs(l_strike - price)
    # risk_reward = max_profit / abs(max_loss)

    df = df[df['Kelly'] > 0]
    if not df.empty:
        maxr = df['Kelly'].values.argmax()
        return df.iloc[maxr]
    
    return df
