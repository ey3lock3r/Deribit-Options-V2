import pandas as pd
import numpy as np

put_label = ['bid_p', 'ask_p']
call_label = ['bid_c', 'ask_c']
csv_label = ['strike', 'Call', 'Put']
df_initcols = ['strike', 'instrument_name', 'option_type', 'settlement_period']

# def filter_option()

def selling_premiums(put_options, call_options, price):
    data = []

    # 1500 distance
    l_price = price - price % 500
    h_price = l_price + 500
    l_strike = 0
    h_strike = 0

    if price > l_price + 250:
        l_strike = h_price - 1000
        h_strike = h_price + 1000

    else:
        l_strike = float(l_price - 1000)
        h_strike = float(l_price + 1000)

    if l_strike not in put_options:
        l_strike -= 500
    if h_strike not in call_options:
        h_strike += 500

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

    data.append(p_data + c_data + ['1.5k Dist Strategy'])

    # create put/call dataframes and check if empty
    df_put = pd.DataFrame(put_options.values())
    df_put.set_index('strike', inplace=True, drop=False)
    df_put = df_put[(df_put['delta'] <= -0.1) & (df_put['delta'] >= -0.2)]
    if df_put.empty:
        print('empty put - 10-20% max strategy')
        return np.array(data, dtype=str)

    df_call = pd.DataFrame(call_options.values())
    df_call.set_index('strike', inplace=True, drop=False)
    df_call = df_call[(df_call['delta'] >= 0.1) & (df_call['delta'] <= 0.2)]
    if df_call.empty:
        print('empty call - 10-20% max strategy')
        return np.array(data, dtype=str)

    # 10-20% max delta strategy
    
    pmax = df_put['delta'].values.argmax()
    df_put_bk = df_put.drop(df_put.iloc[pmax]['strike'])
    df_put = df_put.iloc[pmax]

    cmax = df_call['delta'].values.argmax()
    df_call_bk = df_call.drop(df_call.iloc[cmax]['strike'])
    df_call = df_call.iloc[cmax]

    p_data = [price, df_put['instrument_name'], df_put['strike'], df_put['bid'], df_put['delta'], df_put['gamma'], df_put['vega'], df_put['rho']]
    c_data = [df_call['instrument_name'], df_call['strike'], df_call['bid'], df_call['delta'], df_call['gamma'], df_call['vega'], df_call['rho']]

    # data = pd.DataFrame(data)
    data.append(p_data + c_data + ['10-20% Delta Strategy'])

    # 2nd max delta strategy
    df_put = df_put_bk
    df_call = df_call_bk
    if df_put.empty:
        print('empty put - 2nd max strategy')
        return np.array(data, dtype=str)
    if df_call.empty:
        print('empty call - 2nd max strategy')
        return np.array(data, dtype=str)

    df_put = df_put.iloc[df_put['delta'].values.argmax()]
    df_call = df_call.iloc[df_call['delta'].values.argmax()]

    p_data = [price, df_put['instrument_name'], df_put['strike'], df_put['bid'], df_put['delta'], df_put['gamma'], df_put['vega'], df_put['rho']]
    c_data = [df_call['instrument_name'], df_call['strike'], df_call['bid'], df_call['delta'], df_call['gamma'], df_call['vega'], df_call['rho']]

    data.append(p_data + c_data + ['2nd Max Delta Strategy'])

    return np.array(data, dtype=str)

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
