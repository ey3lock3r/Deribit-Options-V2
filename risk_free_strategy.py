import pandas as pd

put_label = ['bid_p', 'ask_p']
call_label = ['bid_c', 'ask_c']
csv_label = ['strike', 'Call', 'Put']
df_initcols = ['strike', 'instrument_name', 'option_type', 'settlement_period']

# def filter_option()

def selling_premiums(put_options, call_options, price):
    # put options 
    df_put = pd.DataFrame(put_options.values())
    df_put.set_index('strike', inplace=True, drop=False)
    df_put = df_put[(df_put['delta'] <= -0.1) & (df_put['delta'] >= -0.2)]
    if df_put.empty:
        print('empty put')
        return df_put

    df_put = df_put.iloc[df_put['delta'].values.argmax()]

    # call options 
    df_call = pd.DataFrame(call_options.values())
    df_call.set_index('strike', inplace=True, drop=False)
    df_call = df_call[(df_call['delta'] > 0.09) & (df_call['delta'] <= 0.2)]
    if df_call.empty:
        print('empty call')
        return df_call

    df_call = df_call.iloc[df_call['delta'].values.argmax()]

    data = [
        ['Put', df_put['instrument_name'], df_put['strike'], price, df_put['bid'], df_put['delta'], df_put['gamma'], df_put['vega'], df_put['rho']],
        ['Call', df_call['instrument_name'], df_call['strike'], price, df_call['bid'], df_call['delta'], df_call['gamma'], df_call['vega'], df_call['rho']],
    ]
    data = pd.DataFrame(data)
    return data

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
