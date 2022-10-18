# Loading an Environment Variable File with dotenv
from dotenv import load_dotenv
load_dotenv()
import os

from Bot_V3 import CBot, FILE
from exchange import Deribit_Exchange
# from arbitrage_strategy import check_riskfree_trade, check_riskfree_trade_v2
from risk_free_strategy import collar_strategy, selling_premiums, sell_008_premium_2k_dist, test

import yaml
import logging.config
from datetime import date

def main():

    # Подгружаем конфиг
    with open('./config_v3.yaml','r') as f:
        config = yaml.load(f.read(), Loader = yaml.FullLoader)

    if config['exchange']['env'] == 'prod':
        config['exchange']['auth']['prod']['client_id'] = os.getenv('client_id')
        config['exchange']['auth']['prod']['client_secret'] = os.getenv('client_secret')
    
    logging.addLevelName(FILE,"FILE")
    logging.config.dictConfig(config['logging'])
    
    # option_strats = {
    #     'test': selling_premiums,
    #     'trading': sell_008_premium_2k_dist
    # }
    option_strats = {
        'test': selling_premiums,
        'trading': test
    }
    
    deribit_exch = Deribit_Exchange(**config['exchange'])
    bot = CBot(**config['bot'], exchange=deribit_exch, run_strategy=option_strats, money_mngmt=None)
    bot.run()


if __name__ == '__main__':
    main()