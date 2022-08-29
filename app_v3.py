from Bot_V3 import CBot, FILE
from exchange import Deribit_Exchange
# from arbitrage_strategy import check_riskfree_trade, check_riskfree_trade_v2
from risk_free_strategy import collar_strategy, selling_premiums

import yaml
import logging.config
from datetime import date

def main():

    # Подгружаем конфиг
    with open('./config_v3.yaml','r') as f:
        config = yaml.load(f.read(), Loader = yaml.FullLoader)

    # logfile = date.today().strftime('%y-%m-%d') + '_bot_log.csv'
    # config['logging']['handlers']['file']['filename'] = logfile
    
    logging.addLevelName(FILE,"FILE")
    logging.config.dictConfig(config['logging'])
    
    # arbitrage_strat = check_riskfree_trade_v2
    # arbitrage_strat = collar_strategy
    arbitrage_strat = selling_premiums
    
    deribit_exch = Deribit_Exchange(**config['exchange'])
    bot = CBot(**config['bot'], exchange=deribit_exch, arbitrage_strategy=arbitrage_strat, money_mngmt=None)
    bot.run()


if __name__ == '__main__':
    main()