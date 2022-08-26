from Bot_V3 import CBot, FILE
from exchange import Deribit_Exchange
# from arbitrage_strategy import check_riskfree_trade, check_riskfree_trade_v2
from risk_free_strategy import collar_strategy, selling_premiums

import asyncio
import yaml
import logging.config
import traceback
from datetime import date

def main():

    # Подгружаем конфиг
    with open('./config_v3.yaml','r') as f:
        config = yaml.load(f.read(), Loader = yaml.FullLoader)

    logging.addLevelName(FILE,"FILE")
    
    # arbitrage_strat = check_riskfree_trade_v2
    # arbitrage_strat = collar_strategy
    arbitrage_strat = selling_premiums

    while True:
        config['logging']['handlers']['file']['filename'] = date.today().strftime('%y-%m-%d') + '_bot_log.log'
        logging.config.dictConfig(config['logging'])

        deribit_exch = Deribit_Exchange(**config['exchange'])
        bot = CBot(**config['bot'], exchange=deribit_exch, arbitrage_strategy=arbitrage_strat, money_mngmt=None)
        
        try:
            bot.run()

        except KeyboardInterrupt:
            bot.logger.info('Keyboard Interrupt detected...')

        except Exception as E:
            bot.logger.info(f'Error!: {E}')
            bot.logger.info(traceback.print_exc())
            bot.exchange.keep_alive = False

        finally:
            bot.exchange.keep_alive = False
            loop = asyncio.get_event_loop()
            loop.run_until_complete(bot.exchange.grace_exit())
            bot.logger.info('Gracefully exit')


if __name__ == '__main__':
    main()