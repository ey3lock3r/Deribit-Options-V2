# https://github.com/n-eliseev/deribitsimplebot/blob/master/deribitsimplebot/bot.py

import time
import asyncio
import concurrent.futures
import logging
import pandas as pd
# import traceback

from typing import Union, Optional, NoReturn

FILE = 60

class CBot:
    """The class describes the object of a simple bot that works with the Deribit exchange.
    Launch via the run method or asynchronously via start.
    The business logic of the bot itself is described in the worker method."""

    def __init__(self, exchange, money_mngmt, arbitrage_strategy, interval: int = 2, 
        logger: Union[logging.Logger, str, None] = None):

        self.interval = interval
        self.exchange = exchange
        self.money_mngmt = money_mngmt
        self.arbitrage_strategy = arbitrage_strategy

        self.call_options = {}
        self.put_options = {}

        self.logger = (logging.getLogger(logger) if isinstance(logger,str) else logger)
        if self.logger is None:
            self.logger = logging.getLogger(__name__)

        self.stop = False

        # self.df_initcols = ['strike', 'instrument_name', 'option_type', 'settlement_period']

    # def execute_trade(self):
    #     self.calculate_imargin()
    #     self.calculate_mmargin()

    def check_riskfree_trade(self):

        # Set CSV Header
        # csv_label = ['strike', 'Call', 'Put']
        # self.logger.log(FILE, ",".join(csv_label + ['Side', 'PnL', 'Price']))

        # Set CSV Header
        # csv_label = ['Direction', 'Sell Premium', 'Buy Premium']
        # self.logger.log(FILE, ",".join(csv_label + ['Premium Payout', 'Max Profit', 'Max Loss', 'Risk Reward', 'Kelly']))

        # Set CSV Header
        csv_label = ['Option', 'Strike', 'Price', 'Sell Premium']
        self.logger.log(FILE, ",".join(csv_label + ['Delta', 'Gamma', 'Vega', 'Rho']))

        while self.exchange.keep_alive:
            self.logger.info('Checking for risk free trade...')

            if self.exchange.updated:
                price = self.exchange.asset_price

                df_arbi = self.arbitrage_strategy(self.put_options, self.call_options, price)

                if not df_arbi.empty:
                    self.logger.info(f'Price index: {price}')
                    self.logger.log(FILE, ",".join(df_arbi.iloc[0].values.astype(str)))
                    self.logger.log(FILE, ",".join(df_arbi.iloc[1].values.astype(str)))

                    # min = df_arbi['Cost'].values.argmin()
                    # self.logger.log(FILE, ",".join(df_arbi.iloc[min].values.astype(str)))

                self.exchange.updated = False
                time.sleep(self.interval)
            
            else:
                self.logger.info('Prices not updated')
                time.sleep(self.interval * 0.3)

        self.logger.info('check_riskfree_trade ended!')


    async def start(self):
        """Starts the bot with the parameters for synchronization.
        Synchronization will be carried out only if the store (store) is specified """

        self.logger.info('Bot start')

        tasks = []

        tasks.append(asyncio.to_thread(self.check_riskfree_trade))
        tasks.append(asyncio.ensure_future(self.end_of_day()))
        tasks.append(asyncio.ensure_future(self.exchange.fetch_deribit_price_index()))
        self.call_options, self.put_options = await self.exchange.prepare_option_struct()

        # if not self.call_options or not self.put_options:
        #     return

        # def update_options_dict(options_dict, strike: str, new_data) -> NoReturn:
        #     options_dict[strike].update(new_data)

        for key, val in self.call_options.items():
            tasks.append(
                asyncio.ensure_future(
                    self.exchange.fetch_orderbook_data(key, val['instrument_name'], self.call_options)
                )
            )
        
        for key, val in self.put_options.items():
            tasks.append(
                asyncio.ensure_future(
                    self.exchange.fetch_orderbook_data(key, val['instrument_name'], self.put_options)
                )
            )

        self.logger.info(f'Number of tasks: {len(tasks)}')

        for task in tasks:
            # await asyncio.gather(task)
            await task
            time.sleep(0.5)

        self.logger.info(f'Tasks created: {len(tasks)}')
        self.logger.info('start > end !')

        # asyncio.gather(asyncio.to_thread(self.check_riskfree_trade))

        # with concurrent.futures.ThreadPoolExecutor() as executor:
        # #     [executor.submit(task) for task in tasks]

        #     for key, val in self.call_options.items():
        #         executor.submit(self.fetch_orderbook_data, key, val['instrument_name'], val['option_type'])
        #         # tasks.append([self.fetch_orderbook_data, key, val['instrument_name'], val['option_type']])
            
        #     for key, val in self.put_options.items():
        #         executor.submit(self.fetch_orderbook_data, key, val['instrument_name'], val['option_type'])
        #         # tasks.append([self.fetch_orderbook_data, key, val['instrument_name'], val['option_type']])

        #     # executor.submit(self.check_riskfree_trade)

        # # loop = asyncio.get_running_loop()
        # # await loop.run_in_executor(None, self.check_riskfree_trade)
        # # executor.submit(self.check_riskfree_trade)

    async def end_of_day(self):
        # await asyncio.sleep( 86400 - time.time() % 86400 + 60 )
        await asyncio.sleep( 180 - time.time() % 180 )
        self.exchange.keep_alive = False
        await asyncio.sleep( 5 )
        self.logger.info('End of day!')

            
    def run(self) -> NoReturn:
        """Wrapper for start to run without additional libraries for managing asynchronous"""

        loop = asyncio.get_event_loop()

        try:
            loop.run_until_complete(self.start())
        
        except KeyboardInterrupt:
            self.stop = True
            bot.logger.info('Keyboard Interrupt detected...')

        except Exception as E:
            self.logger.info(f'Error in run: {E}')
            self.logger.info(traceback.print_exc())
            self.exchange.keep_alive = False
            self.stop = True

        finally:
            self.exchange.keep_alive = False
            loop.run_until_complete(self.exchange.grace_exit())
            self.logger.info('Gracefully exit')