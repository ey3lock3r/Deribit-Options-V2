# https://github.com/n-eliseev/deribitsimplebot/blob/master/deribitsimplebot/bot.py

import time
import asyncio
import concurrent.futures
import logging
import pandas as pd
import traceback

from datetime import date, datetime
from typing import Union, Optional, NoReturn
from exceptions import CBotError

FILE = 60

class CBot:
    """The class describes the object of a simple bot that works with the Deribit exchange.
    Launch via the run method or asynchronously via start.
    The business logic of the bot itself is described in the worker method."""

    def __init__(self, exchange, money_mngmt, run_strategy, interval: int = 2, 
        logger: Union[logging.Logger, str, None] = None):

        self.interval = interval
        self.exchange = exchange
        self.money_mngmt = money_mngmt

        self.test_strategy = run_strategy['test']
        self.trade_strategy = run_strategy['trading']

        self.logger = (logging.getLogger(logger) if isinstance(logger,str) else logger)
        if self.logger is None:
            self.logger = logging.getLogger(__name__)

        self.init_vals()
        
        self.logger.info('Bot initialized!')

        # self.df_initcols = ['strike', 'instrument_name', 'option_type', 'settlement_period']

    # def execute_trade(self):
    #     self.calculate_imargin()
    #     self.calculate_mmargin()

    def init_vals(self):
        self.stop = False
        self.count_to_reset = 0
        # self.tasks = []

        # if not first run, rename logfile
        # logfile = date.today().strftime('%y-%m-%d_%H_%M') + '_bot_log.csv'
        # self.logconf['handlers']['file']['filename'] = logfile
        # logging.config.dictConfig(self.logconf)
        # self.logger = logging.getLogger(__name__)

        self.exchange.init_vals()


    async def check_riskfree_trade(self):

        # Set CSV Header
        # csv_label = ['strike', 'Call', 'Put']
        # self.logger.log(FILE, ",".join(csv_label + ['Side', 'PnL', 'Price']))

        # Set CSV Header
        # csv_label = ['Direction', 'Sell Premium', 'Buy Premium']
        # self.logger.log(FILE, ",".join(csv_label + ['Premium Payout', 'Max Profit', 'Max Loss', 'Risk Reward', 'Kelly']))

        # Set CSV Header
        put_label = ['Price', 'instrument_name', 'P_Strike', 'P_Premium', 'P_Delta', 'P_Gamma', 'P_Vega', 'P_Rho']
        call_label = ['instrument_name', 'C_Strike', 'C_Premium', 'C_Delta', 'C_Gamma', 'C_Vega', 'C_Rho']
        self.logger.log(FILE, ",".join(put_label + call_label))

        sum_premium = 0
        while self.exchange.keep_alive:
            self.logger.info('Checking for risk free trade...')

            if self.exchange.updated:
                price = self.exchange.asset_price

                # trade strategy
                if self.trade_strategy:
                    data = self.trade_strategy(self.exchange.put_options, self.exchange.call_options, price, self.exchange.order_size)
                    await self.exchange.post_orders(data)

                # log strategy results for testing
                if self.test_strategy:
                    order_list = self.test_strategy(self.exchange.put_options, self.exchange.call_options, price)

                    if order_list.size:
                        self.logger.info(f'Price index: {price}')
                        
                        for d in order_list:
                            self.logger.log(FILE, ",".join(d))
                            # self.logger.log(FILE, ",".join(df_arbi.iloc[1].values.astype(str)))

                            # min = df_arbi['Cost'].values.argmin()
                            # self.logger.log(FILE, ",".join(df_arbi.iloc[min].values.astype(str)))

                self.exchange.updated = False
                self.count_to_reset = 0
                await asyncio.sleep(self.interval)
            
            else:
                self.logger.info('Prices not updated')
                self.count_to_reset += 1

                if self.count_to_reset == 100:
                    # self.exchange.keep_alive = False
                    self.logger.info('Resetting connection... ')
                    raise CBotError('Count_to_reset reached!')

                await asyncio.sleep(self.interval * 0.3)

        self.logger.info('check_riskfree_trade ended!')

    async def test_start(self):

        self.logger.info('start running')

        tasks = []

        await self.exchange.test_run()


    async def start(self):
        """Starts the bot with the parameters for synchronization.
        Synchronization will be carried out only if the store (store) is specified """

        self.logger.info('start running')

        tasks = []
    
        # tasks.append(asyncio.to_thread(self.check_riskfree_trade))
        tasks.append(asyncio.create_task(self.end_of_day()))
        tasks.append(asyncio.create_task(self.exchange.fetch_deribit_price_index()))
        # tasks.append(asyncio.create_task(self.exchange.order_mgmt_func(self.interval)))
        tasks.append(asyncio.create_task(self.check_riskfree_trade()))

        self.exchange.call_options, self.exchange.put_options = await self.exchange.prepare_option_struct()

        # if not self.exchange.call_options or not self.exchange.put_options:
        #     return

        # def update_options_dict(options_dict, strike: str, new_data) -> NoReturn:
        #     options_dict[strike].update(new_data)

        delay = 0.5
        for key in self.exchange.call_options:
            tasks.append(
                asyncio.create_task(
                    self.exchange.fetch_orderbook_data(key, delay=delay)
                )
            )
            delay += 0.5
        
        # for key, val in self.exchange.put_options.items():
        #     tasks.append(
        #         asyncio.create_task(
        #             self.exchange.fetch_orderbook_data(key, val['instrument_name'], self.exchange.put_options, delay=delay)
        #         )
        #     )
        #     delay += 0.5

        self.logger.info(f'Number of tasks: {len(tasks)}')
        await asyncio.gather(*tasks)
        # self.tasks = tasks

        # for task in tasks:
            # await asyncio.gather(task)
            # await task
            # time.sleep(0.5)

        self.logger.info(f'Tasks created: {len(tasks)}')
        self.logger.info('start > end !')

        # asyncio.gather(asyncio.to_thread(self.check_riskfree_trade))

        # with concurrent.futures.ThreadPoolExecutor() as executor:
        # #     [executor.submit(task) for task in tasks]

        #     for key, val in self.exchange.call_options.items():
        #         executor.submit(self.fetch_orderbook_data, key, val['instrument_name'], val['option_type'])
        #         # tasks.append([self.fetch_orderbook_data, key, val['instrument_name'], val['option_type']])
            
        #     for key, val in self.exchange.put_options.items():
        #         executor.submit(self.fetch_orderbook_data, key, val['instrument_name'], val['option_type'])
        #         # tasks.append([self.fetch_orderbook_data, key, val['instrument_name'], val['option_type']])

        #     # executor.submit(self.check_riskfree_trade)

        # # loop = asyncio.get_running_loop()
        # # await loop.run_in_executor(None, self.check_riskfree_trade)
        # # executor.submit(self.check_riskfree_trade)

    async def end_of_day(self):
        if datetime.now().hour > 8:
            await asyncio.sleep( 86400 - time.time() % 86400 + 28800)   # 24hrs + 8hrs, 8am
        else:
            await asyncio.sleep( 28800 - time.time() % 28800)   # 8hrs, 8am
            
        # await asyncio.sleep( 120 - time.time() % 120 )
        self.exchange.keep_alive = False
        self.logger.info('End of day!')
        await asyncio.sleep( 600 )  # sleep/wait for 10 minutes before starting
        
    def run(self) -> NoReturn:
        """Wrapper for start to run without additional libraries for managing asynchronous"""

        self.logger.info('Run started')
        loop = asyncio.get_event_loop()

        if self.exchange.env == 'test':
            self.start = self.test_start

        while True:
            try:
                loop.run_until_complete(self.start())
            
            except KeyboardInterrupt:
                self.exchange.keep_alive = False
                self.stop = True
                self.logger.info('Keyboard Interrupt detected...')

            except Exception as E:
                self.exchange.keep_alive = False
                self.logger.info(f'Error in run: {E}')
                self.logger.info(traceback.print_exc())


            finally:
                time.sleep(1)
                loop.run_until_complete(self.exchange.grace_exit())
                self.logger.info('Gracefully exit')
                
                for task in asyncio.all_tasks(loop):
                    task.cancel()

                time.sleep(1)

                if self.stop or self.exchange.env == 'test':
                    break
                    
                self.init_vals()