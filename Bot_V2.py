# https://github.com/n-eliseev/deribitsimplebot/blob/master/deribitsimplebot/bot.py
import json
import time
import asyncio
import concurrent.futures
import logging
import numpy as np
import pandas as pd

from datetime import date, timedelta
from typing import Union, Optional, NoReturn
import websockets

from exceptions import CBotResponseError , CBotError
# from .order import COrder
# from .interface import IBotStore

FILE = 60

class CBot:
    """The class describes the object of a simple bot that works with the Deribit exchange.
    Launch via the run method or asynchronously via start.
    The business logic of the bot itself is described in the worker method."""

    def __init__(self, url, auth: dict, currency: str = 'ETH', interval: int = 2, env: str = 'test',
                logger: Union[logging.Logger, str, None] = None):

        self.currency = currency
        self.interval = interval
        self.call_options = {}
        self.put_options = {}

        self.url = url[env]
        self.instrument = {}
        self.order = {}
        self.__credentials = auth[env]
        self.logger = (logging.getLogger(logger) if isinstance(logger,str) else logger)

        if self.logger is None:
            self.logger = logging.getLogger(__name__)

        self.logger.info(f'Bot init instrument = {self.instrument}')

        self.bot_stat = {
            'updated': False,
            'keep_alive': True,
            'asset_price': np.nan
        }
        self.df_initcols = ['strike', 'instrument_name', 'option_type', 'settlement_period']


    def create_message(self, method: str, params: dict = {},
                        mess_id: Union[int, str, None] = None,
                        as_dict: bool = False) -> Union[str, dict] :
        """The method returns an object or a JSON dump string with a body for a request to the exchange API"""

        obj = {
            "jsonrpc" : "2.0",
            "id" : ( str(time.time()).replace('.','_') if mess_id is None else mess_id),
            "method" : method,
            "params" : params
        }

        self.logger.debug(f'Create message = {obj}')

        return obj if as_dict else json.dumps(obj)


    def get_response_result(self, raw_response: str, raise_error: bool = True,
                            result_prop: str = 'result') -> Optional[dict]:
        """Receives the response body from the server, then returns an object
        located in result_prop, or throws an exception if from the server
        received error information
        """

        obj = json.loads(raw_response)

        self.logger.debug(f'Get response = {obj}')

        if result_prop in obj:
            return obj[result_prop]

        if 'error' in obj and raise_error:
            raise CBotResponseError(obj['error']['message'],obj['error']['code'])

        return None


    async def auth(self, ws) -> Optional[dict]:

        await ws.send(
            self.create_message(
                'public/auth',
                self.__credentials
            )
        )

        return self.get_response_result(await ws.recv())

    async def get_instruments(self, ws) -> Optional[dict]:
        self.logger.info('get_instruments')

        prop = {
            'currency': self.currency,
            'kind': 'option',
            'expired': False
        }

        await ws.send(
            self.create_message(
                'public/get_instruments',
                {**prop}
            )
        )

        return self.get_response_result(await ws.recv())

    async def unsubscribe_all(self, ws) -> Optional[dict]:
        self.logger.info('unsubscribe_all')

        await ws.send(
            self.create_message(
                'public/unsubscribe_all',
                {}
            )
        )

        return self.get_response_result(await ws.recv())

    async def fetch_deribit_price_index(self) -> NoReturn:
        """Реализует логику работы бота"""

        # self.logger.info(f'{instrument} Start worker order = {order.id} ,\
        #     prev_order = {prev_order.id}, group_id = {group_id}')

        # logger = logging.getLogger(__name__)

        async with websockets.connect(self.url) as websocket:

            await self.auth(websocket)

            await websocket.send(
                self.create_message(
                    'private/subscribe',
                    { "channels": [f'deribit_price_index.{self.currency.lower()}_usd'] }
                )
            )

            while websocket.open and self.bot_stat['keep_alive']:
                data = None
                message = self.get_response_result(await websocket.recv(), result_prop='params')

                if (not message is None and
                        ('channel' in message) and
                        ('data' in message)):

                    data = message['data']
                    self.bot_stat['asset_price'] = data['price']
                    self.logger.debug(f'Price index: {data}')

    async def fetch_orderbook_data(self, strike: str, instrument: str, option_type: str) -> NoReturn:
        """Реализует логику работы бота"""

        self.logger.info(f'fetch_orderbook_data: Listener for {instrument} started..')

        async with websockets.connect(self.url) as websocket:

            await self.auth(websocket)

            await websocket.send(
                self.create_message(
                    'private/subscribe',
                    { "channels": [f'quote.{instrument}'] }
                )
            )

            while websocket.open and self.bot_stat['keep_alive']:
                data = None
                message = self.get_response_result(await websocket.recv(), result_prop='params')

                if (not message is None and
                        ('channel' in message) and
                        ('data' in message)):

                    data = message['data']
                    self.logger.debug(f'Option quotes: {data}')

                    if option_type == 'put':
                        option_data = self.put_options[strike]

                        option_data['bid'] = data['best_bid_price'] if data['best_bid_price'] > 0 else np.nan
                        option_data['ask'] = data['best_ask_price'] if data['best_ask_price'] > 0 else np.nan

                    else:
                        option_data = self.call_options[strike]
                        
                        option_data['bid'] = data['best_bid_price'] if data['best_bid_price'] > 0 else np.nan
                        option_data['ask'] = data['best_ask_price'] if data['best_ask_price'] > 0 else np.nan
                    
                    self.bot_stat['updated'] = True
            
        self.logger.info(f'fetch_orderbook_data: Listener for {instrument} ended..')

    def check_riskfree_trade(self):

        put_label = ['bid_p', 'ask_p']
        call_label = ['bid_c', 'ask_c']
        csv_label = ['strike', 'buyOpt', 'sellOpt']

        # Set CSV Header
        self.logger.log(FILE, ",".join(csv_label + ['Side', 'PnL', 'Price']))

        while self.bot_stat['keep_alive']:
            self.logger.info('Checking for risk free trade...')


            if self.bot_stat['updated']:
                price = self.bot_stat['asset_price']

                df_put_data = pd.DataFrame(self.put_options.values())
                df_put_data.set_index('strike', inplace=True, drop=False)
                df_put_data.columns = self.df_initcols + ['date'] + put_label

                # Convert bids and asks to $
                put_bid_ask = df_put_data[put_label]
                put_bid_ask = put_bid_ask * price

                df_call_data = pd.DataFrame(self.call_options.values())
                df_call_data.set_index('strike', inplace=True, drop=False)
                df_call_data.columns = self.df_initcols + ['date'] + call_label

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
                
                if not df_arbi.empty:
                    self.logger.info(f'Price index: {self.bot_stat["asset_price"]}')

                    max = df_arbi['PnL'].values.argmax()
                    self.logger.log(FILE, ",".join(df_arbi.iloc[max].values.astype(str)))

                    # 
                    # csv = df_arbi.apply(lambda row: ",".join(row.to_string(header=False, index=False, name=False).split('\n')), axis=1)
                    # list(map(self.logger.log, [FILE]*csv.shape[0], csv))

                    # self.logger.info(df_arbi)

                self.bot_stat['updated'] = False
                time.sleep(self.interval)
            
            else:
                self.logger.info('Prices not updated')
                time.sleep(self.interval * 0.3)

        self.logger.info('Checking for risk free trade... stopping!')

    def calculate_imargin(self):
        # Futures >>
        # The initial margin starts with 2.0% (50x leverage trading) and linearly increases by 0.5% per 100 BTC increase in position size.
        # Initial margin = 2% + (Position Size in BTC) * 0.005%

        # Options >>
        # The initial margin is calculated as the amount of BTC that will be reserved to open a position.
        # Long call/put: None
        
        # Short call: Maximum (0.15 - OTM Amount/Underlying Mark Price, 0.1) + Mark Price of the Option
        # Short put : Maximum (Maximum (0.15 - OTM Amount/Underlying Mark Price, 0.1) + Mark Price of the Option, Maintenance Margin)

        pass

    def calculate_mmargin(self):
        # Futures >>
        # The maintenance margin starts with 1% and linearly increases by 0.5% per 100 BTC increase in position size.
        # When the account margin balance is lower than the maintenance margin, positions in the account will be incrementally reduced to keep the maintenance margin 
        # lower than the equity in the account. Maintenance margin requirements can be changed without prior notice if market circumstances demand such action.
        # Maintenance Margin= 1% + (PositionSize in BTC) * 0.005%

        # Options >>
        # The maintenance margin is calculated as the amount of BTC that will be reserved to maintain a position.
        # Long call/put: None

        # Short call: 0.075 + Mark Price of the Option
        # Short put : Maximum (0.075, 0.075 * Mark Price of the Option) + Mark Price of the Option

        pass

    def execute_trade(self):
        self.calculate_imargin()
        self.calculate_mmargin()

        pass


    async def prepare_option_struct(self) -> NoReturn:
        self.logger.info('prepare_option_struct')
        DAY = timedelta(1)
        expire_dt = date.today() + DAY
        expire_dt = expire_dt.strftime("%-d%b%y").upper()
        self.logger.info(f'Today is {expire_dt}')

        async with websockets.connect(self.url) as websocket:
            
            await self.auth(websocket)
            
            raw_instruments = await self.get_instruments(websocket)
            self.logger.info(f'Instruments: \n{raw_instruments[0]}')

            if raw_instruments:
                raw_instruments = pd.DataFrame(raw_instruments)

                pd_inst = pd.DataFrame(raw_instruments)[self.df_initcols].set_index('strike', drop=False)
                pd_inst['date'] = pd_inst['instrument_name'].str.split('-', expand=True)[1]
                pd_inst = pd_inst[pd_inst['date'] == expire_dt]
                # pd_inst = pd_inst[(pd_inst['settlement_period'] != 'month') & (pd_inst['settlement_period'] != 'week')]
                pd_inst.sort_index(inplace=True)

                pd_inst['bid'] = np.nan
                pd_inst['ask'] = np.nan

                self.logger.info('List of Instruments ----->>>>')
                self.logger.info(pd_inst)

                self.call_options = pd_inst[pd_inst['option_type'] == 'call'].to_dict('index')
                self.put_options  = pd_inst[pd_inst['option_type'] == 'put'].to_dict('index')

    async def grace_exit(self):
        self.logger.info('grace_exit')
        async with websockets.connect(self.url) as websocket:
            await self.unsubscribe_all(websocket)

    async def start(self) -> NoReturn:
        """Starts the bot with the parameters for synchronization.
        Synchronization will be carried out only if the store (store) is specified """

        self.logger.info('Bot start')

        tasks = []

        await self.prepare_option_struct()

        tasks.append(asyncio.to_thread(self.check_riskfree_trade))
        tasks.append(asyncio.ensure_future(self.fetch_deribit_price_index()))

        for key, val in self.call_options.items():
            tasks.append(
                asyncio.ensure_future(
                    self.fetch_orderbook_data(key, val['instrument_name'], val['option_type'])
                )
            )
        
        for key, val in self.put_options.items():
            tasks.append(
                asyncio.ensure_future(
                    self.fetch_orderbook_data(key, val['instrument_name'], val['option_type'])
                )
            )

        self.logger.info(f'Number of tasks: {len(tasks)}')

        for task in tasks:
            await asyncio.gather(task)
            time.sleep(0.3)

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
            
    def run(self) -> NoReturn:
        """Wrapper for start to run without additional libraries for managing asynchronous"""

        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(self.start())

        except KeyboardInterrupt:
            self.exchange.keep_alive = False
            self.logger.info('Keyboard Interrupt detected...')

        except Exception as E:
            self.logger.info(f'Error in run: {E}')
            self.logger.info(traceback.print_exc())

        finally:
            loop.run_until_complete(self.exchange.grace_exit())
            self.logger.info('Gracefully exit')