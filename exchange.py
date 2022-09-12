# https://github.com/n-eliseev/deribitsimplebot/blob/master/deribitsimplebot/bot.py
import asyncio
import json
import time
import logging
import numpy as np
import pandas as pd

from datetime import date, timedelta
from typing import Union, Optional, NoReturn
import websockets

from exceptions import CBotResponseError , CBotError

class Deribit_Exchange:
    """The class describes the object of a simple bot that works with the Deribit exchange.
    Launch via the run method or asynchronously via start.
    The business logic of the bot itself is described in the worker method."""

    def __init__(self, url, auth: dict, currency: str = 'ETH', env: str = 'test',
                logger: Union[logging.Logger, str, None] = None):

        self.currency = currency
        self.url = url[env]
        self.__credentials = auth[env]
        self.logger = (logging.getLogger(logger) if isinstance(logger,str) else logger)

        if self.logger is None:
            self.logger = logging.getLogger(__name__)

        self.df_initcols = ['strike', 'instrument_name', 'option_type']

        self.init_vals()
        self.logger.info(f'Bot init for {self.currency} options')

    def init_vals(self):
        # self.logger = logging.getLogger(__name__)
        self.order = {}
        self.keep_alive = True
        self.updated = False
        self.asset_price = 0

    @property
    def keep_alive(self) -> bool :
        return self._keep_alive

    @keep_alive.setter
    def keep_alive(self, ka: bool):
        self._keep_alive = ka

    @property
    def updated(self) -> bool :
        return self._updated

    @updated.setter
    def updated(self, upd: bool):
        self._updated = upd

    @property
    def asset_price(self) -> bool :
        return self._asset_price

    @asset_price.setter
    def asset_price(self, price: float):
        self._asset_price = price

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
            self.keep_alive = False
            self.logger.debug('Error found!')
            self.logger.debug(f'Error: code: {obj["error"]["code"]}')
            self.logger.debug(f'Error: msg: {obj["error"]["message"]}')
            raise CBotResponseError(obj['error']['message'],obj['error']['code'])

        else:
            # self.keep_alive = False
            self.logger.debug('Other unexpected messages!')
            self.logger.debug(f'Object contents: {obj}')

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

        async with websockets.connect(self.url) as websocket:

            await self.auth(websocket)

            await websocket.send(
                self.create_message(
                    'private/subscribe',
                    { "channels": [f'deribit_price_index.{self.currency.lower()}_usd'] }
                )
            )

            while self.keep_alive:
                if websocket.open:
                    data = None
                    message = self.get_response_result(await websocket.recv(), result_prop='params')

                    if (not message is None and
                            ('channel' in message) and
                            ('data' in message)):

                        data = message['data']
                        self.asset_price = data['price']
                        self.updated = True

                        self.logger.debug(f'Price index: {data}')
                
                else:
                    self.logger.info(f'Reconnecting Price listener...')
                    await self.auth(websocket)
                    await websocket.send(
                        self.create_message(
                            'private/subscribe',
                            { "channels": [f'deribit_price_index.{self.currency.lower()}_usd'] }
                        )
                    )
                    time.sleep(0.5)

        self.logger.info('fetch_deribit_price_index listener ended..')

    async def fetch_orderbook_data(self, strike: str, instrument: str, options_dict: dict) -> NoReturn:
        """Реализует логику работы бота"""

        self.logger.info(f'fetch_orderbook_data: Listener for {instrument} started..')

        async with websockets.connect(self.url) as websocket:

            await self.auth(websocket)

            await websocket.send(
                self.create_message(
                    'private/subscribe',
                    # { "channels": [f'quote.{instrument}'] }
                    { "channels": [f'ticker.{instrument}.raw'] }
                )
            )

            while self.keep_alive:
                if websocket.open:
                    data = None
                    message = self.get_response_result(await websocket.recv(), result_prop='params')

                    if (not message is None and
                            ('channel' in message) and
                            ('data' in message)):

                        data = message['data']
                        self.logger.debug(f'Option quotes: {data}')

                        new_data = {
                            'bid': data['best_bid_price'] if data['best_bid_price'] > 0 else np.nan,
                            'bid_amt': data['best_bid_amount'],
                            'ask': data['best_ask_price'] if data['best_ask_price'] > 0 else np.nan,
                            'ask_amt': data['best_ask_amount'],
                            'delta': data['greeks']['delta'],
                            'gamma': data['greeks']['gamma'],
                            'vega': data['greeks']['vega'],
                            'rho': data['greeks']['rho']
                        }

                        # func(options_dict, strike, new_data)
                        options_dict[strike].update(new_data)
                        self.updated = True
                    
                    else:
                        self.logger.info('Data not updated > ')
                        self.logger.info(f'Message: {message}')
                
                else:
                    self.logger.info(f'Reconnecting listener for {instrument}')
                    await self.auth(websocket)
                    await websocket.send(
                        self.create_message(
                            'private/subscribe',
                            # { "channels": [f'quote.{instrument}'] }
                            { "channels": [f'ticker.{instrument}.raw'] }
                        )
                    )
                    time.sleep(0.5)

            self.logger.info(f'fetch_orderbook_data: Listener for {instrument} ended..')

    async def prepare_option_struct(self) -> NoReturn:
        self.logger.info('prepare_option_struct')
        DAY = timedelta(2)          # 2 days+ option expiry
        expire_dt = date.today() + DAY
        self.logger.info(f'Today is {expire_dt}')
        expire_dt = expire_dt.strftime(f"{expire_dt.day}%b%y").upper()
        self.logger.info(f'Today is {expire_dt}')

        async with websockets.connect(self.url) as websocket:
            
            await self.auth(websocket)
            
            raw_instruments = await self.get_instruments(websocket)
            # self.logger.info(f'Instruments: \n{raw_instruments[0]}')

            if not raw_instruments:
                self.logger.info('Raw Instruments empty!')
                return (None, None)

            raw_instruments = pd.DataFrame(raw_instruments)

            # self.logger.info('List of Raw Instruments ----->>>>')
            # self.logger.info(raw_instruments['instrument_name'])

            pd_inst = pd.DataFrame(raw_instruments)[self.df_initcols].set_index('strike', drop=False)
            pd_inst['date'] = pd_inst['instrument_name'].str.split('-', expand=True)[1]

            while self.asset_price == 0:     # wait for price to be fetched
                self.logger.info('Price not updated!')
                await asyncio.sleep(0.5)
            
            styk_interval = 500
            bounds = 5000
            price = self.asset_price
            price -= price % styk_interval

            pd_inst = pd_inst[(pd_inst['date'] == expire_dt) & (pd_inst['strike'] >= price - bounds) & (pd_inst['strike'] <= price + bounds)]
            # pd_inst = pd_inst[(pd_inst['settlement_period'] != 'month') & (pd_inst['settlement_period'] != 'week')]
            pd_inst.sort_index(inplace=True)

            if pd_inst.empty:
                self.logger.info(f'No available options for day {expire_dt}')
                return (None, None)

            pd_inst['bid'] = np.nan
            pd_inst['ask'] = np.nan
            pd_inst['delta'] = 0.0
            pd_inst['gamma'] = 0.0
            pd_inst['vega'] = 0.0
            pd_inst['rho'] = 0.0

            self.logger.info('List of Instruments ----->>>>')
            self.logger.info(pd_inst)

            call_options = pd_inst[pd_inst['option_type'] == 'call'].to_dict('index')
            put_options  = pd_inst[pd_inst['option_type'] == 'put'].to_dict('index')

            return (call_options, put_options)

    async def grace_exit(self):
        self.logger.info('grace_exit')
        async with websockets.connect(self.url) as websocket:
            await self.unsubscribe_all(websocket)