# https://github.com/n-eliseev/deribitsimplebot/blob/master/deribitsimplebot/bot.py
import asyncio
import json
import time
import logging
import numpy as np
import pandas as pd

from datetime import date, datetime, timedelta
from typing import Union, Optional, NoReturn
import websockets

from exceptions import CBotResponseError , CBotError

class Deribit_Exchange:
    """The class describes the object of a simple bot that works with the Deribit exchange.
    Launch via the run method or asynchronously via start.
    The business logic of the bot itself is described in the worker method."""

    def __init__(self, url, auth: dict, currency: str = 'ETH', env: str = 'test', trading: bool = False, order_size: float = 0.1,
                logger: Union[logging.Logger, str, None] = None):

        self.currency = currency
        self.order_size = order_size
        self.url = url[env]
        self.__credentials = auth[env]
        self.env = env
        self.trading = trading
        self.logger = (logging.getLogger(logger) if isinstance(logger,str) else logger)

        if self.logger is None:
            self.logger = logging.getLogger(__name__)

        self.df_initcols = ['strike', 'instrument_name', 'option_type']

        if env == 'test': # set 
            self.close_losing_positions = self.close_all_positions

        self.init_vals()
        self.logger.info(f'Bot init for {self.currency} options, tradin = {trading}')

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

    def init_vals(self):
        # self.logger = logging.getLogger(__name__)
        self.orders = {}
        self.keep_alive = True
        self.updated = False
        self.asset_price = 0
        self.put_options = {}
        self.call_options = {}
        self.equity = 0
        self.init_price = None
        self.dates_traded = {}
        self.traded_prems = set()
        self.odate = None
        self.prev_call_options = {}
        self.prev_put_options = {}
        
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

    async def get_instrument(self, ws, instrument_name) -> Optional[dict]:
        self.logger.info('get_instrument')

        prop = {
            'instrument_name': instrument_name
        }

        await ws.send(
            self.create_message(
                'public/get_instrument',
                {**prop}
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

    async def get_index_price(self, ws, delay = 0) -> Optional[dict]:
        
        self.logger.info('get_index_price')

        await asyncio.sleep(delay)

        prop = { 'index_name': f'{self.currency.lower()}_usd' }

        await ws.send(
            self.create_message(
                'public/get_index_price',
                { 'index_name': f'{self.currency.lower()}_usd' }
            )
        )

        price = self.get_response_result(await ws.recv())
        if 'index_price' in price:
            self.init_price = price['index_price']
            self.asset_price = price['index_price']
            self.updated = True

    async def create_order(self, ws, instrument_name: str, price: float, amount: float,
                            direction: str = 'sell', label: str = '',
                            raise_error: bool = True):

        await ws.send(
            self.create_message(
                f'private/{direction}',
                { 'instrument_name' : instrument_name,
                  'amount' : amount,
                  'type' : 'limit',
                  'price' : price,
                  'label' : label }
            )
        )

        return self.get_response_result(await ws.recv(), raise_error = raise_error)

    # todo delete, not needed ?
    async def cancel_all_by_currency(self, ws, currency: str = 'BTC', kind: str = 'option',
                            raise_error: bool = True):

        await ws.send(
            self.create_message(
                f'private/cancel_all_by_currency',
                { 'currency': currency,
                  'kind': kind }
            )
        )

        return self.get_response_result(await ws.recv(), raise_error = raise_error)

    async def get_order_state(self, ws, order_id: Union[int, str],
                                raise_error: bool = True):

        await ws.send(
            self.create_message(
                f'private/get_order_state',
                { 'order_id': order_id }
            )
        )

        return self.get_response_result(await ws.recv(), raise_error = raise_error)

    async def get_open_orders_by_currency(self, ws, currency: str = 'BTC', kind: str = 'option',
                                raise_error: bool = True):

        await ws.send(
            self.create_message(
                f'private/get_open_orders_by_currency',
                { 'currency': currency,
                  'kind': kind }
            )
        )

        return self.get_response_result(await ws.recv(), raise_error = raise_error)

    async def get_user_trades_by_currency(self, ws, currency: str = 'BTC', kind: str = 'option',
                                    raise_error: bool = True):

        await ws.send(
            self.create_message(
                f'private/get_user_trades_by_currency',
                { 'currency': currency,
                  'kind': kind }
            )
        )

        return self.get_response_result(await ws.recv(), raise_error = raise_error)

    async def get_positions(self, ws, currency: str = 'BTC', kind: str = 'option',
                                    raise_error: bool = True):

        await ws.send(
            self.create_message(
                f'private/get_positions',
                { 'currency': currency,
                  'kind': kind }
            )
        )

        return self.get_response_result(await ws.recv(), raise_error = raise_error)

    async def get_order_history_by_currency(self, ws, currency: str = 'BTC', kind: str = 'option',
                                    raise_error: bool = True):

        await ws.send(
            self.create_message(
                f'private/get_order_history_by_currency',
                { 'currency': currency,
                  'kind': kind }
            )
        )

        return self.get_response_result(await ws.recv(), raise_error = raise_error)

    async def get_account_summary(self, ws, currency: str = 'BTC',
                                    raise_error: bool = True):

        await ws.send(
            self.create_message(
                f'private/get_account_summary',
                { 'currency': currency }
            )
        )

        return self.get_response_result(await ws.recv(), raise_error = raise_error)

    async def close_position(self, ws, instrument_name: str, price: float, 
                                ordtype: str = 'limit',
                                raise_error: bool = True):

        await ws.send(
            self.create_message(
                f'private/close_position',
                { 'instrument_name': instrument_name,
                  'type': ordtype, 
                  'price': price }
            )
        )

        self.get_response_result(await ws.recv(), raise_error = raise_error)
        

    async def unsubscribe_all(self, ws) -> Optional[dict]:
        self.logger.info('unsubscribe_all')

        await ws.send(
            self.create_message(
                'public/unsubscribe_all',
                {}
            )
        )

        return self.get_response_result(await ws.recv())

    async def post_orders(self, order_list):

        if not self.trading: return
        if self.equity <= 0: return
        if self.avail_funds / self.equity <= 0.2: return

        # order_list, premium = data
        if order_list:
            self.logger.info(f'post_orders')
            err_tresh = 0

            _, odate, _, _  = order_list[0]['instrument']['instrument_name'].split('-')
            premium = str(order_list[0]['sum_prem'])
            # if odate in self.dates_traded:
            #     return                      # trading done for the day

            # if multiple trades
            # currently not possible to distinguish positions per premium group during get positions
            # for order in order_list:
            #     tprems += float(order['bid'])

            # if odate in self.dates_traded:
            if premium in self.traded_prems:
                return

            # websocket = await websockets.connect(self.url)
            async with websockets.connect(self.url) as websocket:

                await self.auth(websocket)
            
                try:
                    for idx, order in enumerate(order_list.copy()):
                        self.logger.info(f'Selling {self.order_size} amount of {order["instrument"]["instrument_name"]} at {order["bid"]} premium')
                        strike_dist = order['strike_dist']
                        order_res = await self.create_order(
                            websocket,
                            instrument_name = order['instrument']['instrument_name'],
                            price = order['bid'],
                            amount = self.order_size,
                            label =  f'{premium},{strike_dist}' #premium, strike distance, 
                        )
                        if 'order' in order_res:
                            order_det = order_res['order']
                            self.orders[order_det['instrument_name']] = order['instrument']
                            order_list.pop(idx)
                            # premiums += float(order['bid'])
                            await asyncio.sleep(0.5)

                        else:
                            self.logger.info('Error in post_orders: Order not in order_res!')

                    # update equity
                    res = await self.get_account_summary(websocket, currency=self.currency)
                    self.equity = float(res['equity'])

                except Exception as E:
                    self.logger.info(f'Error in post_orders: {E}')
                    # self.logger.info(f'Reconnecting post_orders...')
                    # err_tresh += 1
                    # websocket = await websockets.connect(self.url)
                    # await self.auth(websocket)
                    # await asyncio.sleep(0.5)

                    # if err_tresh == 4:
                        # close openned positions after 3 errors
                        # raise CBotError('Error treshold reached in post_orders!')

            # if odate in self.dates_traded:
                # currently not possible to distinguish positions per premium group during get positions
                # if premium not in self.dates_traded[odate]:  
                # self.dates_traded[odate].add(premium)
            
            # else:
            self.traded_prems.add(premium)
    
    async def close_losing_positions(self):

        if self.orders:
            err_tresh = 0
            # websocket = await websockets.connect(self.url)
            async with websockets.connect(self.url) as websocket:

                await self.auth(websocket)

                try:
                    for id, order in self.orders.copy().items():

                        if (order['option_type'] == 'put' and self.asset_price <= order['strike']) or \
                            (order['option_type'] == 'call' and self.asset_price >= order['strike']):
                            
                            self.logger.info(f'Closing position {order["instrument_name"]} at price {order["ask"]}')
                            res = await self.close_position(websocket, order['instrument_name'], order['ask'])
                            self.orders.pop(id, None)
                            await asyncio.sleep(0.5)

                except Exception as E:
                    self.logger.info(f'Error in close_losing_positions: {E}')
                    # self.logger.info(f'Reconnecting close_losing_positions...')
                    # err_tresh += 1
                    # websocket = await websockets.connect(self.url)
                    # await self.auth(websocket)
                    # await asyncio.sleep(0.5)

                    # if err_tresh == 4:
                    #     raise CBotError('Error treshold reached in close_losing_positions!')

    async def close_all_positions(self):

        if self.orders:
            err_tresh = 0
            # websocket = await websockets.connect(self.url)
            async with websockets.connect(self.url) as websocket:
                await self.auth(websocket)

                try:
                    for id, order in self.orders.copy().items():
                        self.logger.info(f'Closing position {order["instrument_name"]} at price {order["ask"]}')
                        res = await self.close_position(websocket, order['instrument_name'], order['ask'])
                        self.orders.pop(id, None)
                        await asyncio.sleep(0.5)

                except Exception as E:
                    self.logger.info(f'Error in close_all_positions: {E}')
                    self.logger.info(f'Reconnecting close_all_positions...')
                    err_tresh += 1
                    websocket = await websockets.connect(self.url)
                    await self.auth(websocket)
                    await asyncio.sleep(0.5)

                    if err_tresh == 4:
                        raise CBotError('Error treshold reached in close_all_positions!')

            raise CBotError('Test cycle ended!')

    async def fetch_account_equity(self, ws, delay=0):

        if not self.trading: return

        self.logger.info(f'fetch_account_equity')

        await asyncio.sleep(delay)
        res = await self.get_account_summary(ws, currency=self.currency)
        self.equity = float(res['equity'])
        self.avail_funds = float(res['available_funds'])

    async def fetch_account_positions(self, ws, delay = 0):

        if not self.trading: return

        self.logger.info(f'fetch_account_positions')

        await asyncio.sleep(delay)
        orders = await self.get_positions(ws, currency=self.currency)
        orders_hist = await self.get_order_history_by_currency(ws, currency=self.currency)
        instrument = None

        for order in orders:
            _, odate, strike, order_type  = order['instrument_name'].split('-')

            if order['realized_profit_loss'] == '0':
                if odate == self.odate:
                    if order_type == 'P':
                        instrument = self.put_options[float(strike)]
                    else:
                        instrument = self.call_options[float(strike)]

                else:
                    if order_type == 'P':
                        instrument = self.prev_put_options[float(strike)]
                    else:
                        instrument = self.prev_call_options[float(strike)]
                
                self.orders[order['instrument_name']] = instrument

        for order in orders_hist:
            _, odate, strike, order_type  = order['instrument_name'].split('-')
            
            if odate == self.odate:
                try: 
                    lbl_prem, _ = order['label'].split(',')
                except Exception as E:
                    lbl_prem = order['label']

                if lbl_prem not in self.traded_prems:
                    self.traded_prems.add(lbl_prem)

    async def order_mgmt_func_bk(self):

        if not self.trading: return

        instrument = {}

        websocket = await websockets.connect(self.url)
            
        await self.auth(websocket)

        # initialize equity
        res = await self.get_account_summary(websocket, currency=self.currency)
        self.equity = float(res['equity'])

        orders = await self.get_positions(websocket, currency=self.currency)

        for order in orders:
            _, _, strike, order_type  = order['label'].split('-')

            if order_type == 'P':
                instrument = self.put_options[float(strike)]
            else:
                instrument = self.call_options[float(strike)]

            self.orders[order['order_id']] = instrument

        while self.keep_alive:
            try:

                for id, order in self.orders.copy().items():

                    if (order['option_type'] == 'put' and self.asset_price <= order['strike']) or \
                        (order['option_type'] == 'call' and self.asset_price >= order['strike']):
                        
                        await self.close_position(websocket, order['instrument_name'], order['ask'])
                        self.orders.pop(id, None)
                        await asyncio.sleep(0.5)
                    
            except Exception as E:
                self.logger.info(f'Error in order_mgmt_func: {E}')
                self.logger.info(f'Reconnecting order_mgmt_func...')
                websocket = await websockets.connect(self.url)
                await self.auth(websocket)

            await asyncio.sleep(0.5)

    async def test_run(self) -> NoReturn:

        self.logger.info(f'test_run')

        # websocket = await websockets.connect(self.url)
        async with websockets.connect(self.url) as websocket:
            await self.auth(websocket)
            await asyncio.gather(
                self.fetch_account_equity(websocket, 0.5),
                # self.fetch_account_positions(websocket, 1),
                self.get_index_price(websocket, 1)
            )

            order_res = await self.create_order(
                websocket,
                instrument_name = 'BTC-20OCT22-18000-P',
                price = 0.0205,
                amount = self.order_size,
                label = '0.0205'
            )
            if 'order' in order_res:
                order_det = order_res['order']
                await asyncio.sleep(0.5)

            await self.close_position(websocket, 'BTC-20OCT22-18000-P', 0.0255)


            self.logger.info(f'test_run ended!')

    async def fetch_deribit_price_index(self) -> NoReturn:
        """Реализует логику работы бота"""
        self.logger.info(f'fetch_deribit_price_index')

        websocket = await websockets.connect(self.url)

        await self.auth(websocket)
        await self.fetch_account_equity(websocket)
        await self.fetch_account_positions(websocket)
        # await self.get_index_price(websocket)

        await websocket.send(
            self.create_message(
                'private/subscribe',
                { "channels": [f'deribit_price_index.{self.currency.lower()}_usd'] }
            )
        )

        self.logger.info(f'fetch_deribit_price_index: before while loop')

        data = None
        while self.keep_alive:

            try:    
                message = self.get_response_result(await websocket.recv(), result_prop='params')

                if (not message is None and
                        ('channel' in message) and
                        ('data' in message)):

                    data = message['data']
                    self.asset_price = data['price']
                    self.updated = True

                    self.logger.debug(f'Price index: {self.asset_price}')

                    await self.close_losing_positions()

                    price = int(self.asset_price)
                    if price in self.put_options:
                        self.logger.info(f'ATM PUT buy price:  {self.put_options[price]["ask"]}: price: {price}')
                        self.logger.info(f'ATM CALL buy price: {self.call_options[price]["ask"]}: price: {price}')

                    # if self.asset_price >= self.init_price + 2000 or self.asset_price <= self.init_price - 2000:
                    #     self.logger.info('Resetting bot... ')
                    #     raise CBotError('Price moved +-2000!')
            
            except Exception as E:
                self.logger.info(f'Error in fetch_deribit_price_index: {E}')
                self.logger.info(f'Reconnecting Price listener...')
                websocket = await websockets.connect(self.url)
                await self.auth(websocket)
                await websocket.send(
                    self.create_message(
                        'private/subscribe',
                        { "channels": [f'deribit_price_index.{self.currency.lower()}_usd'] }
                    )
                )
                await asyncio.sleep(0.5)

        self.logger.info('fetch_deribit_price_index listener ended..')

    async def fetch_orderbook_data(self, strike: str, delay: float = 0, odate: str = '') -> NoReturn:
        """Реализует логику работы бота"""
        await asyncio.sleep(delay)
        
        self.logger.info(f'fetch_orderbook_data: Listener for {strike} started..')

        websocket = await websockets.connect(self.url)

        await self.auth(websocket)

        put_inst_name = ''
        call_inst_name = ''
        put_options = {}
        call_options = {}
        if odate == '':
            put_inst_name = self.put_options[float(strike)]['instrument_name']
            call_inst_name = self.call_options[float(strike)]['instrument_name']
            put_options = self.put_options
            call_options = self.call_options
        else:
            put_inst_name = self.prev_put_options[float(strike)]['instrument_name']
            call_inst_name = self.prev_call_options[float(strike)]['instrument_name']
            put_options = self.prev_put_options
            call_options = self.prev_call_options

        await websocket.send(
            self.create_message(
                'private/subscribe',
                { "channels": [f'ticker.{put_inst_name}.raw'] }
            )
        )
        
        await websocket.send(
            self.create_message(
                'private/subscribe',
                { "channels": [f'ticker.{call_inst_name}.raw'] }
            )
        )

        data = None
        while self.keep_alive:

            try:
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

                    _, _, strike, order_type  = data['instrument_name'].split('-')

                    if order_type == 'P':
                        put_options[float(strike)].update(new_data)
                    else:
                        call_options[float(strike)].update(new_data)

                    # options_dict[strike].update(new_data)
                    self.updated = True
                
                else:
                    self.logger.info('Data not updated > ')
                    self.logger.info(f'Message: {message}')
            
            except Exception as E:
                self.logger.info(f'Reconnecting listener for {strike}')
                websocket = await websockets.connect(self.url)
                await self.auth(websocket)
                await websocket.send(
                    self.create_message(
                        'private/subscribe',
                        { "channels": [f'ticker.{put_inst_name}.raw'] }
                    )
                )
                await websocket.send(
                    self.create_message(
                        'private/subscribe',
                        { "channels": [f'ticker.{call_inst_name}.raw'] }
                    )
                )
                await asyncio.sleep(delay)

        self.logger.info(f'fetch_orderbook_data: Listener for {strike} ended..')

    async def prepare_prev_option_struct(self) -> NoReturn:

        if not self.trading: return

        self.logger.info(f'prepare_cont_option_struct')

        async with websockets.connect(self.url) as websocket:

            orders = await self.get_positions(websocket, currency=self.currency)

            for order in orders:
                _, odate, strike, order_type  = order['instrument_name'].split('-')

                if odate != self.odate:
                    if order['realized_profit_loss'] == '0':
                        instrument = await self.get_instrument(websocket, order['instrument_name'])

                        if order_type == 'P':
                            self.prev_put_options[float(strike)] = instrument
                        else:
                            self.prev_call_options[float(strike)] = instrument



    async def prepare_option_struct(self) -> NoReturn:

        self.logger.info('prepare_option_struct')
        DAY = None

        if datetime.now().hour < 8 or self.env == 'test':
            DAY = timedelta(1)          # 1 day option expiry
        else:
            DAY = timedelta(2)          # 2 days option expiry

        expire_dt = date.today() + DAY
        self.logger.info(f'Today is {expire_dt}')
        expire_dt = expire_dt.strftime(f"{expire_dt.day}%b%y").upper()
        self.logger.info(f'Today is {expire_dt}')

        self.odate = expire_dt

        async with websockets.connect(self.url) as websocket:
            
            await self.auth(websocket)
            
            raw_instruments = await self.get_instruments(websocket)
            await self.get_index_price(websocket)
            
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
            
            styk_interval = 250
            bounds = 5000
            price = self.asset_price
            price -= price % styk_interval

            pd_inst = pd_inst[(pd_inst['date'] == expire_dt) & (pd_inst['strike'] >= price - bounds) & (pd_inst['strike'] <= price + bounds)]
            # pd_inst = pd_inst[(pd_inst['date'] == expire_dt) \
            #     & (
            #         ((pd_inst['option_type'] == 'call') & (pd_inst['strike'] >= price - 2000) & (pd_inst['strike'] <= price + bounds)) \
            #         | ((pd_inst['option_type'] == 'put') & (pd_inst['strike'] >= price - bounds) & (pd_inst['strike'] <= price + 2000))
            #     )]
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