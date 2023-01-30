# https://github.com/n-eliseev/deribitsimplebot/blob/master/deribitsimplebot/bot.py
import asyncio
import json
import time
import logging
import numpy as np
import pandas as pd

from datetime import date, datetime, timedelta, timezone
from typing import Union, Optional, NoReturn
import websockets

from exceptions import CBotResponseError , CBotError

class Deribit_Exchange:
    """The class describes the object of a simple bot that works with the Deribit exchange.
    Launch via the run method or asynchronously via start.
    The business logic of the bot itself is described in the worker method."""

    def __init__(self, url, auth: dict, currency: str = 'ETH', env: str = 'test', trading: bool = False, order_size: float = 0.1,
                daydelta: int = 2, risk_perc: float = 0.003, min_prem: float = 0.008, strike_dist: int = 1500, expire_time: int = 7,
                dvol_min: float = 50.0, dvol_mid: float = 60.0, default_prems = None, max_prem_cnt = 2, maker: bool = False,
                logger: Union[logging.Logger, str, None] = None):

        self.currency = currency
        self.order_size = order_size
        self.daydelta = daydelta
        self.risk_perc = risk_perc 
        self.min_prem = min_prem
        self.strike_dist = strike_dist
        self.expire_time = expire_time
        self.dvol_min = dvol_min
        self.dvol_mid = dvol_mid
        self.default_prems = default_prems
        self.max_prem_cnt = max_prem_cnt
        self.maker = maker

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
        self.logger.info(f'min_prem={min_prem} strike_dist={strike_dist}')

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
        self.pos_updated = False
        self.asset_price = 0
        self.put_options = {}
        self.call_options = {}
        self.equity = 0
        self.avail_funds = 0
        self.dvol = 0
        self.dates_traded = {}
        self.traded_prems = {}
        self.max_traded_prem = 0.0
        self.odate = None
        # self.prev_call_options = {}
        # self.prev_put_options = {}
        self.trigger_orders = {}
        # self.best_put_instr = None
        # self.best_call_instr = None
        
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

        if 'error' in obj:
            self.keep_alive = False
            self.logger.info('Error found!')
            self.logger.info(f'Error: code: {obj["error"]["code"]}')
            self.logger.info(f'Error: msg: {obj["error"]["message"]}')

            if raise_error:
                raise CBotResponseError(obj['error']['message'],obj['error']['code'])

        else:
            # self.keep_alive = False
            self.logger.debug('Other unexpected messages!')
            self.logger.debug(f'Object contents: {obj}')

        return None


    async def auth(self, ws, creds=None) -> Optional[dict]:

        if creds is None:
            creds = self.__credentials

        await ws.send(
            self.create_message(
                'public/auth',
                creds
                # self.__credentials
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

    async def get_index_price(self, ws, delay = 0):
        
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
            # self.init_price = price['index_price']
            self.asset_price = price['index_price']
            self.updated = True

        return self.asset_price 

    async def create_order(self, ws, direction: str = 'sell', params: dict = {},
                            raise_error: bool = True):

        await ws.send(
            self.create_message(
                f'private/{direction}',
                { **params }
            )
        )

        return self.get_response_result(await ws.recv(), raise_error = raise_error)

    async def edit_order(self, ws, params: dict = {}, raise_error: bool = True):

        await ws.send(
            self.create_message(
                f'private/edit',
                { **params }
            )
        )

        return self.get_response_result(await ws.recv(), raise_error = raise_error)

    async def create_order_bk(self, ws, instrument_name: str, price: float, amount: float,
                            direction: str = 'sell', label: str = '', ord_type: str = 'limit',
                            params: dict = {},
                            raise_error: bool = True):

        await ws.send(
            self.create_message(
                f'private/{direction}',
                { 'instrument_name' : instrument_name,
                  'amount' : amount,
                  'type' : ord_type,
                  'price' : price,
                  'label' : label }
            )
        )

        return self.get_response_result(await ws.recv(), raise_error = raise_error)

    async def cancel_all(self, ws, raise_error: bool = True):

        await ws.send(
            self.create_message(
                f'private/cancel_all',
                {}
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

    async def get_open_orders_by_instrument(self, ws, instrument_name: str = '', oo_type: str = '',
                                raise_error: bool = True):

        await ws.send(
            self.create_message(
                f'private/get_open_orders_by_instrument',
                { 'instrument_name': instrument_name,
                  'type': oo_type }
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

    async def close_position(self, ws, params, raise_error: bool = True):

        await ws.send(
            self.create_message(
                f'private/close_position',
                { **params }
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

    async def get_ord_size(self):
        # total premium (reward) = 0.008
        # estimated loss         = 0.01
        # diff                   = 0.002
        # % risk 0.003

        loss = 0.015 - self.min_prem
        

        # to_risk = self.avail_funds * self.risk_perc
        to_risk = self.equity * self.risk_perc
        to_risk /= loss
        to_risk -= to_risk % 0.1
        to_risk = np.round(to_risk, 1)
        self.order_size = max( to_risk , 0.1 )

    async def check_init_margin_vs_fund(self):
        # calc init margin for new orders (put and call): ordsize * 0.1
        init_margin = self.order_size * 0.1 * 2
        # calc 5% of available funds
        fund_perc = self.avail_funds * 0.05
        im_fund = init_margin + fund_perc

        self.logger.info(f'order_size={self.order_size}')
        self.logger.info(f'chk init margin vs fund: {im_fund} > {self.avail_funds}')

        # return true if not enough fund available
        return im_fund >= self.avail_funds

    def calc_amount(self, call_strike, ord_size):
        # call_strike = float(order_list[0]['call_strike'])
        amount = call_strike * ord_size # * 0.1
        self.logger.info(f'{amount} amt = {call_strike} strike * {ord_size} size')
        if amount % 10 != 0:
            amount -= amount % 10
            amount += 10

        self.logger.info(f'new amount = {amount}')
        return amount

    async def post_orders(self, order_list):

        if not self.trading: return
        if self.avail_funds <= 0: return
        
        self.logger.info(f'order_list: {len(order_list)}')

        # order_list, premium = data
        if order_list:
            await self.get_ord_size()

            self.logger.info(f'post_orders')
            err_tresh = 0
            bid_ask = 'bid'

            new_order_list = []
            new_order = {}
            strk_dist = 0.0
            premium = 0.0
            # call_strike = 0.0
            price = 0.0
            
            # for order in order_list.copy():
            #     # if order[bid_ask] == 0.0005:
            #     #     price = 0.001
            #     # else:
            #     price = order[bid_ask]

            #     if order['option_type'] == 'put':
            #         if self.best_put_instr:
            #             if self.best_put_instr['bid'] > price                    \
            #                 and self.best_put_instr['strike'] < self.asset_price \
            #                 and abs(self.asset_price - self.best_put_instr['strike']) >= 30:
            #                 new_order = {
            #                     'instrument' : self.best_put_instr,
            #                     'bid'        : self.best_put_instr['bid'],
            #                     'ask'        : self.best_put_instr['ask'],
            #                     'strike'     : self.best_put_instr['strike'],
            #                     'option_type': 'put',
            #                     'direction'  : 'sell',
            #                     'trigger_price': self.best_put_instr['strike'] - 10
            #                 }
            #             else:
            #                 self.best_put_instr = order['instrument']
            #                 new_order = order
            #         else:
            #             self.best_put_instr = order['instrument']
            #             new_order = order

            #     else:
            #         if self.best_call_instr:
            #             if self.best_call_instr['bid'] > price                    \
            #                 and self.best_call_instr['strike'] > self.asset_price \
            #                 and abs(self.best_call_instr['strike'] - self.asset_price) >= 30:
            #                 new_order = {
            #                     'instrument' : self.best_call_instr,
            #                     'bid'        : self.best_call_instr['bid'],
            #                     'ask'        : self.best_call_instr['ask'],
            #                     'strike'     : self.best_call_instr['strike'],
            #                     'option_type': 'call',
            #                     'direction'  : 'buy',
            #                     'trigger_price': self.best_call_instr['strike'] + 10
            #                 }
            #             else:
            #                 self.best_call_instr = order['instrument']
            #                 new_order = order
            #         else:
            #             self.best_call_instr = order['instrument']
            #             new_order = order

            #         # call_strike = float(new_order['strike'])

            #     new_order_list.append(new_order)
            #     strk_dist += new_order['strike']
            #     premium += new_order[bid_ask]

            # self.logger.info(f'Best call strike: {self.best_call_instr["strike"]}')
            # self.logger.info(f'Best put strike: {self.best_put_instr["strike"]}')
            # _, odate, strike, _  = order_list[0]['instrument']['instrument_name'].split('-')
            # order_list = new_order_list

            # if premium < self.min_prem and len(self.traded_prems) == 0:
            #     premium = 0

            # allow all trades when low volatility and time between 0-exp time

            if self.maker:
                bid_ask = 'ask' 
            else:
                bid_ask = 'bid' 

            max_prem_cnt = self.max_prem_cnt

            premium = order_list[0]['sum_premium'][bid_ask]
            self.logger.info(f'Premium is {premium}')

            if np.isnan(premium):
                return

            if datetime.now(timezone.utc).hour >= 8:

                if self.dvol < self.dvol_min:
                    max_prem_cnt = self.max_prem_cnt * 2

                else:
                    if self.dvol >= self.dvol_mid:
                        if premium < self.min_prem or \
                            strk_dist <= self.strike_dist:
                            self.logger.info(f'Premium {premium} < {self.min_prem} or Strike Dist {strk_dist} <= {self.strike_dist}')
                            return

                        # if premium <= self.max_traded_prem:
                        #     self.logger.info(f'{premium} premium <= {self.max_traded_prem} max traded prem')
                        #     return
                    
                    else:
                        max_prem_cnt = self.max_prem_cnt * 2

                    if str(premium) in self.traded_prems:
                        if self.traded_prems[str(premium)] >= max_prem_cnt:
                            self.logger.info(f'Max count of {self.traded_prems[str(premium)]} for premium {premium} already traded!')
                            return

            # websocket = await websockets.connect(self.url)

            premium = str(premium)
            async with websockets.connect(self.url) as websocket:

                await self.auth(websocket)

                await self.fetch_account_equity(websocket)

                # update equity
                # res = await self.get_account_summary(websocket, currency=self.currency)
                # self.equity = float(res['equity'])

                if self.avail_funds / self.equity <= 0.3: 
                    self.logger.info(f'Available fund {self.avail_funds} / {self.equity} equity <= 30%')
                    return
                
                if await self.check_init_margin_vs_fund(): return
            
                # try:
                direction = ''
                amount = 0.0
                price = 0.0
                err_loc = ''
                for idx, order in enumerate(order_list.copy()):

                    try:
                        err_loc = order['instrument']['instrument_name']

                        # if datetime.now(timezone.utc).hour >= 8 and order[bid_ask] == 0.0005:
                        #     price = 0.001
                        # else:
                        #     price = order[bid_ask]

                        price = order[bid_ask]
                        ord_size = self.order_size * max_prem_cnt

                        self.logger.info(f'Selling {ord_size} amount of {order["instrument"]["instrument_name"]} at {price} premium')
                        params = {
                            'instrument_name' : order['instrument']['instrument_name'],
                            'type'            : 'limit',
                            'price'           : price,
                            'amount'          : ord_size,
                            'label'           :  f'{premium},{strk_dist}' #premium, strike distance, 
                        }
                        order_res = await self.create_order(websocket, 'sell', params)
                        if 'order' in order_res:
                            order_det = order_res['order']
                            self.orders[order_det['instrument_name']] = order['instrument']
                            # order_list.pop(idx)
                            # premiums += float(order['bid'])
                            await asyncio.sleep(0.5)
                            
                            if order['strike'] in self.trigger_orders:
                                err_loc = f'Modify BTC-PERPETUAL {order["option_type"]}'
                                osize = self.trigger_orders[order['strike']]['order_size'] + ord_size
                                self.trigger_orders[order['strike']]['order_size'] = osize
                                self.logger.info(f'Total order size: {osize}')
                                amount = self.calc_amount(order['trigger_price'], osize)
                                self.logger.info(f'Modifying order with amount {amount}')

                                params = {
                                    'order_id': self.trigger_orders[order['strike']]['order_id'],
                                    'amount'  : amount
                                }
                                await self.edit_order(websocket, params)
                            
                            else:
                                err_loc = f'New BTC-PERPETUAL {order["option_type"]}'
                                direction = order['direction']
                                price = order['trigger_price']
                                amount = self.calc_amount(order['trigger_price'], ord_size)
                                self.logger.info(f'{direction}ing {amount} amount of BTC-PERPETUAL at {price} price and trigger price at {order["strike"]}')
                                params = {
                                    'instrument_name' : 'BTC-PERPETUAL',
                                    'type'            : 'stop_limit',
                                    'price'           : price,
                                    'amount'          : amount,
                                    'trigger'         : 'mark_price',
                                    'trigger_price'   : order['strike'],
                                    'post_only'       : True,
                                    'max_show'        : 0,
                                    'label'           :  f'{premium},{strk_dist}' #premium, strike distance, 
                                }
                                order_res = await self.create_order(websocket, direction, params)
                                
                                trig_ord = {
                                    'order_size'  : ord_size,
                                    'order_id': order_res['order']['order_id']
                                }
                                self.trigger_orders[order['strike']] = trig_ord
                                
                        else:
                            self.logger.info('Error in post_orders: Order not in order_res!')

                        await asyncio.sleep(0.5)

                    except Exception as E:
                        self.logger.info(f'Error in post_orders: {err_loc} : {E}')
            
            # else:
            # self.traded_prems.add(premium)
            if premium in self.traded_prems:
                self.traded_prems[premium] += max_prem_cnt
            else:
                self.traded_prems[premium] = max_prem_cnt

            self.max_traded_prem = float(premium)
    
    async def close_losing_positions(self):

        if self.orders:
            err_tresh = 0
            # websocket = await websockets.connect(self.url)
            async with websockets.connect(self.url) as websocket:

                await self.auth(websocket)

                try:
                    # for id, order in self.orders.copy().items():
                    if (order['option_type'] == 'put' and self.asset_price <= float(order['strike'])) or \
                        (order['option_type'] == 'call' and self.asset_price >= float(order['strike'])):
                        
                        # self.logger.info(f'Closing position {order["instrument_name"]} at price {order["ask"]}')
                        params = { 
                            'instrument_name': order['instrument_name'],
                            'type': 'limit', 
                            'price': order['ask'] 
                        }
                        res = await self.close_position(websocket, params)
                        self.orders.pop(id, None)
                        await asyncio.sleep(0.5)

                except Exception as E:
                    self.logger.info(f'Error in close_losing_positions: {E}')


    async def close_all_positions(self):

        if self.orders:
            err_tresh = 0
            # websocket = await websockets.connect(self.url)
            async with websockets.connect(self.url) as websocket:
                await self.auth(websocket)

                try:
                    # cancel all user orders and triggers on all currencies
                    await self.cancel_all(websocket)
                    await asyncio.sleep(0.5)

                    instrument_name = 'BTC-PERPETUAL'
                    self.logger.info(f'Closing position {instrument_name}')
                    params = {
                            'instrument_name': instrument_name,
                            'type': 'market'
                        }
                    order_res = await self.close_position(websocket, params, raise_error = False)
                    if 'order' in order_res:
                        self.logger.info('BTC-PERPETUAL closed...')
                        # self.logger.info(f'BTC-PERPETUAL closed at price {order_res["order"]["price"]} profit loss of {order_res["order"]["profit_loss"]}')
                    else:
                        self.logger.info('Order not in response. Error closing BTC-PERPETUAL ...')

                    await asyncio.sleep(0.5)

                except Exception as E:
                    self.logger.info(f'Error in close_all_positions: {E}')
            
            self.logger.info('All positions closed!')

    async def fetch_account_equity(self, ws, delay=0):

        if not self.trading: return

        self.logger.info(f'fetch_account_equity')

        await asyncio.sleep(delay)
        res = await self.get_account_summary(ws, currency=self.currency)
        self.equity = float(res['equity'])
        self.avail_funds = float(res['available_funds'])

    async def fetch_trigger_orders(self, ws, delay = 0):
        if not self.trading: return

        self.logger.info(f'fetch_trigger_orders')

        await asyncio.sleep(delay)
        orders = await self.get_positions(ws, currency=self.currency)

        trig_orders = await self.get_open_orders_by_instrument(ws, 'BTC-PERPETUAL', 'stop_limit')

        for order in trig_orders:
            params = {
                'order_id': order['order_id'],
                'order_size'  : 0
            }
            self.trigger_orders[float(order['price'])] = params

    async def fetch_account_positions(self, ws, delay = 0):

        if not self.trading: return

        self.logger.info(f'fetch_account_positions')

        await asyncio.sleep(delay)
        orders = await self.get_positions(ws, currency=self.currency)
        orders_hist = await self.get_order_history_by_currency(ws, currency=self.currency)
        instrument = None

        for order in orders:
            if order['instrument_name'] == 'BTC-PERPETUAL':
                continue

            _, odate, strike, order_type  = order['instrument_name'].split('-')
            
            self.logger.info(f"{order['instrument_name']} : {order['realized_profit_loss']}")
            if float(order['realized_profit_loss']) == 0:
                # if odate == self.odate:
                if order_type == 'P':
                    instrument = self.put_options[float(strike)]
                else:
                    instrument = self.call_options[float(strike)]

                # else:
                #     if order_type == 'P':
                #         instrument = self.prev_put_options[float(strike)]
                #     else:
                #         instrument = self.prev_call_options[float(strike)]
                
                self.orders[order['instrument_name']] = instrument
                # if order_type == 'P':
                #     if self.best_put_instr is not None:
                #         self.logger.info(f"Best Put Stike: {self.best_put_instr['strike']} bid: {self.best_put_instr['bid']}   Order Strike: {instrument['strike']} bid: {instrument['bid']}")
                #         if instrument['bid'] > self.best_put_instr['bid']:
                #             self.best_put_instr = instrument
                #     else:
                #         self.best_put_instr = instrument
                # else:
                #     if self.best_call_instr is not None:
                #         self.logger.info(f"Best Call Stike: {self.best_call_instr['strike']} bid: {self.best_call_instr['bid']}   Order Strike: {instrument['strike']} bid: {instrument['bid']}")
                #         if instrument['bid'] > self.best_call_instr['bid']:
                #             self.best_call_instr = instrument
                #     else:
                #         self.best_call_instr = instrument

                if float(strike) in self.trigger_orders:
                    self.logger.info(f'Strike {strike} found in triger_orders!')
                    self.trigger_orders[float(strike)]['order_size'] = abs(float(order['size']))
                else:
                    self.logger.info(f'Strike {strike} not found in triger_orders!')

        for order in orders_hist:
            if order['instrument_name'] == 'BTC-PERPETUAL':
                continue

            _, odate, strike, order_type  = order['instrument_name'].split('-')
            
            if odate == self.odate: # and order['instrument_name'] in self.orders:
                try: 
                    lbl_prem, _ = order['label'].split(',')

                    if float(lbl_prem) > self.max_traded_prem:
                        self.max_traded_prem = float(lbl_prem)

                except Exception as E:
                    lbl_prem = order['label']

                if order_type == 'P':
                    if lbl_prem not in self.traded_prems:
                        self.traded_prems[lbl_prem] = self.max_prem_cnt
                    else:
                        self.traded_prems[lbl_prem] += self.max_prem_cnt

        self.pos_updated = True
        self.logger.info(f'There are {len(self.orders)} open positions!')

    async def fetch_account_info(self) -> NoReturn:

        self.logger.info(f'fetch_account_info')

        async with websockets.connect(self.url) as websocket:
            await self.auth(websocket)

            await asyncio.gather(
                self.fetch_account_equity(websocket, 0.5),
                self.fetch_trigger_orders(websocket, 1),
                self.fetch_account_positions(websocket, 1.5)
            )

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

        # websocket = await websockets.connect(self.url)

        # first_run = True
        async for websocket in websockets.connect(self.url):

            await self.auth(websocket)

            # if first_run:
            #     await self.fetch_account_equity(websocket)
                # await self.fetch_account_positions(websocket)
                # await self.get_index_price(websocket)
                # first_run = False

            await websocket.send(
                self.create_message(
                    'private/subscribe',
                    { "channels": [f'deribit_price_index.{self.currency.lower()}_usd'] }
                )
            )

            self.logger.info(f'fetch_deribit_price_index: before while loop')
            await asyncio.sleep(0.5)

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
                    break
            
            if not self.keep_alive:
                break

        self.logger.info('fetch_deribit_price_index listener ended..')

    async def fetch_dvol_index(self) -> NoReturn:
        """Реализует логику работы бота"""
        self.logger.info(f'fetch_dvol_index')

        async for websocket in websockets.connect(self.url):

            await self.auth(websocket)

            await websocket.send(
                self.create_message(
                    'private/subscribe',
                    { "channels": [f'deribit_volatility_index.{self.currency.lower()}_usd'] }
                )
            )

            self.logger.info(f'fetch_dvol_index: before while loop')
            await asyncio.sleep(0.5)

            data = None
            while self.keep_alive:

                try:    
                    message = self.get_response_result(await websocket.recv(), result_prop='params')

                    if (not message is None and
                            ('channel' in message) and
                            ('data' in message)):

                        data = message['data']
                        self.dvol = data['volatility']

                        self.logger.debug(f'DVOL index: {self.dvol}')
                
                except Exception as E:
                    self.logger.info(f'Error in fetch_dvol_index: {E}')
                    self.logger.info(f'Reconnecting DVOL listener...')
                    break
            
            if not self.keep_alive:
                break

        self.logger.info('fetch_dvol_index listener ended..')

    async def fetch_orderbook_data(self, strike: str, delay: float = 0, odate: str = '') -> NoReturn:
        """Реализует логику работы бота"""
        await asyncio.sleep(delay)
        
        self.logger.info(f'fetch_orderbook_data: Listener for {strike} started..')

        # websocket = await websockets.connect(self.url)
        put_inst_name = ''
        call_inst_name = ''
        put_options = {}
        call_options = {}
        # if odate == '':
        put_inst_name = self.put_options[float(strike)]['instrument_name']
        call_inst_name = self.call_options[float(strike)]['instrument_name']
        put_options = self.put_options
        call_options = self.call_options
        # else:
        #     put_inst_name = self.prev_put_options[float(strike)]['instrument_name']
        #     call_inst_name = self.prev_call_options[float(strike)]['instrument_name']
        #     put_options = self.prev_put_options
        #     call_options = self.prev_call_options

        max_err_cnt = 2
        err_cnt = 0

        async for websocket in websockets.connect(self.url):

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
                    await asyncio.sleep(delay)
                    self.logger.info(f'Reconnecting listener for {strike}')
                    
                    err_cnt += 1
                    if err_cnt == max_err_cnt:
                        raise CBotError('Max connection error count reached!')

                    break

            if not self.keep_alive:
                break

        self.logger.info(f'fetch_orderbook_data: Listener for {strike} ended..')

    # async def prepare_prev_option_struct(self) -> NoReturn:

    #     if not self.trading: return

    #     self.logger.info(f'prepare_cont_option_struct')

    #     async with websockets.connect(self.url) as websocket:

    #         await self.auth(websocket)

    #         orders = await self.get_positions(websocket, currency=self.currency)

    #         for order in orders:
    #             _, odate, strike, order_type  = order['instrument_name'].split('-')

    #             if odate != self.odate:
    #                 if float(order['realized_profit_loss']) == 0:
    #                     instrument = await self.get_instrument(websocket, order['instrument_name'])

    #                     if order_type == 'P':
    #                         self.prev_put_options[float(strike)] = instrument
    #                     else:
    #                         self.prev_call_options[float(strike)] = instrument



    async def prepare_option_struct(self) -> NoReturn:

        daydelta = self.daydelta
        if daydelta < 1:
            raise CBotError(f'Daydelta value invalid: {daydelta}!')

        self.logger.info('prepare_option_struct')
        DAY = None

        async with websockets.connect(self.url) as websocket:
            
            await self.auth(websocket)

            if datetime.now(timezone.utc).hour < self.expire_time or self.env == 'test':
                DAY = timedelta(daydelta-1)          # 1 day option expiry
            else:
                DAY = timedelta(daydelta)          # 2 days option expiry

            expire_dt = date.today() + DAY
            self.logger.info(f'Today is {expire_dt}')
            expire_dt = expire_dt.strftime(f"{expire_dt.day}%b%y").upper()
            self.logger.info(f'Today is {expire_dt}')

            self.odate = expire_dt
            
            raw_instruments = await self.get_instruments(websocket)
            await self.get_index_price(websocket)
            
            # self.logger.info(f'Instruments: \n{raw_instruments[0]}')

            if not raw_instruments:
                self.logger.info('Raw Instruments empty!')
                return # (None, None)

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
                return #(None, None)

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

            self.call_options, self.put_options = (call_options, put_options)
            # return (call_options, put_options)

            # await self.fetch_account_positions(websocket)

    async def grace_exit(self):
        self.logger.info('grace_exit')
        async with websockets.connect(self.url) as websocket:
            await self.unsubscribe_all(websocket)