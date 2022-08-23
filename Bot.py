import json
import time
import asyncio
import logging
from typing import Union, Optional, NoReturn
import websockets
# from .exceptions import CBotResponseError , CBotError
# from .order import COrder
# from .interface import IBotStore

class CBot:
    def __init__(self, url, auth: dict, instrument: dict,
                logger: Union[logging.Logger, str, None] = None,
                store: Optional[IBotStore] = None,
                order_label: str = 'dsb'):

        self.store = store
        self.url = url
        self.order_label = order_label
        self.instrument = {}
        self.order = {}
        self.__credentials = auth
        self.logger = (logging.getLogger(logger) if isinstance(logger,str) else logger)

        if self.logger is None:
            self.logger = logging.getLogger(__name__)

        _d = instrument['default']

        for i in instrument:

            _i = i.upper()

            if _i == 'DEFAULT':
                continue

            self.instrument[_i] = _d if instrument[i] is None else { **_d, **instrument[i] }

        self.logger.debug(f'Bot init instrument = {self.instrument}')


    def create_message(self, method: str, params: dict = {},
                        mess_id: Union[int, str, None] = None,
                        as_dict: bool = False) -> Union[str, dict] :
        """Метод возвращает объект или строку дамп JSON с телом для запроса на API биржи"""
        obj = {
            "jsonrpc" : "2.0",
            "id" : ( str(time.time()).replace('.','_') if mess_id is None else mess_id),
            "method" : method,
            "params" : params
        }

        self.logger.debug(f'Creat message = {obj}')

        return obj if as_dict else json.dumps(obj)


    def get_response_result(self, raw_response: str, raise_error: bool = True,
                            result_prop: str = 'result') -> Optional[dict]:
        """Получает тело ответа от сервера, после чего возвращает объект
        находящийся в result_prop, либо бросает исключение, если с сервера
        пришли данные об ошибке
        """

        obj = json.loads(raw_response)

        self.logger.debug(f'Get response = {obj}')

        if result_prop in obj:
            return obj[result_prop]

        # if 'error' in obj and raise_error:
        #     raise CBotResponseError(obj['error']['message'],obj['error']['code'])

        return None


    async def auth(self, ws) -> Optional[dict]:

        await ws.send(
            self.create_message(
                'public/auth',
                self.__credentials
            )
        )

        return self.get_response_result(await ws.recv())


    async def create_order(self, ws, direction: str, instrument: str,
                            price: float, amount: float, order_prop: dict = {},
                            raise_error: bool = True) -> COrder:

        prop = {
            'instrument_name' : instrument,
            'amount' : amount,
            'type' : 'limit',
            'price' : price,
            'advanced': 'usd'
        }

        await ws.send(
            self.create_message(
                f'private/{direction}',
                { **prop , **order_prop }
            )
        )

        order_res = self.get_response_result(await ws.recv(), raise_error = raise_error)

        if not self.store is None and not order_res is None:
            self.store.insert(order_res['order'])

        return COrder(order = (None if order_res is None else order_res['order']))


    async def cancel_order(self, ws, order_id:Union[int, str, None], active: bool = False,
                            raise_error: bool = True) -> COrder:

        await ws.send(
            self.create_message(
                f'private/cancel',
                { 'order_id': order_id }
            )
        )

        order_res = self.get_response_result(await ws.recv(), raise_error = raise_error)

        if not self.store is None:
            self.store.update(
                order_id = order_id,
                order = order_res,
                other_param = ({} if active else {
                                                    'active' : 0,
                                                    'active_comment' : 'Order cancelled'
                                                }),
                return_is_active = False
            )

        return COrder(order_res)


    async def get_order_state(self, ws, order_id: Union[int, str],
                                raise_error: bool = True) -> Optional[COrder]:

        await ws.send(
            self.create_message(
                f'private/get_order_state',
                { 'order_id': order_id }
            )
        )

        return COrder(self.get_response_result(await ws.recv(), raise_error = raise_error))


    async def worker(self, instrument: str, gap: float, gap_ignore: float, amount: float,
                    price_id: str = 'mark_price', order: COrder = None,
                    prev_order: COrder = None) -> NoReturn:
        """Реализует логику работы бота"""

        if order is None:
            order = COrder()

        if prev_order is None:
            prev_order = COrder()

        group_id = (order.label
                        if not order.label is None
                        else f'{self.order_label}.{instrument}.{time.time()}')

        is_change_order = False

        self.logger.info(f'{instrument} Start worker order = {order.id} ,\
            prev_order = {prev_order.id}, group_id = {group_id}')

        async with websockets.connect(self.url) as websocket:

            await self.auth(websocket)

            await websocket.send(
                self.create_message(
                    'private/subscribe',
                    {
                        "channels": [
                            f'ticker.{instrument}.raw',
                            f'user.orders.{instrument}.raw'
                        ]
                    }
                )
            )

            while websocket.open:

                # Учёт необходимости обновления данных об ордере
                if is_change_order and order.isset and not self.store is None:
                    self.store.update(order.id, order.source)

                if order.state in ['rejected', 'cancelled', 'untriggered']:
                    self.logger.info(f'{instrument} reset order = {order.id} , \
                        state = {order.state}')
                    order.reset()

                # Получаем сообщение от сервера
                message = self.get_response_result(await websocket.recv(), result_prop='params')
                data = None
                is_change_order = False

                if (
                        not message is None and
                        ('channel' in message) and
                        ('data' in message)
                ):
                    data = message['data']
                else:
                    continue

                # Обработка сообщений приходящих от тикера
                if message['channel'].startswith('ticker') :

                    price = data[price_id]

                    if order.isset is False :

                        order = await self.create_order(
                            ws = websocket,
                            direction = 'buy',
                            instrument = instrument,
                            price = (price - gap / 2),
                            amount = amount,
                            order_prop = { 'trigger' : price_id, 'label' : group_id }
                        )

                        self.logger.info(f'{instrument} create {order.direction} \
                            order = {order.id}')

                    elif order.direction == 'buy':

                        if order.state == 'filled':

                            prev_order = order

                            order = await self.create_order(
                                ws = websocket,
                                direction = 'sell',
                                instrument = instrument,
                                price = (price + gap),
                                amount = order.amount,
                                order_prop = { 'trigger' : price_id, 'label' : group_id }
                            )

                            self.logger.info(f'{instrument} create {order.direction} \
                                order = {order.id}, price = {price}')

                        elif price > order.price + gap + gap_ignore :

                            await self.cancel_order(ws = websocket, order_id = order.id)

                            self.logger.info(f'{instrument} cancel {order.direction} \
                                order = {order.id}, \
                                price = {price}, \
                                prev_order = {prev_order.id}')

                            order = prev_order

                    elif order.direction == 'sell':

                        if order.state == 'filled':

                            prev_order = order

                            order = await self.create_order(
                                ws = websocket,
                                direction = 'buy',
                                instrument = instrument,
                                price = (price - gap / 2),
                                amount = order.amount,
                                order_prop = { 'trigger' : price_id, 'label' : group_id }
                            )

                            self.logger.info(f'{instrument} create {order.direction} \
                                order = {order.id}, price = {price}')

                        elif price < order.price - gap - gap_ignore :

                            await self.cancel_order(ws = websocket, order_id = order.id)

                            self.logger.info(f'{instrument} cancel {order.direction} \
                                order = {order.id}, \
                                price = {price}, \
                                prev_order = {prev_order.id}')

                            order = prev_order

                # Обработка сообщений об изменении ордеров
                elif message['channel'].startswith('user.orders') :

                    if data['last_update_timestamp'] == data['creation_timestamp']:
                        continue

                    is_change_order = True

                    if data['order_id'] == order.id:
                        self.logger.info(f'{instrument} update {order.id}')
                        order.set_source(data)


    async def synch(self, mod: int = 1, get_actual: list[str] = []) -> dict:
        """Функция проводит синхронизацию ордеров из хранилища с биржей.

            - get_actual -  список инструментов, актуальных для текущего запуска,
                            если он указан то:
                            по каждому из инструментов будут найдены актуальные
                            ордера во внутреннем хранилище.
                            После каждый из ордеров будет синхронизирован с биржей

            - mod -         режим синхронизации - указывает как надо проводить
                            синхронизацию тех ордеров у которых active = 1 (т.е.
                            управляемые ботом).

                            mod = 0 - ничего не делать

                            mod = 1 - все ордера с active = 1 (кроме тех, которые
                            актуальны для тех инструментов, которые указаны в
                            get_actual) имеющие статус Open на бирже - будут
                            принудительно закрыты. active примет значение 0

                            mod не 0 и не 1 - все ордера с active = 1 (кроме тех,
                            которые актуальны для тех инструментов, которые указаны в
                            get_actual) получат active = 0, без доп.запросов на биржу
        """

        result = {}
        ignore_id = []

        self.logger.info(f'Start synch mod = {mod}, get_actual = {get_actual}')

        async with websockets.connect(self.url) as websocket:

            await self.auth(websocket)

            if len(get_actual):

                orders = self.store.get(
                    param = {
                        'active' : 1 ,
                        'instrument' : get_actual,
                        'state': ['open','filled']
                    },
                    order_by= { 'real_create' : 'desc' }
                )

                for order in orders:

                    _i = order['instrument']

                    if (
                            (_i in result) and
                            (
                                (len(result[_i]) == 2) or
                                (
                                    len(result[_i]) == 1 and
                                    (
                                        result[_i][0].state == 'filled' or
                                        result[_i][0].label!=order['group_id']
                                    )
                                )
                            )
                    ):
                        continue

                    src_order = await self.get_order_state(
                        ws = websocket,
                        order_id = order['id'],
                        raise_error = False
                    )

                    if src_order.state in [None, 'rejected', 'cancelled', 'untriggered']:
                        continue

                    self.store.update(order['id'], src_order.source)

                    if not _i in result:
                        result[_i] = []

                    result[_i].append(src_order)

                    ignore_id.append(order['id'])


            if mod>0 :

                other_param={ 'active' : 0 ,'active_comment' : 'Synch' }

                param = { 'active' : 1 }
                if len(ignore_id):
                    param['id'] = { 'operation': 'not in', 'value': ignore_id }

                orders = self.store.get(param = param)

                for order in orders:

                    if mod == 1:
                        if not order['state'] in ['open', 'filled']:
                            self.store.update(order['id'], other_param = other_param)
                        else:
                            src_order = await self.get_order_state(
                                ws = websocket,
                                order_id = order['id'],
                                raise_error = False
                            )

                            if src_order.state == 'open':
                                await self.cancel_order(
                                    ws = websocket,
                                    order_id = order['id'],
                                    raise_error = False
                                )
                            else:
                                self.store.update(
                                    order['id'],
                                    order = src_order.source,
                                    other_param = other_param
                                )
                    else:
                        self.store.update(order['id'], other_param = other_param)

        return result


    async def start(self, synch_mod: int = 1, synch_actual: bool = True) -> NoReturn:
        """Стартует работу бота с указанием параметров для синхронизации.
        Синхронизация будет проводится только в случае если задано хранилище (store)"""

        self.logger.info('Bot start')

        order = {}
        task = []

        if not self.store is None:
            order = await self.synch(
                mod = synch_mod,
                get_actual = ( list(self.instrument.keys()) if synch_actual else [] )
            )

        for i in self.instrument:

            _order = None
            _prev_order = None

            if i in order:
                _order = order[i][0]
                if len(order[i]) == 2:
                    _prev_order = order[i][1]

            task.append(
                asyncio.create_task(
                    self.worker(
                        instrument = i,
                        order = _order,
                        prev_order = _prev_order,
                        **self.instrument[i]
                    )
                )
            )

        await asyncio.gather(*task)


    def run(self, synch_mod: int = 1, synch_actual: bool = True) -> NoReturn:
        """Объёртка для start для запуска без доп библиотек для управления асинхроном"""
        asyncio.get_event_loop().run_until_complete(
            self.start(
                synch_mod = synch_mod,
                synch_actual = synch_actual
            )
        )
