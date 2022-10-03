import numpy as np
import pandas as pd

# x = np.random.rand(10,2)
# x = pd.DataFrame(x)

# list(map(print, np.array2string(x, separator=',')))
# print(np.array2string(x, separator=','))

# x = x.apply(lambda row: ",".join(row.to_string(header=False, index=False, name=False).split('\n')), axis=1)
# list(map(print, x))

# print(x.values)

# x = [['Hello', 'World', 12.9], ['Hello', 'World', 99.9]]
# x = pd.DataFrame(x)


# print(x.iloc[x[2].values.argmax()])

# x = np.array(x).astype(str)

# print(",".join(x))

# from datetime import date

# expire_dt = date.today()
# # expire_dt = expire_dt.strftime("%-d%b%y").upper()
# expire_dt = expire_dt.strftime(f"{expire_dt.day}%b%y").upper()
# print(f'Today is {expire_dt}')

import asyncio
import time

updated = {
    'flag': False
}

async def count():
    for i in range(100):
        if i == 50: updated['flag'] = True
        print(f'count: {i}')


async def check():
    while not updated['flag']:
        pass

    print('check completed')

async def start():
    print('start')
    await check()
    print('end')

async def main():
    await asyncio.gather(start(), count())


s = time.perf_counter()
asyncio.run(main())
elapsed = time.perf_counter() - s
print(f"{__file__} executed in {elapsed:0.2f} seconds.")