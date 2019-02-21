# coding=utf-8
# Copyright 2019 StrTrek Team Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import pandas as pd
import time
from multiprocessing import Pool, Queue, Manager


BlockSize = 4096                    # 块行数
Batches = 100                       # 批量
ChunkSize = BlockSize * Batches     # 批行数
Accuracy = 10000000000              # 精度
Processes = 8                       # 进程数
BlockingTime = 0.025                # 堵塞时间 = 子进程处理时间 / Processes
Debugging = 20                                  # 调试轮询
pd.set_option('display.max_columns', 10)        # 调试列数
pd.set_option('display.max_rows', 500)          # 调试行数
pd.set_option('display.width', 2000)            # 调试宽度


def print_run_time(func):
    def wrapper(*args, **kw):
        local_time = time.time()
        func(*args, **kw)
        print('Function [%s] run %.4f seconds.' % (func.__name__, time.time() - local_time))
    return wrapper


def get_indicator(elem):
    return elem[0]


def remove_indicator(elems):
    without_indicator = []
    for elem in elems:
        without_indicator.append(elem[1])
    return without_indicator


def product_collect(queue_products: Queue, df_stat: pd.DataFrame, cnt_products: int):
    products = [(0, df_stat)]
    while not queue_products.empty():
        indicator, chunk_stat = queue_products.get()
        products.append((indicator, pd.DataFrame(chunk_stat)))
        cnt_products = cnt_products + 1
    if len(products) > 1:
        products.sort(key=get_indicator)
        products = remove_indicator(products)
        df_stat = pd.concat(products, ignore_index=True)
    return cnt_products, df_stat


def material_prepare(queue_materials: Queue, args: tuple):
    chunk, indicator, subset = args
    while queue_materials.full():
        time.sleep(BlockingTime)
    queue_materials.put((chunk, indicator, subset))


def data_statistical(path, subset: str):
    data = pd.read_csv(path, iterator=True, dtype=str)
    loop = True
    indicator = 0
    df_stat = pd.DataFrame(columns=["start", "stop", "begin", "end", "min", "max", "mean"])
    process_pool = Pool(Processes)                   # 进程池
    queue_materials = Manager().Queue(Processes)     # 原料队列
    cnt_materials = 0                                # 发料计数
    queue_products = Manager().Queue(Processes)      # 成品队列
    cnt_products = 0                                 # 出厂计数
    while loop:
        try:
            chunk = data.get_chunk(ChunkSize)
            chunk_length = len(chunk)
            # Material prepare
            while queue_materials.full():
                time.sleep(BlockingTime)
            queue_materials.put((chunk, indicator, subset))
            process_pool.apply_async(func=chunk_statistical, args=(queue_materials, queue_products))      # 非堵塞异步
            cnt_materials = cnt_materials + 1       # 发料计数
            # Product collect
            cnt_products, df_stat = product_collect(queue_products, df_stat, cnt_products)
            # Indicator
            indicator = indicator + chunk_length
            # Debugging
            if (indicator > (ChunkSize * Debugging)) and (Debugging > 0):
                loop = False
                print("Iteration is stopped by debugging.")
        except StopIteration:
            loop = False
            print("Iteration is stopped at {0}.".format(indicator))
    # Product collect
    while cnt_products < cnt_materials:
        cnt_products, df_stat = product_collect(queue_products, df_stat, cnt_products)
    process_pool.close()
    process_pool.join()
    return {
        "DataFrame": df_stat,
        "lines": indicator,
    }


def block_statistical(block: pd.DataFrame, subset: str):
    return {
        "begin": block.iloc[0][subset],
        "end": block.iloc[-1][subset],
        "mean": int(block[subset].mean()),
        "min": int(block[subset].min()),
        "max": int(block[subset].max()),
    }


def chunk_statistical(queue_in: Queue, queue_out: Queue):
    start_time = time.time()
    chunk, indicator, subset = queue_in.get()
    # Setting accuracy
    chunk[subset] = chunk.apply(lambda x: int(float(x[subset]) * Accuracy), axis=1)
    # Statistical table
    chunk_stat = {
        "start": [],
        "stop": [],
        "begin": [],
        "end": [],
        "min": [],
        "max": [],
        "mean": [],
    }
    for b in range(0, Batches, 1):
        block_start = BlockSize * b
        block_stop = BlockSize * (b+1)
        block_stat = block_statistical(chunk[block_start:block_stop], subset)
        chunk_stat["start"].append(block_start + indicator)
        chunk_stat["stop"].append(block_stop + indicator)
        chunk_stat["begin"].append(block_stat["begin"])
        chunk_stat["end"].append(block_stat["end"])
        chunk_stat["min"].append(block_stat["min"])
        chunk_stat["max"].append(block_stat["max"])
        chunk_stat["mean"].append(block_stat["mean"])
    print("STAT {0}-{1} @ {2} seconds.".format(indicator, indicator + ChunkSize, time.time() - start_time))
    queue_out.put((indicator, chunk_stat))


@print_run_time
def main():
    train_file_path = "data/train.csv"
    dt = data_statistical(train_file_path, "time_to_failure")
    df = dt["DataFrame"]
    print(df)
    # df.to_excel("data/statistical.xlsx")


if __name__ == '__main__':
    main()

