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
Processes = 5                       # 进程数
BlockingTime = 2                    # 堵塞时间 = 子进程处理时间
Threads = 5                         # 线程数
Debugging = 10                                  # 调试轮询
pd.set_option('display.max_columns', 10)        # 调试列数
pd.set_option('display.max_rows', 500)          # 调试行数
pd.set_option('display.width', 2000)            # 调试宽度


def print_run_time(func):
    def wrapper(*args, **kw):
        local_time = time.time()
        func(*args, **kw)
        print('Function [%s] run %.2f' % (func.__name__, time.time() - local_time))
    return wrapper


def get_sequence(elem):
    return elem[0]


def remove_sequence(elems):
    without_seq = []
    for elem in elems:
        without_seq.append(elem[1])
    return without_seq


def product_collect(queue:Queue, df_stat:pd.DataFrame):
    return


def data_statistical(path, subset: str):


    data = pd.read_csv(path, iterator=True, dtype=str)
    loop = True
    indicator = 0
    df_stat = pd.DataFrame(columns=["start", "stop", "begin", "end", "min", "max", "mean"])
    process_pool = Pool(Processes)              # 进程池
    queue_material = Manager().Queue(Processes)      # 待加工队列
    queue_product = Manager().Queue(Processes)             # 输出队列
    while loop:
        try:
            chunk = data.get_chunk(ChunkSize)
            # Setting accuracy
            chunk[subset] = chunk.apply(lambda x: int(float(x[subset]) * Accuracy), axis=1)
            # Material prepare
            while queue_material.full():
                time.sleep(BlockingTime)
            queue_material.put((chunk, indicator, subset))
            process_pool.apply_async(func=chunk_statistical, args=(queue_material, queue_product))      # 非堵塞调度
            # Product collect
            product = []
            while not queue_product.empty():
                i, chunk_stat = queue_product.get()
                product.append((i, pd.DataFrame(chunk_stat)))
            product.sort(key=get_sequence)
            product = remove_sequence(product)
            if len(product) > 0:
                df_stat = pd.concat([df_stat, ].extend(product), ignore_index=True)
            else:
                pass
            # Indicator
            indicator = indicator + len(chunk)
            # Debugging
            if (indicator > (ChunkSize * Debugging)) and (Debugging > 0):
                loop = False
                print("Iteration is stopped by debugging.")
        except StopIteration:
            loop = False
            print("Iteration is stopped at {0}.".format(indicator))
            # Product collect
            product = []
            while not queue_material.empty():
                time.sleep(BlockingTime)
            while not queue_product.empty():
                i, chunk_stat = queue_product.get()
                product.append((i, pd.DataFrame(chunk_stat)))
            product.sort(key=get_sequence)
            product = remove_sequence(product)
            df_stat = pd.concat(product, ignore_index=True)
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
    chunk, indicator, subset = queue_in.get()
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
    print("STAT at {0}-{1}".format(indicator, indicator + ChunkSize))
    queue_out.put((indicator, chunk_stat))
    # return chunk_stat


@print_run_time
def main():
    train_file_path = "data/train.csv"
    dt = data_statistical(train_file_path, "time_to_failure")
    df = dt["DataFrame"]
    # print(df)
    # df.to_excel("data/statistical.xlsx")


if __name__ == '__main__':
    main()

