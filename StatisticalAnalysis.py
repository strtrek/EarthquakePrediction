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

BlockSize = 4096                    # 块行数
Batches = 100                       # 批量
ChunkSize = BlockSize * Batches     # 批行数
Accuracy = 10000000000              # 精度
Debugging = 0                       # 调试轮询
pd.set_option('display.max_columns', 10)       # 调试列数
pd.set_option('display.max_rows', 500)         # 调试行数
pd.set_option('display.width', 2000)           # 调试宽度


def data_statistical(path, subset: str):
    data = pd.read_csv(path, iterator=True, dtype=str)
    loop = True
    indicator = 0
    df_stat = pd.DataFrame(columns=["start", "stop", "begin", "end", "min", "max", "mean"])
    while loop:
        try:
            chunk = data.get_chunk(ChunkSize)
            # Setting accuracy
            chunk[subset] = chunk.apply(lambda x: int(float(x[subset]) * Accuracy), axis=1)
            chunk_stat = chunk_statistical(chunk, indicator, subset)
            df_stat = pd.concat([df_stat, pd.DataFrame(chunk_stat)], ignore_index=True)
            # Indicator
            indicator = indicator + len(chunk)
            # Debugging
            if (indicator > (ChunkSize * Debugging)) and (Debugging > 0):
                loop = False
                print("Iteration is stopped by debugging.")
        except StopIteration:
            loop = False
            print("Iteration is stopped at {0}.".format(indicator))
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


def chunk_statistical(chunk: pd.DataFrame, indicator: int, subset: str):
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
    return chunk_stat


def main():
    train_file_path = "data/train.csv"
    dt = data_statistical(train_file_path, "time_to_failure")
    df = dt["DataFrame"]
    df.to_excel("data/statistical.xlsx")


if __name__ == '__main__':
    main()

