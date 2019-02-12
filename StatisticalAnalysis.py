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

ChunkSize = 4096
Steps = 10


train_file_path = "data/train.csv"
train_data = pd.read_csv(train_file_path, iterator=True, dtype=str)
loop = True
chunks = {
    "start": [0, ],
    "end": [0, ],
    "min": [0, ],
    "min_delta": [0, ],
}
indicator = 0
while loop:
    try:
        # Title shifting
        if indicator == 0:
            chunk = train_data.get_chunk(ChunkSize * Steps + 1)
        else:
            chunk = train_data.get_chunk(ChunkSize * Steps)
        # Time_to_failure
        chunk["time"] = chunk.apply(lambda x: int(float(x["time_to_failure"]) * 10000000000), axis=1)
        # Chunk Statistical
        for j in range(0, Steps, 1):
            block_start = ChunkSize * j
            block_end = (ChunkSize * (j+1))
            block = chunk[block_start:block_end]
            print(block)
            block_min = int(block["time"].min())
            block_min_delta = abs(chunks["min"][-1] - block_min)
            chunks["start"].append(block_start + indicator)
            chunks["end"].append(block_end + indicator)
            chunks["min_delta"].append(block_min_delta)
            chunks["min"].append(block_min)
            # print("START:{0} END:{1} MIN:{2}|{3}".format(block_start, block_end-1, block_min, block_min_delta))
        # Indicator
        indicator = indicator + (ChunkSize * Steps)
        # Debugging
        if indicator > (ChunkSize * Steps * 3):
            loop = False
            print("Iteration is stopped by debugging.")
    except StopIteration:
        loop = False
        print("Iteration is stopped.")
# print(chunks)
df = pd.DataFrame(chunks)
print(df)
