import asyncio
import functools
import sys
import time
from asyncio import AbstractEventLoop
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Value
from typing import Dict, List

progress: Value
def init_progress(val: Value):
    global progress
    progress = val

def partition(data: List,
              chunk_size: int) -> List:
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]


def map_frequencies(chunk: List[str]) -> Dict[str, int]:
    counter = {}
    for line in chunk:
        word, _, count, _ = line.split('\t')
        if counter.get(word):
            counter[word] = counter[word] + int(count)
        else:
            counter[word] = int(count)
    with progress.get_lock():
        progress.value += 1
    return counter


def merge_dictionaries(first: Dict[str, int],
                       second: Dict[str, int]) -> Dict[str, int]:
    merged = first
    for key in second:
        if key in merged:
            merged[key] = merged[key] + second[key]
        else:
            merged[key] = second[key]
    return merged


async def reduce(loop, pool, counters, chunk_size) -> Dict[str, int]:
    chunks: List[List[Dict]] = list(partition(counters, chunk_size))
    reducers = []
    while len(chunks[0]) > 1:
        for chunk in chunks:
            reducer = functools.partial(functools.reduce,
                                        merge_dictionaries, chunk)
            reducers.append(loop.run_in_executor(pool, reducer))
        reducer_chunks = await asyncio.gather(*reducers)
        chunks = list(partition(reducer_chunks, chunk_size))
        reducers.clear()
    return chunks[0][0]

async def progress_reporter(total: int):
    while progress.value < total:
        for i in range(41):
            sys.stdout.write('\r')
            # the exact output you're looking for:
            sys.stdout.write("[%-40s] %d%%" % ('=' * i, 2.5 * i))
            sys.stdout.flush()
            await asyncio.sleep(0.25)


async def main(partition_size: int):
    global progress
    with open(file="./googlebooks-eng-all-1gram-20120701-a") as f:
        lines = f.readlines()
        loop: AbstractEventLoop = asyncio.get_event_loop()
        tasks = []
        start = time.time()

        progress = Value('i',0)
        with ProcessPoolExecutor(initializer=init_progress,initargs=(progress,)) as process_pool:
            total_partitions = len(lines) // partition_size
            progress_check = asyncio.create_task(progress_reporter(total_partitions))
            for chunk in partition(lines, partition_size):
                tasks.append(loop.run_in_executor(executor=process_pool, func=functools.partial(map_frequencies, chunk)))
            await progress_check
            from_mappers = await asyncio.gather(*tasks)
            final_result = functools.reduce(merge_dictionaries, from_mappers)
            print(f"\nAardvark has appeared {final_result['Aardvark']} times.")
            end = time.time()
            print(f'MapReduce took: {(end - start):.4f} seconds')


if __name__ == "__main__":
    asyncio.run(main(partition_size=60000))