import asyncio
import aiohttp
import aiofiles
import csv
import os
from tqdm import tqdm

INPUT_FILE = "data/raw/product_urls.csv"
OUTPUT_FILE = "data/processed/active_product_urls.csv"
CHECKPOINT_FILE = "data/processed/checkpoint.txt"

CONCURRENT_REQUESTS = 100
TIMEOUT = 10


async def fetch(session, url):

    retries = 3

    for _ in range(retries):

        try:

            async with session.get(url, timeout=TIMEOUT) as response:

                if response.status == 200:
                    return True

                if response.status in [403, 404]:
                    return False

        except:
            await asyncio.sleep(1)

    return False


async def worker(queue, session, writer, checkpoint):

    while True:

        item = await queue.get()

        if item is None:
            break

        index, url = item

        active = await fetch(session, url)

        if active:
            writer.writerow([url])

        if index % 1000 == 0:
            checkpoint.write(str(index))
            checkpoint.flush()

        queue.task_done()


async def main():

    os.makedirs("data/processed", exist_ok=True)

    urls = []

    with open(INPUT_FILE, encoding="utf-8") as f:

        reader = csv.reader(f)
        next(reader)

        for row in reader:
            urls.append(row[0])

    start_index = 0

    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE) as f:
            start_index = int(f.read())

    queue = asyncio.Queue()

    for i, url in enumerate(urls[start_index:], start=start_index):
        queue.put_nowait((i, url))

    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS)

    timeout = aiohttp.ClientTimeout(total=TIMEOUT)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:

        with open(OUTPUT_FILE, "a", newline="", encoding="utf8") as f, open(
            CHECKPOINT_FILE, "w"
        ) as checkpoint:

            writer = csv.writer(f)

            workers = []

            for _ in range(CONCURRENT_REQUESTS):

                workers.append(
                    asyncio.create_task(worker(queue, session, writer, checkpoint))
                )

            progress = tqdm(total=len(urls))

            while not queue.empty():

                progress.update(1)
                await asyncio.sleep(0.01)

            await queue.join()

            for _ in workers:
                queue.put_nowait(None)

            await asyncio.gather(*workers)


if __name__ == "__main__":
    asyncio.run(main())