# import asyncio
# import aiohttp
# from concurrent.futures import ThreadPoolExecutor
# from tqdm import tqdm

# from config import *
# from pipelines.loader import load_product_ids
# from pipelines.fetcher import fetch_batch
# from pipelines.transformer import transform
# from pipelines.writer import write_batch
# from pipelines.checkpoint import load_checkpoint, save_checkpoint
# from pipelines.monitor import record_batch, summary, send_alert
# from pipelines.error_handler import log_failed_id

# async def main():
#     try:
#         product_ids = load_product_ids(CSV_FILE)
#         checkpoint = load_checkpoint()
#         start_batch = checkpoint["last_batch"] + 1

#         connector = aiohttp.TCPConnector(limit=MAX_CONNECTIONS)
#         async with aiohttp.ClientSession(connector=connector) as session:
#             batch_index = 1

#             for i in tqdm(range(0, len(product_ids), BATCH_SIZE)):
#                 if batch_index < start_batch:
#                     batch_index += 1
#                     continue

#                 batch_ids = product_ids[i:i + BATCH_SIZE]
#                 raw = await fetch_batch(session, batch_ids)

#                 loop = asyncio.get_event_loop()
#                 with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#                     processed = await asyncio.gather(
#                         *[loop.run_in_executor(executor, transform, d) for d in raw]
#                     )

#                 processed = [p for p in processed if p]
#                 write_batch(processed, batch_index, OUTPUT_DIR)

#                 record_batch(len(processed))
#                 save_checkpoint(batch_index)

#                 print(f"[Batch {batch_index}] Collected: {len(processed)}")
#                 batch_index += 1

#         total, duration = summary()
#         print(f"TOTAL COLLECTED: {total}")
#         print(f"TIME (seconds): {duration:.2f}")

#     except Exception as e:
#         send_alert(f"PIPELINE CRASHED\nError: {str(e)}")
#         raise

# if __name__ == "__main__":
#     asyncio.run(main())

#----------------------------------------------------------------------------------------------------------------------------------------------------------

import asyncio
import aiohttp
import sys
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

from config import *
from pipelines.loader import load_product_ids
from pipelines.fetcher import fetch_batch
from pipelines.transformer import transform
from pipelines.writer import write_batch
from pipelines.checkpoint import load_checkpoint, save_checkpoint
from pipelines.monitor import record_batch, summary, send_alert
from pipelines.error_handler import log_failed_id
from pipelines.reset import reset_pipeline

async def main():
    if "--reset" in sys.argv:
        print("RESET MODE ENABLED")
        reset_pipeline()

    try:
        # Load & resume
        product_ids = load_product_ids(CSV_FILE)
        checkpoint = load_checkpoint()
        start_batch = checkpoint["last_batch"] + 1

        connector = aiohttp.TCPConnector(limit=MAX_CONNECTIONS)
        async with aiohttp.ClientSession(connector=connector) as session:

            batch_index = 1

            for i in tqdm(range(0, len(product_ids), BATCH_SIZE)):

                # Resume logic
                if batch_index < start_batch:
                    batch_index += 1
                    continue

                batch_ids = product_ids[i:i + BATCH_SIZE]

                # FETCH
                results = await fetch_batch(session, batch_ids)

                valid_data = []

                for pid, result in zip(batch_ids, results):
                    if result["error"]:
                        log_failed_id(
                            product_id=pid,
                            batch_index=batch_index,
                            stage="fetch",
                            error=result["error"]
                        )
                    else:
                        valid_data.append(result["data"])

                # TRANSFORM (CPU-bound)
                loop = asyncio.get_event_loop()
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    transformed = await asyncio.gather(
                        *[loop.run_in_executor(executor, transform, d) for d in valid_data]
                    )

                transformed = [t for t in transformed if t]

                # WRITE
                write_batch(transformed, batch_index, OUTPUT_DIR)

                # MONITOR + CHECKPOINT
                record_batch(len(transformed))
                save_checkpoint(batch_index)

                print(f"[Batch {batch_index}] Collected: {len(transformed)}")

                batch_index += 1

        # SUMMARY
        total, duration = summary()
        print(f"\nTOTAL COLLECTED: {total}")
        print(f"TIME (seconds): {duration:.2f}")

    except Exception as e:
        send_alert(f"PIPELINE CRASHED\nError: {str(e)}")
        raise


if __name__ == "__main__":
    asyncio.run(main())

