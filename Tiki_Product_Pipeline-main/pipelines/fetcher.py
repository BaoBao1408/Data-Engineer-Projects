import asyncio
from config import API_URL

async def fetch_product(session, pid):
    try:
        async with session.get(API_URL.format(pid), timeout=15) as resp:
            if resp.status == 200:
                return {"data": await resp.json(), "error": None}
            return {"data": None, "error": f"HTTP {resp.status}"}
    except Exception as e:
        return {"data": None, "error": str(e)}

async def fetch_batch(session, ids):
    return await asyncio.gather(
        *[fetch_product(session, pid) for pid in ids]
    )
