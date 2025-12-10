# discord_utils.py
import os
import logging
import aiohttp

# Có thể hard-code webhook hoặc lấy từ ENV (gọn & bảo mật hơn)
DISCORD_WEBHOOK_URL = os.getenv(
    "DISCORD_WEBHOOK_URL",
    "https://discord.com/api/webhooks/1448173231675543553/n9_VKPxoJzedBQNAbzRwUclQqpiCxySaGV_bSqUE_8JkperxWhhvXek22Tb2cgaTaIRj"
)

async def send_discord_alert(message: str) -> None:
    """
    Gửi 1 message đơn giản vào Discord bằng webhook.
    Dùng trong cả crawler và script test.
    """
    if not DISCORD_WEBHOOK_URL:
        logging.warning("DISCORD_WEBHOOK_URL is empty, skip sending alert")
        return

    try:
        async with aiohttp.ClientSession() as session:
            resp = await session.post(DISCORD_WEBHOOK_URL, json={"content": message})

            # Discord webhook OK thường trả 204 No Content
            if resp.status not in (200, 204):
                text = await resp.text()
                logging.warning(
                    "Discord webhook returned %s: %s",
                    resp.status,
                    text[:200]
                )
            else:
                logging.info("Discord alert sent successfully")

    except Exception as e:
        logging.warning("Failed to send discord alert: %s", e)
