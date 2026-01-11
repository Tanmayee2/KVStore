import requests
from config import cfg

def send(addr, route, message):
    """POST a JSON message to addr/route. Returns the response or None on failure."""
    url = addr + '/' + route
    try:
        reply = requests.post(
            url=url,
            json=message,
            timeout=cfg.REQUESTS_TIMEOUT / 1000,
        )
    except Exception:
        return None

    if reply.status_code == 200:
        return reply
    return None