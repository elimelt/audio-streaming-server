import logging
from aiohttp import web
import aiohttp
import aiohttp_jinja2

logger = logging.getLogger(__name__)

channel_consumers: dict[str, set[web.WebSocketResponse]] = {}
channel_producers: dict[str, set[web.WebSocketResponse]] = {}

def get_active_channels():
    return set(channel_consumers.keys()) | set(channel_producers.keys())

@aiohttp_jinja2.template('index.html')
async def index(request):
    channel_id = request.match_info['channel_id']
    logger.info(f"Serving index page for channel {channel_id}")
    return {'channel_id': channel_id}

async def consume(request):
    channel_id = request.match_info['channel_id']
    ip = request.remote
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    if channel_id not in channel_consumers:
        channel_consumers[channel_id] = set()
    channel_consumers[channel_id].add(ws)
    logger.info(f"Consumer connected to channel {channel_id} from {ip}")

    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.ERROR:
                logger.error(f'WebSocket error from consumer {ip}: {ws.exception()}')
    finally:
        if channel_id in channel_consumers:
            channel_consumers[channel_id].discard(ws)
            if not channel_consumers[channel_id]:
                del channel_consumers[channel_id]
        logger.info(f"Consumer disconnected from channel {channel_id} from {ip}")
    return ws

async def produce(request):
    channel_id = request.match_info['channel_id']
    ip = request.remote
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    if channel_id not in channel_producers:
        channel_producers[channel_id] = set()
    channel_producers[channel_id].add(ws)
    logger.info(f"Producer connected to channel {channel_id} from {ip}")

    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.BINARY:
                consumers = list(channel_consumers.get(channel_id, set()))
                dead_consumers = []
                for consumer in consumers:
                    if consumer.closed:
                        dead_consumers.append(consumer)
                        continue
                    try:
                        await consumer.send_bytes(msg.data)
                    except ConnectionResetError:
                        dead_consumers.append(consumer)
                for dead in dead_consumers:
                    channel_consumers.get(channel_id, set()).discard(dead)
            elif msg.type == aiohttp.WSMsgType.ERROR:
                logger.error(f'WebSocket error from producer {ip}: {ws.exception()}')
    finally:
        if channel_id in channel_producers:
            channel_producers[channel_id].discard(ws)
            if not channel_producers[channel_id]:
                del channel_producers[channel_id]
        logger.info(f"Producer disconnected from channel {channel_id} from {ip}")
    return ws

async def list_channels(request):
    return web.json_response({'channels': list(get_active_channels())})

async def metrics(request):
    return web.json_response({
        'active_channels': len(get_active_channels()),
        'total_consumers': sum(len(c) for c in channel_consumers.values()),
        'total_producers': sum(len(p) for p in channel_producers.values())
    })
