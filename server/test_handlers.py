import pytest
from server.main import create_app
from server import handlers


@pytest.fixture
async def client(aiohttp_client):
    app = create_app()
    return await aiohttp_client(app)


@pytest.fixture(autouse=True)
def reset_state():
    handlers.channel_consumers.clear()
    handlers.channel_producers.clear()
    yield
    handlers.channel_consumers.clear()
    handlers.channel_producers.clear()


class TestChannelsEndpoint:
    async def test_list_channels_empty(self, client):
        resp = await client.get('/channels')
        assert resp.status == 200
        data = await resp.json()
        assert data == {'channels': []}

    async def test_list_channels_with_producer(self, client):
        ws = await client.ws_connect('/channel/test-channel/produce')
        resp = await client.get('/channels')
        assert resp.status == 200
        data = await resp.json()
        assert 'test-channel' in data['channels']
        await ws.close()

    async def test_list_channels_with_consumer(self, client):
        ws = await client.ws_connect('/channel/consumer-channel/consume')
        resp = await client.get('/channels')
        data = await resp.json()
        assert 'consumer-channel' in data['channels']
        await ws.close()


class TestMetricsEndpoint:
    async def test_metrics_empty(self, client):
        resp = await client.get('/metrics')
        assert resp.status == 200
        data = await resp.json()
        assert data['active_channels'] == 0
        assert data['total_consumers'] == 0
        assert data['total_producers'] == 0

    async def test_metrics_with_connections(self, client):
        producer = await client.ws_connect('/channel/metrics-test/produce')
        consumer = await client.ws_connect('/channel/metrics-test/consume')
        resp = await client.get('/metrics')
        data = await resp.json()
        assert data['active_channels'] == 1
        assert data['total_consumers'] == 1
        assert data['total_producers'] == 1
        await producer.close()
        await consumer.close()


class TestWebSocketProducer:
    async def test_producer_connects(self, client):
        ws = await client.ws_connect('/channel/producer-test/produce')
        assert not ws.closed
        await ws.close()

    async def test_producer_disconnect_cleanup(self, client):
        ws = await client.ws_connect('/channel/cleanup-test/produce')
        assert 'cleanup-test' in handlers.channel_producers
        await ws.close()
        assert 'cleanup-test' not in handlers.channel_producers


class TestWebSocketConsumer:
    async def test_consumer_connects(self, client):
        ws = await client.ws_connect('/channel/consumer-test/consume')
        assert not ws.closed
        await ws.close()

    async def test_consumer_disconnect_cleanup(self, client):
        ws = await client.ws_connect('/channel/cleanup-test/consume')
        assert 'cleanup-test' in handlers.channel_consumers
        await ws.close()
        assert 'cleanup-test' not in handlers.channel_consumers


class TestAudioStreaming:
    async def test_producer_sends_to_consumer(self, client):
        producer = await client.ws_connect('/channel/stream-test/produce')
        consumer = await client.ws_connect('/channel/stream-test/consume')
        
        test_data = b'\x00\x01\x02\x03\x04\x05\x06\x07'
        await producer.send_bytes(test_data)
        
        msg = await consumer.receive()
        assert msg.data == test_data
        
        await producer.close()
        await consumer.close()

    async def test_producer_broadcasts_to_multiple_consumers(self, client):
        producer = await client.ws_connect('/channel/broadcast-test/produce')
        consumer1 = await client.ws_connect('/channel/broadcast-test/consume')
        consumer2 = await client.ws_connect('/channel/broadcast-test/consume')
        
        test_data = b'broadcast-data'
        await producer.send_bytes(test_data)
        
        msg1 = await consumer1.receive()
        msg2 = await consumer2.receive()
        assert msg1.data == test_data
        assert msg2.data == test_data
        
        await producer.close()
        await consumer1.close()
        await consumer2.close()

    async def test_multiple_messages(self, client):
        producer = await client.ws_connect('/channel/multi-msg/produce')
        consumer = await client.ws_connect('/channel/multi-msg/consume')
        
        for i in range(5):
            data = bytes([i] * 10)
            await producer.send_bytes(data)
            msg = await consumer.receive()
            assert msg.data == data
        
        await producer.close()
        await consumer.close()

    async def test_no_consumers_producer_still_works(self, client):
        producer = await client.ws_connect('/channel/no-consumers/produce')
        await producer.send_bytes(b'test')
        await producer.close()

    async def test_consumer_without_producer(self, client):
        consumer = await client.ws_connect('/channel/no-producer/consume')
        assert not consumer.closed
        await consumer.close()

