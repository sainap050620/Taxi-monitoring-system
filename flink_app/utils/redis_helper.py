import redis


class RedisHelper:
    def __init__(self, host='redis', port=6379):
        self.client = redis.Redis(host=host, port=port, decode_responses=True)

    def set(self, key, value):
        self.client.set(key, value)

    def get(self, key):
        return self.client.get(key)
