import redis

class RedisDBConfig:
    HOST = '127.0.0.1'
    PORT = 6379
    DBID = 0

def operator_status(func):
    '''get operatoration status
    '''
    def gen_status(*args, **kwargs):
        error, result = None, None
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            error = str(e)

        return {'result': result, 'error':  error}

    return gen_status

class RedisCache(object):

    def __init__(self,host=RedisDBConfig.HOST,port=RedisDBConfig.PORT):

        if not hasattr(RedisCache, 'pool'):
            RedisCache.create_pool(host,port)
        self._connection = redis.Redis(connection_pool = RedisCache.pool)

    @staticmethod
    def create_pool(host,port):
        RedisCache.pool = redis.ConnectionPool(
            host = host,
            port = port,
            db   = RedisDBConfig.DBID)

    @operator_status
    def set_data(self, key, value,ex=None, px=None, nx=False, xx=False):
        '''set data with (key, value)
        '''
        return self._connection.set(key, value,ex,px,nx,xx)

    @operator_status
    def get_data(self, key):
        '''get data by key
        '''
        return self._connection.get(key)

    @operator_status
    def del_data(self, key):
        '''delete cache by key
        '''
        return self._connection.delete(key)


if __name__ == '__main__':
    import config
    pool = RedisCache(config.redis_host,config.redis_port)
    print pool.set_data('Testkey', "Simple Test")
    print pool.get_data('Testkey')
    print pool.del_data('Testkey')
    print pool.get_data('Testkey')