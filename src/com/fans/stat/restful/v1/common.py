__author__ = 'ZENGFANGUI'
from src.com.fans.common.redis.RedisConnection import RedisCache
import  pyes
from config import  *
es_connection=pyes.ES(es_host)
redispool = RedisCache(redis_host,redis_port)