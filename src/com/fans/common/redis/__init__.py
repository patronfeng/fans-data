__author__ = 'ZENGFANGUI'
from json import  dumps,loads


def cachRequest(pool,key,expire_time,*args, **kwargs):
    dic = dict(*args, **kwargs)
    data=dumps(dict(*args, **kwargs))
    print data
    result=pool.set_data(key,data,expire_time)
    return dic



def getCachRequest(pool,key):
    cachedData = pool.get_data(key)

    if cachedData['error'] is not None or cachedData['result'] is None:
        return None
    else:
        return loads(cachedData['result'])