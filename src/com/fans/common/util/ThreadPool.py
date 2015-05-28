__author__ = 'ZENGFANGUI'
from concurrent import  futures

EXECUTOR = futures.ThreadPoolExecutor(30)



def run_async(func, *args, **kwargs):
    """
    :type handler: tornado.web.RequestHandler
    """
    return EXECUTOR.submit(func, *args, **kwargs)