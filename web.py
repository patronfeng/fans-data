__author__ = 'ZENGFANGUI'
import  tornado.httpserver
from tornroutes import route
from tornado.options import define, options
import src.com.fans.stat.restful.v1.circle
import src.com.fans.stat.restful.v1.topic

import sys,os
from tornroutes import route
# make sure we get our local tornroutes before anything else
sys.path = [os.path.abspath(os.path.dirname(__file__))] + sys.path
define("port", default=8010, help="run on the given port", type=int)
if __name__ == "__main__":
    tornado.options.parse_command_line()
    for item in route.get_routes():
        print item
    app = tornado.web.Application(route.get_routes())
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()