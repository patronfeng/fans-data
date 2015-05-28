#!/usr/bin/python
import tornado
import json
class JSONHandler(tornado.web.RequestHandler):
    def finish(self, chunk=None, notification=None):
        if chunk is None:
            chunk = {}
        if isinstance(chunk, dict):
            chunk = {"err":0, "data": chunk}
            if notification:
                chunk["notification"] = {"message": notification}
        callback = self.get_argument("cb", None)
        if callback:
            self.set_header("Content-Type", "application/x-javascript")
            if isinstance(chunk, dict):
                chunk = json.dumps(chunk)
            wb = callback+ "("+chunk+ ")"
            super(JSONHandler, self).write(wb)
            super(JSONHandler, self).finish()

        else:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            super(JSONHandler, self).finish(chunk)