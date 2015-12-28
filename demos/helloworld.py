#!/usr/bin/env python

import tornado.httpserver
import tornado.ioloop
import tornado.web

from tornado import gen
import totoro
from totoro.test.celery_tasks import tasks


class MainHandler(tornado.web.RequestHandler):
    @gen.coroutine
    def get(self):
        response = yield gen.Task(tasks.echo.apply_async, args=['Hello world!'])
        self.write(response.result)


def main():
    totoro.setup_producer()
    application = tornado.web.Application([
        (r"/", MainHandler),
    ])
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(8888)
    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()
