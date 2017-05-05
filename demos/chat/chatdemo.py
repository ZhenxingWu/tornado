#!/usr/bin/env python
#
# Copyright 2009 Facebook
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import logging
import tornado.escape
import tornado.ioloop
import tornado.web
import os.path
import uuid
import requests
import time
import datetime
import boto3
from boto3.dynamodb.conditions import Key, Attr

from tornado.concurrent import Future
from tornado import gen
from tornado.options import define, options, parse_command_line

define("port", default=80, help="run on the given port", type=int)
define("debug", default=False, help="run in debug mode")


class MessageBuffer(object):
    def __init__(self):
        self.waiters = set()
        self.cache = []
        self.cache_size = 200
        self.global_timestamp =0
        self.instance_id = requests.get('http://169.254.169.254/latest/meta-data/instance-id').text
        self.az= requests.get('http://169.254.169.254/latest/meta-data/placement/availability-zone').text
        self.region= self.az[0:-1]
        self.db = boto3.resource('dynamodb', region_name=self.region).Table('chat')

    def wait_for_messages(self, cursor=None):
        # Construct a Future to return to our caller.  This allows
        # wait_for_messages to be yielded from a coroutine even though
        # it is not a coroutine itself.  We will set the result of the
        # Future when results are available.
        result_future = Future()
        if cursor:
            new_count = 0
            for msg in reversed(self.cache):
                if msg["id"] == cursor:
                    break
                new_count += 1
            if new_count:
                result_future.set_result(self.cache[-new_count:])
                return result_future
        self.waiters.add(result_future)
        return result_future

    def cancel_wait(self, future):
        self.waiters.remove(future)
        # Set an empty result to unblock any coroutines waiting.
        future.set_result([])

    def new_messages(self, messages):
        logging.info("Sending new message to %r listeners", len(self.waiters))
        for future in self.waiters:
            future.set_result(messages)
        self.waiters = set()
        for message in messages:
            self.db.put_item(Item=message)#{'instance-id':self.instance_id+messages['id'], 'message':messages})
        self.updateCache(messages)

    def updateCache(self, messages):
        self.cache.extend(messages)
        if len(self.cache) > self.cache_size:
            self.cache = self.cache[-self.cache_size:]

    def updateDB(self):
        response =self.db.scan(FilterExpression=Key('timestamp').gt(self.global_timestamp), Limit=100)
        if len(response['Items']) > 0:
            messages = []
            messages.extend(response['Items'])
            self.updateCache(messages)
            for message in messages:
                message['timestamp']=str(message['timestamp'])
            for future in self.waiters:
                future.set_result(messages)
            self.waiters = set()
            print self.global_timestamp, response['Items']
            self.global_timestamp = response['Items'][-1]['timestamp']
            
# Making this a non-singleton is left as an exercise for the reader.
global_message_buffer = MessageBuffer()

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.render("index.html", messages=global_message_buffer.cache, 
                        instance=global_message_buffer.instance_id, az=global_message_buffer.az)


class MessageNewHandler(tornado.web.RequestHandler):
    def post(self):
        global_message_buffer.global_timestamp  = int(time.mktime(datetime.datetime.utcnow().timetuple()))
        message = {
            "instance": global_message_buffer.instance_id,# +"---"+ str(uuid.uuid4()),
            "timestamp": global_message_buffer.global_timestamp,
            "id": str(uuid.uuid4()),
            "body": self.get_argument("body"),
        }
        # to_basestring is necessary for Python 3's json encoder,
        # which doesn't accept byte strings.
        message["html"] = tornado.escape.to_basestring(
            self.render_string("message.html", message=message))
        if self.get_argument("next", None):
            self.redirect(self.get_argument("next"))
        else:
            self.write(message)
        global_message_buffer.new_messages([message])


class MessageUpdatesHandler(tornado.web.RequestHandler):
    def check_origin(self, origin):  
        return True  
    @gen.coroutine
    def post(self):
        cursor = self.get_argument("cursor", None)
        # Save the future returned by wait_for_messages so we can cancel
        # it in wait_for_messages
        self.future = global_message_buffer.wait_for_messages(cursor=cursor)
        messages = yield self.future
        if self.request.connection.stream.closed():
            return
        self.write(dict(messages=messages))

    def on_connection_close(self):
        global_message_buffer.cancel_wait(self.future)



def main():
    parse_command_line()
    app = tornado.web.Application(
        [
            (r"/", MainHandler),
            (r"/a/message/new", MessageNewHandler),
            (r"/a/message/updates", MessageUpdatesHandler),
            ],
        cookie_secret="__TODO:_GENERATE_YOUR_OWN_RANDOM_VALUE_HERE__",
        template_path=os.path.join(os.path.dirname(__file__), "templates"),
        static_path=os.path.join(os.path.dirname(__file__), "static"),
        xsrf_cookies=True,
        debug=options.debug,
        )
    app.listen(options.port)
    tornado.ioloop.PeriodicCallback(global_message_buffer.updateDB, 3000).start() 
    tornado.ioloop.IOLoop.instance().start()#current().start()


if __name__ == "__main__":
    main()
