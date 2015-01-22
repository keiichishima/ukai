# Copyright 2014, 2015
# IIJ Innovation Institute Inc. All rights reserved.
# 
# Redistribution and use in source and binary forms, with or
# without modification, are permitted provided that the following
# conditions are met:
# 
# * Redistributions of source code must retain the above copyright
#   notice, this list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above
#   copyright notice, this list of conditions and the following
#   disclaimer in the documentation and/or other materials
#   provided with the distribution.
# 
# THIS SOFTWARE IS PROVIDED BY IIJ INNOVATION INSTITUTE INC. ``AS
# IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
# SHALL IIJ INNOVATION INSTITUTE INC. OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
# OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
# TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
# OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
# OF SUCH DAMAGE.

import socket
import xmlrpclib

class UKAIRPCConnection(object):
    def __init__(self):
        '''Initializes connection pool

        '''
        self._servers = {}

    def call(self, server, port, method, *params):
        '''Gets an existing RPC connection from the connection pool and make
        an RPC call.  The subclass must implement the _call(),
        _get_conn(), and _put_conn() inner methods.

        '''
        conn = self._get_conn(server, port)
        ret = None
        try:
            ret = self._call(conn, method, *params)
        except socket.error, e:
            print e[0]
            del conn
            raise
        else:
            self._put_conn(server, port, conn)

        return ret

    def encode(self, source):
        '''Returns RPC dependent encoded data.  This method must be
        subclassed and implemented.

        '''
        assert(False)

    def decode(self, source):
        '''Returns RPC dependent decoded data.  This method must be
        subclassed and implemented.

        '''
        assert(False)

    def _call(self, conn, method, *params):
        assert(False)

    def _get_conn(self, server, port):
        assert(False)
        
    def _put_conn(self, server, port, conn):
        assert(False)

class UKAIXMLRPCConnection(UKAIRPCConnection):
    def encode(self, source):
        return xmlrpclib.Binary(source)

    def decode(self, source):
        return source.data

    def _call(self, conn, method, *params):
        try:
            return getattr(conn, method)(*params)
        except xmlrpclib.Error, e:
            print e.__class__
            raise

    def _get_conn(self, server, port):
        conn_key = '%s:%d' % (server, port)
        if conn_key not in self._servers:
            self._servers[conn_key] = []
        conn_pool = self._servers[conn_key]
        conn = None
        try:
            conn = conn_pool.pop()
        except IndexError:
            conn = xmlrpclib.ServerProxy(
                'http://%s:%d' % (server, port), allow_none=True)
        return conn

    def _put_conn(self, server, port, conn):
        conn_key = '%s:%d' % (server, port)
        self._servers[conn_key].append(conn)

ukai_rpc_connection = UKAIXMLRPCConnection()
