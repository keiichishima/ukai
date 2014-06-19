# Copyright 2014
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

import errno
import json
import sys

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

from ukai_config import UKAIConfig
from ukai_rpc import UKAIXMLRPCClient, UKAIXMLRPCTranslation

class UKAIFUSE(LoggingMixIn, Operations):
    ''' UKAI FUSE connector.
    '''

    def __init__(self, config):
        self._config = config
        self._rpc_client = UKAIXMLRPCClient(self._config)
        self._rpc_trans = UKAIXMLRPCTranslation()

    def init(self, path):
        ''' Initializing code.
        '''
        pass

    def destroy(self, path):
        ''' Cleanup code.
        '''
        pass

    def chmod(self, path, mode):
        return 0

    def chown(self, path, uid, gid):
        return 0

    def create(self, path, mode):
        raise FuseOSError(errno.EPERM)

    def getattr(self, path, fh=None):
        (ret, json_st) = self._rpc_client.call('getattr', path)
        if ret != 0:
            raise FuseOSError(ret)
        return json.loads(json_st)

    def mkdir(self, path, mode):
        raise FuseOSError(errno.EPERM)

    def open(self, path, flags):
        ret, fh = self._rpc_client.call('open', path, flags)
        if ret != 0:
            raise FuseOSError(ret)
        return fh

    def release(self, path, fh):
        self._rpc_client.call('release', path, fh)
        return 0

    def read(self, path, size, offset, fh):
        # UKAI core returns the data as RPC safe encoded data.
        ret, encoded_data = self._rpc_client.call('read', path,
                                                  size, offset)
        if ret != 0:
            raise FuseOSError(ret)
        return self._rpc_trans.decode(encoded_data)

    def readdir(self, path, fh):
        return self._rpc_client.call('readdir', path)

    def readlink(self, path):
        raise FuseOSError(errno.EPERM)

    def rename(self, old, new):
        raise FuseOSError(errno.EPERM)

    def rmdir(self, path):
        raise FuseOSError(errno.EPERM)

    def statfs(self, path):
        return self._rpc_client.call('statfs', path)

    def symlink(self, target, source):
        raise FuseOSError(errno.EPERM)

    def truncate(self, path, length, fh=None):
        ret = self._rpc_client.call('truncate', path, str(length))
        if ret != 0:
            raise FuseOSError(ret)
        return ret

    def unlink(self, path):
        raise FuseOSError(errno.EPERM)

    def utimens(self, path, times=None):
        pass

    def write(self, path, data, offset, fh):
        # need to convert data to UKAI Core as a RPC safe
        # encoded data.
        encoded_data = self._rpc_trans.encode(data)
        ret, nwritten = self._rpc_client.call('write', path,
                                              encoded_data, offset)
        if ret != 0:
            raise FuseOSError(ret)
        return nwritten
