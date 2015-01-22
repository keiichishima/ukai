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

''' The ukai_fuse_connector.py module provides a FUSE interface
implementation.
'''

import errno
import json
import sys

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

from ukai_config import UKAIConfig
from ukai_rpc import ukai_rpc_connection

class UKAIFUSE(LoggingMixIn, Operations):
    ''' The UKAIFUSE class provides a FUSE operation implementation.
    '''

    def __init__(self, config):
        ''' Initializes the UKAUFUSE class.

        param config: an UKAIConfig instance
        '''
        self._config = config

    def init(self, path):
        ''' Initializes the FUSE operation.
        '''
        pass

    def destroy(self, path):
        ''' Cleanups the FUSE operation.
        '''
        pass

    def chmod(self, path, mode):
        ''' This interface is provided for changing file modes,
        howerver UKAI doesn't support any such operation.
        '''
        return 0

    def chown(self, path, uid, gid):
        ''' This interface is provided for changing owndership of a
        file, howerver UKAI doesn't support any such operation.
        '''
        return 0

    def create(self, path, mode):
        ''' This interface is provided for creating a file.  At this
        moment, UKAI doesn't support creating a virtual disk using
        this interface.  To create a virtual disk image, use the
        ukai_admin command.
        '''
        raise FuseOSError(errno.EPERM)

    def getattr(self, path, fh=None):
        ''' Returns file stat information of a specified file.

        param path: the path name of a file
        param fh: the file handle of the file (not used)
        '''
        (ret, json_st) = ukai_rpc_connection.call(
            self._config.get('core_server'), self._config.get('core_port'),
            'getattr', path)
        if ret != 0:
            raise FuseOSError(ret)
        return json.loads(json_st)

    def mkdir(self, path, mode):
        ''' This interface is provided for creating a directory,
        however UKAI doesn't support hierarchical directory structure
        at this moment.
        '''
        raise FuseOSError(errno.EPERM)

    def open(self, path, flags):
        ''' Opens a file specified by the path parameter.

        param path: the path name of a file
        param flags: the flags passed via the open(2) system call
        '''
        ret, fh = ukai_rpc_connection.call(
            self._config.get('core_server'), self._config.get('core_port'),
            'open', path, flags)
        if ret != 0:
            raise FuseOSError(ret)
        return fh

    def release(self, path, fh):
        ''' Releases a file opened before.

        param path: the path name of a file
        param fh: the file handle of the file
        '''
        ukai_rpc_connection.call(
            self._config.get('core_server'), self._config.get('core_port'),
            'release', path, fh)
        return 0

    def read(self, path, size, offset, fh):
        ''' Reads data from the UKAI core filesystem.

        param path: the path name of a file
        param size: the size to be read
        param offset: the offset from the beginning of the file
        param fh: the file handle of the file
        '''
        # The data returned by the UKAICore.read() method is encoded
        # using a RPC encorder.
        ret, encoded_data = ukai_rpc_connection.call(
            self._config.get('core_server'), self._config.get('core_port'),
            'read', path, str(size), str(offset))
        if ret != 0:
            raise FuseOSError(ret)
        return ukai_rpc_connection.decode(encoded_data)

    def readdir(self, path, fh):
        ''' Returns directory entries of a path.

        param path: a path name to be investigated
        '''
        return ukai_rpc_connection.call(
            self._config.get('core_server'), self._config.get('core_port'),
            'readdir', path)

    def readlink(self, path):
        ''' This interface is provided for reading a symbolic link
        destination, however UKAI doesn't support symbolic links.
        '''
        raise FuseOSError(errno.EPERM)

    def rename(self, old, new):
        ''' This interface is provided for renaming (moving) a file
        path, however UKAI doesn't support a rename operation.
        '''
        raise FuseOSError(errno.EPERM)

    def rmdir(self, path):
        ''' This interface is provided for removing a directory,
        however UKAI doesn't support hierarchical directory structure
        at this moment.
        '''
        raise FuseOSError(errno.EPERM)

    def statfs(self, path):
        ''' Returns a stat information of a file system where the
        specified file belongs to.

        param path: the path name of a file
        '''
        return ukai_rpc_connection.call(
            self._config.get('core_server'), self._config.get('core_port'),
            'statfs', path)

    def symlink(self, target, source):
        ''' This interface is provided for creating a symbolic link
        file, however UKAI doesn't support symbolic links.
        '''
        raise FuseOSError(errno.EPERM)

    def truncate(self, path, length, fh=None):
        ''' Changes the size of a file.

        param path: the path name of a file
        param length: the new size of the file
        param fh: the file handle of the file
        '''
        ret = ukai_rpc_connection.call(
            self._config.get('core_server'), self._config.get('core_port'),
            'truncate', path, str(length))
        if ret != 0:
            raise FuseOSError(ret)
        return ret

    def unlink(self, path):
        ''' This interface is provided for removing a file, howerver
        UKAI doesn't support removing files using this interface.  To
        remove a file (virtual disk image), use the ukai_admin
        command.
        '''
        raise FuseOSError(errno.EPERM)

    def utimens(self, path, times=None):
        ''' This interface is provided for setting time stamp
        information of a file, howerver, UKAI doesn't have such
        metadata.
        '''
        pass

    def write(self, path, data, offset, fh):
        ''' Writes data to a file.

        param path: the path name of a file
        param data: the data to be written
        param offset: the offset from the beginning of the file
        param fh: the file handle of the file
        '''
        # The data passed to the UKAICore.write interface must be
        # encoded using a proper RPC encoding mechanism.
        encoded_data = ukai_rpc_connection.encode(data)
        ret, nwritten = ukai_rpc_connection.call(
            self._config.get('core_server'), self._config.get('core_port'),
            'write', path, encoded_data, str(offset))
        if ret != 0:
            raise FuseOSError(ret)
        return nwritten
