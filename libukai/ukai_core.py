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

''' The ukai_core.py module defines classes required for the UKAI
filesystem core operations.
'''

import errno
import json
import os
import stat
import sys
import threading
import zlib

from ukai_config import UKAIConfig
from ukai_data import UKAIData
from ukai_data import ukai_data_destroy, ukai_data_location_destroy
from ukai_db import ukai_db_client
from ukai_local_io import ukai_local_read, ukai_local_write
from ukai_local_io import ukai_local_allocate_dataspace
from ukai_local_io import ukai_local_destroy_image
from ukai_metadata import UKAIMetadata, UKAI_OUT_OF_SYNC
from ukai_metadata import ukai_metadata_create, ukai_metadata_destroy
from ukai_node_error_state import UKAINodeErrorStateSet
from ukai_rpc import UKAIXMLRPCTranslation
from ukai_statistics import UKAIStatistics, UKAIImageStatistics

# XXX Fix this
lock = threading.Lock()

class UKAIWriters(object):
    def __init__(self):
        self._images = {}

    def add_writer(self, image_name, fh):
        if image_name not in self._images:
            self._images[image_name] = fh
            return 0
        else:
            return errno.EBUSY

    def remove_writer(self, image_name, fh):
        if image_name not in self._images:
            return 0
        if self._images[image_name] == fh:
            del self._images[image_name]
        return 0

class UKAIOpenImageCount(object):
    def __init__(self):
        self._images = {}

    def increment(self, image_name):
        if image_name not in self._images:
            self._images[image_name] = 1
        else:
            self._images[image_name] += 1
        return self._images[image_name]

    def decrement(self, image_name):
        assert image_name in self._images
        ret = 0
        self._images[image_name] -= 1
        if self._images[image_name] == 0:
            del self._images[image_name]
        else:
            ret = self._images[image_name]
        return ret

class UKAICore(object):
    ''' The UKAICore class implements core processing of the UKAI
    filesystem.
    '''

    def __init__(self, config):
        self._metadata_dict = {}
        self._data_dict = {}
        self._config = config
        self._node_error_state_set = UKAINodeErrorStateSet()
        self._rpc_trans = UKAIXMLRPCTranslation()
        self._writers = UKAIWriters()
        self._open_count = UKAIOpenImageCount()
        self._fh = 0
        ukai_db_client.connect(self._config)

    ''' Filesystem I/O processing.
    '''
    def getattr(self, path):
        ret = 0
        st = None
        if path == '/':
            st = dict(st_mode=(stat.S_IFDIR | 0755), st_ctime=0,
                      st_mtime=0, st_atime=0, st_nlink=2)
        else:
            image_name = path[1:]
            metadata = self._get_metadata(image_name)
            if metadata is not None:
                st = dict(st_mode=(stat.S_IFREG | 0644), st_ctime=0,
                          st_mtime=0, st_atime=0, st_nlink=1,
                          st_size=metadata['used_size'])
            else:
                ret = errno.ENOENT
        return ret, json.dumps(st)

    def open(self, path, flags):
      try:
          lock.acquire()
          ret = 0
          image_name = path[1:]
          metadata = self._get_metadata(image_name)
          if metadata is None:
              return errno.ENOENT, None
          self._fh += 1
          if (flags & 3) != os.O_RDONLY:
              if self._writers.add_writer(image_name, self._fh) == errno.EBUSY:
                  return errno.EBUSY, None
          if self._open_count.increment(image_name) == 1:
              self._add_image(image_name)
          return 0, self._fh
      finally:
          lock.release()

    def release(self, path, fh):
      try:
          lock.acquire()
          image_name = path[1:]
          self._writers.remove_writer(image_name, fh)
          if self._open_count.decrement(image_name) == 0:
              self._remove_image(image_name)
          return 0
      finally:
          lock.release()

    def read(self, path, str_size, str_offset):
        image_name = path[1:]
	size = int(str_size)
	offset = int(str_offset)
        if not self._exists(image_name):
            return errno.ENOENT, None
        image_data = self._data_dict[image_name]
        data = image_data.read(size, offset)
        return 0, self._rpc_trans.encode(data)

    def readdir(self, path):
        return ['.', '..'] + self._metadata_dict.keys()

    def statfs(self, path):
        ''' TODO: the values are fake right now.
        '''
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def truncate(self, path, str_length):
        image_name = path[1:]
        length = int(str_length)
        if not self._exists(image_name):
            return errno.ENOENT
        image_metadata = self._metadata_dict[image_name]
        if image_metadata.size < length:
            return errno.EINVAL
        image_metadata.used_size = length
        image_metadata.flush()
        return 0

    def unlink(self, path):
        return errno.EPERM

    def write(self, path, encoded_data, str_offset):
        image_name = path[1:]
	offset = int(str_offset)
        if not self._exists(image_name):
            return errno.ENOENT, None
        image_data = self._data_dict[image_name]
        return 0, image_data.write(self._rpc_trans.decode(encoded_data),
                                   offset)

    def _get_metadata(self, image_name):
        return ukai_db_client.get_metadata(image_name)

    def _add_image(self, image_name):
        assert image_name not in self._metadata_dict
        metadata = UKAIMetadata(image_name, self._config)
        ukai_db_client.join_reader(image_name, self._config.get('id'))
        self._metadata_dict[image_name] = metadata
        data = UKAIData(metadata=metadata,
                        node_error_state_set=self._node_error_state_set,
                        config=self._config)
        self._data_dict[image_name] = data
        UKAIStatistics[image_name] = UKAIImageStatistics()

    def _remove_image(self, image_name):
        assert image_name in self._metadata_dict
        ukai_db_client.leave_reader(image_name, self._config.get('id'))
        del self._metadata_dict[image_name]
        del self._data_dict[image_name]
        del UKAIStatistics[image_name]

    def _exists(self, image_name):
        if image_name not in self._metadata_dict:
            return False
        if image_name not in self._data_dict:
            return False
        return True


    ''' Proxy server processing.
    '''
    def proxy_read(self, image_name, block_size, block_index, offset,
                   size):
        data = ukai_local_read(image_name, block_size, block_index,
                               offset, size, self._config)
        return self._rpc_trans.encode(zlib.compress(data))

    def proxy_write(self, image_name, block_size, block_index, offset,
                    encoded_data):
        data = zlib.decompress(self._rpc_trans.decode(encoded_data))
        return ukai_local_write(image_name, block_size, block_index,
                                offset, data, self._config)

    def proxy_allocate_dataspace(self, image_name, block_size, block_index):
        return ukai_local_allocate_dataspace(image_name, block_size,
                                             block_index, self._config)

    def proxy_deallocate_dataspace(self, image_name, block_index):
        return ukai_local_deallocate_dataspace(image_name, block_index,
                                               self._config)

    def proxy_update_metadata(self, image_name, encoded_metadata):
        metadata_raw = json.loads(zlib.decompress(self._rpc_trans.decode(
                    encoded_metadata)))
        if image_name in self._metadata_dict:
            self._metadata_dict[image_name].metadata = metadata_raw
        else:
            metadata = UKAIMetadata(image_name, self._config, metadata_raw)
            self._metadata_dict[image_name] = metadata
            self._data_dict[image_name] = UKAIData(metadata,
                                                   self._node_error_state_set,
                                                   self._config)
            UKAIStatistics[image_name] = UKAIImageStatistics()

        return 0

    def proxy_destroy_image(self, image_name):
        return ukai_local_destroy_image(image_name, self._config)


    ''' Controll processing.
    '''
    def ctl_create_image(self, image_name, str_size, block_size=None,
                         location=None):
        assert image_name is not None
        size = int(str_size)
        assert size > 0

        if block_size is None:
            defaults = self._config.get('create_default')
            block_size = defaults['block_size']
        assert block_size > 0
        assert size > block_size
        assert size % block_size == 0
        block_count = size / block_size

        if location is None:
            location = self._config.get('core_server')

        ukai_metadata_create(image_name, size, block_size,
                             location, self._config)

    def ctl_destroy_image(self, image_name):
        assert image_name is not None

        ukai_data_destroy(image_name, self._config)
        ukai_metadata_destroy(image_name, self._config)

    def ctl_get_metadata(self, image_name):
        metadata = self._get_metadata(image_name)
        if metadata is None:
            return errno.ENOENT, None
        return 0, json.dumps(metadata)

    def ctl_add_location(self, image_name, location,
                         start_index=0, end_index=-1,
                         sync_status=UKAI_OUT_OF_SYNC):
        metadata = None
        if image_name in self._metadata_dict:
            # the image is in use on this node.
            metadata = self._metadata_dict[image_name]
        else:
            # XXX need to check if no one is using this image.
            metadata_raw = self._get_metadata(image_name)
            if metadata_raw is None:
                return errno.ENOENT
            metadata = UKAIMetadata(image_name, self._config, metadata_raw)
        metadata.add_location(location, start_index, end_index, sync_status)
        return 0

    def ctl_remove_location(self, image_name, location,
                            start_index=0, end_index=-1):
        metadata = None
        if image_name in self._metadata_dict:
            # the image is in use on this node.
            metadata = self._metadata_dict[image_name]
        else:
            # XXX need to check if no one is using this image.
            metadata_raw = self._get_metadata(image_name)
            if metadata_raw is None:
                return errno.ENOENT
            metadata = UKAIMetadata(image_name, self._config, metadata_raw)
        metadata.remove_location(location, start_index, end_index)
        ukai_data_location_destroy(image_name, location, self._config)
        return 0 

    def ctl_add_hypervisor(self, image_name, hypervisor):
        metadata = None
        if image_name in self._metadata_dict:
            # the image is in use on this node.
            metadata = self._metadata_dict[image_name]
        else:
            # XXX need to check if no one is using this image.
            metadata_raw = self._get_metadata(image_name)
            if metadata_raw is None:
                return errno.ENOENT
            metadata = UKAIMetadata(image_name, self._config, metadata_raw)
        metadata.add_hypervisor(hypervisor)
        return 0

    def ctl_remove_hypervisor(self, image_name, hypervisor):
        metadata = None
        if image_name in self._metadata_dict:
            # the image is in use on this node.
            metadata = self._metadata_dict[image_name]
        else:
            # XXX need to check if no one is using this image.
            metadata_raw = self._get_metadata(image_name)
            if metadata_raw is None:
                return errno.ENOENT
            metadata = UKAIMetadata(image_name, self._config, metadata_raw)
        metadata.remove_hypervisor(hypervisor)
        return 0

    def ctl_synchronize(self, image_name, start_index=0, end_index=-1,
                        verbose=False):
        metadata = None
        if image_name in self._metadata_dict:
            # the image is in use on this node.
            metadata = self._metadata_dict[image_name]
            data = self._data_dict[image_name]
        else:
            # XXX need to check if no one is using this image.
            metadata_raw = self._get_metadata(image_name)
            if metadata_raw is None:
                return errno.ENOENT
            metadata = UKAIMetadata(image_name, self._config, metadata_raw)
            data = UKAIData(metadata, self._node_error_state_set, self._config)
        if end_index == -1:
            end_index = (metadata.size / metadata.block_size) - 1
        for block_index in range(start_index, end_index + 1):
            if verbose is True:
                print 'Syncing block %d (from %d to %d)' % (block_index,
                                                            start_index,
                                                            end_index)
            if data.synchronize_block(block_index) is True:
                metadata.flush()
        return 0

    def ctl_get_node_error_state_set(self):
        return self._node_error_state_set.get_list()

    def ctl_get_image_names(self):
        return ukai_db_client.get_image_names()

    def ctl_diag(self):
        print self._open_count._images
        print self._writers._images
	print self._metadata_dict
        return 0
