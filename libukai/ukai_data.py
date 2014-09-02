# Copyright 2013, 2014
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

''' The ukai_data.py module defines classes and functions to handle
image data of a UKAI virtual disk image.
'''

import os
import sys
import threading
import zlib

import netifaces

from ukai_config import UKAIConfig
from ukai_local_io import ukai_local_read, ukai_local_write
from ukai_metadata import UKAIMetadata
from ukai_metadata import UKAI_IN_SYNC, UKAI_SYNCING, UKAI_OUT_OF_SYNC
from ukai_rpc import UKAIXMLRPCCall, UKAIXMLRPCTranslation
from ukai_statistics import UKAIStatistics
from ukai_utils import UKAIIsLocalNode

def ukai_data_destroy(image_name, config):
    ''' The ukai_datadestroy function deletes data files of a virtual
    disk image.

    param image_name: the name of a virtual disk image
    param config: an UKAIConfig instance
    '''
    metadata = UKAIMetadata(image_name, config)
    blocks = metadata.blocks
    location_set = set()
    for n in blocks:
        for location in n.keys():
            location_set.add(location)
    for location in location_set:
        rpc_call = UKAIXMLRPCCall(location, config.get('core_port'))
        # XXX handle rpc error.
        rpc_call.call('proxy_destroy_image', image_name)

    return 0

def ukai_data_location_destroy(image_name, location, config):
    ''' The ukai_data_location_destroy function deletes a data file
    stored at the specified location.

    param image_name: the name of a virtual disk image
    param location: address of a location
    param config: an UKAIConfig instance
    '''
    rpc_call = UKAIXMLRPCCall(location, config.get('core_port'))
    rpc_call.call('proxy_destroy_image', image_name)
    return 0

class UKAIData(object):
    '''
    The UKAIData class provides manipulation functions to modify the
    disk image contents.
    '''

    def __init__(self, metadata, node_error_state_set, config):
        '''
        Initializes the instance with the specified metadata object
        created with the UKAIMetadata class.
        '''
        self._metadata = metadata
        self._node_error_state_set = node_error_state_set
        self._config = config
        self._rpc_trans = UKAIXMLRPCTranslation()
        # Lock objects per block index.
        self._lock = []
        for blk_idx in range(0, len(metadata.blocks)):
            self._lock.append(threading.Lock())

    def _gather_pieces(self, offset, size):
        '''
        Returns a list of tupples specifying which block and index in
        the blocks are related to the offset and size of the disk
        image.  The tupple format is shown below.
          (block index, start position, length)

        offset: offset from the beginning of the disk image.
        size: the length of the data to be handled.
        '''
        assert size > 0
        assert offset >= 0
        assert (size + offset) <= self._metadata.size

        # piece format: (block index, start position, length)
        start_block = offset / self._metadata.block_size
        end_block = (offset + size - 1) / self._metadata.block_size
        start_block_pos = offset - (start_block * self._metadata.block_size)
        end_block_pos = (offset + size) - (end_block * self._metadata.block_size)
        pieces = []
        if start_block == end_block:
            pieces.append((start_block,
                           start_block_pos,
                           size))
        else:
            for block in range(start_block, end_block + 1):
                if block == start_block:
                    pieces.append((block,
                                   start_block_pos,
                                   self._metadata.block_size - start_block_pos))
                elif block == end_block:
                    pieces.append((block,
                                   0,
                                   end_block_pos))
                else:
                    pieces.append((block,
                                   0,
                                   self._metadata.block_size))
        return (pieces)

    def read(self, size, offset):
        '''
        Reads size bytes from the specified location in the disk image
        specified as the offset argument.  The read data is returned
        as a return value.
        '''
        assert size > 0
        assert offset >= 0

        if offset >= self._metadata.used_size:
            # end of the file.
            return ('')
        if offset + size > self._metadata.used_size:
            # shorten the size not to overread the end of the file.
            size = self._metadata.used_size - offset

        data = ''
        partial_data = ''
        metadata_flush_required = False
        pieces = self._gather_pieces(offset, size)
        # read operation statistics.
        UKAIStatistics[self._metadata.name].read_op(pieces)
        try:
            for piece in pieces:
                self._metadata._lock[piece[0]].acquire() # XXX
                self._lock[piece[0]].acquire()

            for piece in pieces:
                blk_idx = piece[0]
                off_in_blk = piece[1]
                size_in_blk = piece[2]
                block = self._metadata.blocks[blk_idx]
                data_read = False
                while not data_read:
                    candidate = self._find_read_candidate(blk_idx)
                    if candidate is None:
                        print 'XXX fatal.  should raise an exception.'
                    try:
                        partial_data = self._get_data(candidate,
                                                      blk_idx,
                                                      off_in_blk,
                                                      size_in_blk)
                        data_read = True
                        break
                    except (IOError, xmlrpclib.Error), e:
                        print e.__class__
                        self._metadata.set_sync_status(blk_idx, candidate,
                                                       UKAI_OUT_OF_SYNC)
                        metadata_flush_required = True
                        self._node_error_state_set.add(candidate, 0)
                        # try to find another candidate node.
                        continue
                if data_read is False:
                    # no node is available to get the peice of data.
                    print 'XXX fatal.  should raise an exception.'

                data = data + partial_data
        finally:
            for piece in pieces:
                self._metadata._lock[piece[0]].release() # XXX
                self._lock[piece[0]].release()

        if metadata_flush_required is True:
            self._metadata.flush()

        return (data)

    def _find_read_candidate(self, blk_idx):
        candidate = None
        for node in self._metadata.blocks[blk_idx].keys():
            if self._node_error_state_set.is_in_failure(node) is True:
                continue
            if self._metadata.get_sync_status(blk_idx, node) != UKAI_IN_SYNC:
                continue
            if UKAIIsLocalNode(node):
                candidate = node
                break
            candidate = node
        return (candidate)

    def _get_data(self, node, blk_idx, off_in_blk, size_in_blk):
        '''
        Returns a data read from a local store or a remote store
        depending on the node location.

        node: the target node from which we read the data.
        num: the block index of the disk image.
        offset: the offset relative to the beginning of the specified
            block.
        size: the length of the data to be read.
        '''
        assert size_in_blk > 0
        assert off_in_blk >= 0
        assert (off_in_blk + size_in_blk) <= self._metadata.block_size

        if UKAIIsLocalNode(node):
            return (self._get_data_local(node,
                                         blk_idx,
                                         off_in_blk,
                                         size_in_blk))
        else:
            return (self._get_data_remote(node,
                                          blk_idx,
                                          off_in_blk,
                                          size_in_blk))

    def _get_data_local(self, node, blk_idx, off_in_blk, size_in_blk):
        '''
        Returns a data read from a local store.

        node: the target node from which we read the data.
        num: the block index of the disk image.
        offset: the offset relative to the beginning of the specified
            block.
        size: the length of the data to be read.
        '''
        data = ukai_local_read(self._metadata.name,
                               self._metadata.block_size,
                               blk_idx, off_in_blk, size_in_blk,
                               self._config)
        return (data)

    def _get_data_remote(self, node, blk_idx, off_in_blk, size_in_blk):
        '''
        Returns a data read from a remote store.  The remote read
        command is sent to a remote proxy program using the XML RPC
        mechanism.

        node: the target node from which we read the data.
        num: the block index of the disk image.
        offset: the offset relative to the beginning of the specified
            block.
        size: the length of the data to be read.
        '''
        rpc_call = UKAIXMLRPCCall(node,
                                  self._config.get('core_port'))
        encoded_data = rpc_call.call('proxy_read',
                                     self._metadata.name,
                                     str(self._metadata.block_size),
                                     str(blk_idx),
                                     str(off_in_blk),
                                     str(size_in_blk))
        return zlib.decompress(self._rpc_trans.decode(encoded_data))

    def write(self, data, offset):
        '''
        Writes the data from the specified location in the disk image
        specified as the offset argument.  The method returns the
        number of written data.
        '''
        assert data is not None
        assert offset >= 0
        assert (offset + len(data)) <= self._metadata.size

        metadata_flush_required = False
        pieces = self._gather_pieces(offset, len(data))
        # write operation statistics.
        UKAIStatistics[self._metadata.name].write_op(pieces)
        data_offset = 0
        try:
            for piece in pieces:
                self._metadata._lock[piece[0]].acquire() # XXX
                self._lock[piece[0]].acquire()

            for piece in pieces:
                blk_idx = piece[0]
                off_in_blk = piece[1]
                size_in_blk = piece[2]
                block = self._metadata.blocks[blk_idx]
                for node in block.keys():
                    try:
                        if (self._node_error_state_set.is_in_failure(node)
                            is True):
                            if (self._metadata.get_sync_status(blk_idx, node)
                                == UKAI_IN_SYNC):
                                self._metadata.set_sync_status(blk_idx, node,
                                                               UKAI_OUT_OF_SYNC)
                                metadata_flush_required = True
                            continue
                        if (self._metadata.get_sync_status(blk_idx, node)
                            != UKAI_IN_SYNC):
                            self._synchronize_block(blk_idx, node)
                            metadata_flush_required = True
                        self._put_data(node,
                                       blk_idx,
                                       off_in_blk,
                                       data[data_offset:data_offset
                                            + size_in_blk])
                    except (IOError, xmlrpclib.Error), e:
                        print e.__class__
                        self._metadata.set_sync_status(blk_idx, node,
                                                       UKAI_OUT_OF_SYNC)
                        metadata_flush_required = True
                        self._node_error_state_set.add(node, 0)
                data_offset = data_offset + size_in_blk
        finally:
            if offset + len(data) > self._metadata.used_size:
                self._metadata.used_size = offset + len(data)
                metadata_flush_required = True
            for piece in pieces:
                self._metadata._lock[piece[0]].release() # XXX
                self._lock[piece[0]].release()

        if metadata_flush_required is True:
            self._metadata.flush()

        return (len(data))

    def _put_data(self, node, blk_idx, off_in_blk, data):
        '''
        Writes the data to a local store or a remote store depending
        on the node location.

        node: the target node from which we read the data.
        num: the block index of the disk image.
        offset: the offset relative to the beginning of the specified
            block.
        data: the data to be written.
        '''
        assert off_in_blk >= 0
        assert (off_in_blk + len(data)) <= self._metadata.block_size

        if UKAIIsLocalNode(node):
            return (self._put_data_local(node,
                                         blk_idx,
                                         off_in_blk,
                                         data))
        else:
            return (self._put_data_remote(node,
                                          blk_idx,
                                          off_in_blk,
                                          data))

    def _put_data_local(self, node, blk_idx, off_in_blk, data):
        '''
        Writes the data to a local store.

        node: the target node from which we read the data.
        num: the block index of the disk image.
        offset: the offset relative to the beginning of the specified
            block.
        data: the data to be written.
        '''
        return ukai_local_write(self._metadata.name,
                                self._metadata.block_size,
                                blk_idx, off_in_blk, data,
                                self._config)

    def _put_data_remote(self, node, blk_idx, off_in_blk, data):
        '''
        Writes the data to a remote store.  The remote write command
        is sent to a remote proxy program using the XML RPC mechanism.

        node: the target node from which we read the data.
        num: the block index of the disk image.
        offset: the offset relative to the beginning of the specified
            block.
        data: the data to be written.
        '''
        rpc_call = UKAIXMLRPCCall(node, self._config.get('core_port'))
        return rpc_call.call('proxy_write',
                             self._metadata.name,
                             str(self._metadata.block_size),
                             str(blk_idx),
                             str(off_in_blk),
                             self._rpc_trans.encode(zlib.compress(data)))

    def synchronize_block(self, blk_idx):
        '''
        Synchronizes the specified block specified by the blk_idx
        argument.

        Return value: True if metadata is modified, otherwise False.

        This function is used only by a background synchronization
        process, and must not be called by any other processes.
        '''
        metadata_flush_required = False
        try:
            self._metadata._lock[blk_idx].acquire() # XXX
            self._lock[blk_idx].acquire()

            for node in self._metadata.blocks[blk_idx].keys():
                if (self._metadata.get_sync_status(blk_idx, node)
                    == UKAI_IN_SYNC):
                    continue
                self._synchronize_block(blk_idx, node)
                metadata_flush_required = True
        finally:
            self._metadata._lock[blk_idx].release() # XXX
            self._lock[blk_idx].release()

        return (metadata_flush_required)

    def _synchronize_block(self, blk_idx, node):
        '''
        Synchronizes the specified block by the blk_idx argument.
        This function first search the already synchronized node block
        and copy the data to all the other not-synchronized nodes.
        '''
        block = self._metadata.blocks[blk_idx]
        final_candidate = None
        for candidate in block.keys():
            if (self._metadata.get_sync_status(blk_idx, candidate)
                != UKAI_IN_SYNC):
                continue
            if UKAIIsLocalNode(candidate):
                final_candidate = candidate
                break
            final_candidate = candidate
        if final_candidate == None:
            # XXX fatal
            # should raise an exception
            print 'Disk image of %s has unrecoverble error.' % self._metadata.name

        self._allocate_dataspace(node, blk_idx)
        data = self._get_data(final_candidate,
                              blk_idx,
                              0,
                              self._metadata.block_size)
        self._put_data(node,
                       blk_idx,
                       0,
                       data)
        self._metadata.set_sync_status(blk_idx, node, UKAI_IN_SYNC)

    def _allocate_dataspace(self, node, blk_idx):
        '''
        Allocates an empty data block in a local store specified by
        the blk_idx argument.
        '''
        if UKAIIsLocalNode(node):
            path = '%s/%s/' % (self._config.get('data_root'),
                           self._metadata.name)
            if not os.path.exists(path):
                os.makedirs(path)
            path = path + self._config.get('blockname_format') % blk_idx
            fh = open(path, 'w')
            fh.seek(self._metadata.block_size - 1)
            fh.write('\0')
            fh.close()
        else:
            rpc_call = UKAIXMLRPCCall(
                node, self._config.get('core_port'))
            rpc_call.call('proxy_allocate_dataspace',
                          self._metadata.name,
                          self._metadata.block_size,
                          blk_idx)

if __name__ == '__main__':
    from ukai_node_error_state import UKAINodeErrorStateSet

    ukai_config.set('data_root', './test/local/data')
    ukai_config.set('metadata_root', './test/local/metadata')
    ness = UKAINodeErrorStateSet()

    meta = UKAIMetadata('./test/local/metadata/test')
    fh = UKAIData(meta, ness)
    data = 'Hello World!'
    offset = 0
    print 'offset %d' % offset
    fh.write(data, offset)
    ver = fh.read(len(data), offset)
    if ver != data:
        print 'error at offset %d' % offset

    offset = meta.block_size - (len(data) / 2)
    print 'offset %d' % offset
    fh.write(data, offset)
    ver = fh.read(len(data), offset)
    if ver != data:
        print ver
        print 'error at offset %d' % offset
    
    block_count = meta.size / meta.block_size
    if meta.size % meta.block_size:
        block_count = block_count + 1
    offset = (meta.block_size * block_count) - len(data)
    print 'offset %d' % offset
    fh.write(data, offset)
    ver = fh.read(len(data), offset)
    if ver != data:
        print 'error at offset %d' % offset
    
    for block_num in range(0, meta.size / meta.block_size):
        print 'sync block_num %d' % block_num
        remote = xmlrpclib.ServerProxy('http://127.0.0.1:%d/' %
                                       ukai_config.get('proxy_port'))
        remote.allocate_dataspace(meta.name,
                                  meta.block_size,
                                  block_num)
        remote.write(meta.name, meta.block_size,
                     block_num, 0,
                     xmlrpclib.Binary(fh._get_data_local('dummy',
                                                         block_num,
                                                         0,
                                                         meta.block_size)))
