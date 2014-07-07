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

''' The ukai_metadata.py module defines classes and functions to
handle metadata information of a UKAI virtual disk image.
'''

import json
import sys
import threading
import zlib

import netifaces

from ukai_config import UKAIConfig
from ukai_db import ukai_db_client
from ukai_rpc import UKAIXMLRPCCall, UKAIXMLRPCTranslation
from ukai_utils import UKAIIsLocalNode

UKAI_IN_SYNC = 0
UKAI_SYNCING = 1
UKAI_OUT_OF_SYNC = 2

UKAI_METADATA_BUCKET = 'metadata'

def ukai_metadata_create(image_name, size, block_size,
                         location, hypervisor, config):
    ''' The ukai_metadata_create function creates a metadata
    information.

    param name: the name of a virtual disk image
    param size: the total size of a virtual disk image, where the
        size must be multiple of the block_size value
    param block_size: The block size of a virtual disk image
    param hypervisor: The hyprevisor address on which a virtual
        machine of this disk user runs
    param location: The node address (currently IPv4 numeric
        address only) of initial data store.
    param config: an UKAIConfig instance.
    '''
    metadata_raw = {}
    metadata_raw['name'] = image_name
    metadata_raw['size'] = size
    metadata_raw['used_size'] = size
    metadata_raw['block_size'] = block_size
    metadata_raw['hypervisors'] = {}
    metadata_raw['hypervisors'][hypervisor] = {'sync_status': UKAI_IN_SYNC}
    metadata_raw['blocks'] = []
    blocks = metadata_raw['blocks']
    for block_num in range(0, size / block_size):
        location_entry = {location: {'sync_status': UKAI_IN_SYNC}}
        blocks.append(location_entry)

    metadata = UKAIMetadata(image_name, config, metadata_raw)
    metadata.flush()
    del metadata

def ukai_metadata_destroy(image_name, config):
    ''' The ukai_metadata_destroy function deletes metadata
    information.

    param image_name: the name of a virtual disk image
    param config: an UKAIConfig instance
    '''
    ukai_db_client.delete_metadata(image_name)
    return 0

class UKAIMetadata(object):
    '''
    The UKAIMetadata class contains metadata information of a disk image
    of the UKAI system.
    '''

    def __init__(self, image_name, config, metadata_raw=None):
        '''
        Initializes the class with the specified file contents.  The
        metadata_file is a JSON format file, which is generated by the
        UKAIMetadataCreate() function or the flush() method of this
        class.

        image_name: The name of a virtual disk image stored in a
            object storage.
        metadata_raw: a raw metadata which is specified when a new
            UKAIMetadata object is inserted.

        Return values: This function does not return any values.
        '''
        self._config = config
        self._rpc_trans = UKAIXMLRPCTranslation()
        if (metadata_raw != None):
            self._metadata = metadata_raw
        else:
            self._metadata = None
            self._metadata = ukai_db_client.get_metadata(image_name)

        self._lock = []
        for idx in range(0, len(self.blocks)):
            self._lock.append(threading.Lock())

    def flush(self):
        '''
        Writes out the latest metadata information stored in memory
        to the metadata file.
        '''
        try:
            self.acquire_lock()

            # Write out to the metadata storage.
            ukai_db_client.put_metadata(self.name, self._metadata)

            # Send the latest metadata information to all the hypervisors
            # using this virtual disk.
            for hv in ukai_db_client.get_readers(self.name):
                if UKAIIsLocalNode(hv):
                    continue
                try:
                    self._set_hypervisor_sync_status(hv, UKAI_IN_SYNC)
                    rpc_call = UKAIXMLRPCCall(
                        hv, self._config.get('core_port'))
                    rpc_call.call('proxy_update_metadata',
                                  self.name,
                                  self._rpc_trans.encode(zlib.compress(json.dumps(self._metadata))))
                except (IOError, xmlrpclib.Error), e:
                    print e.__class__
                    print 'Failed to update metadata at %s.  You cannot migrate a virtual machine to %s' % (hv, hv)
                    self._set_hypervisor_sync_status(hv, UKAI_OUT_OF_SYNC)

        finally:
            self.release_lock()

    @property
    def metadata(self):
        '''
        The metadata dictionary object of this instance.
        '''
        return(self._metadata)

    @metadata.setter
    def metadata(self, metadata_raw):
        if self._metadata is None:
            # If this is the first time to load the metadata, just
            # load it.
            self._metadata = metadata_raw
        else:
            # If the instance has metadata already, need to lock
            # the object to avoid thread confliction.
            try:
                self.acquire_lock()

                self._metadata = metadata_raw

            finally:
                self.release_lock()
        

    @property
    def name(self):
        '''
        The name of the disk image.
        '''
        return (self._metadata['name'])

    @property
    def size(self):
        '''
        The total size of the disk image.
        '''
        return (int(self._metadata['size']))

    @property
    def used_size(self):
        '''
        The used size of the disk image.
        '''
        return (int(self._metadata['used_size']))
    @used_size.setter
    def used_size(self, used_size):
        self._metadata['used_size'] = used_size

    @property
    def block_size(self):
        '''
        The block size of the disk image.
        '''
        return (int(self._metadata['block_size']))

    @property
    def hypervisors(self):
        '''
        The list of all hypervisors to host this virtual machine.
        '''
        return(self._metadata['hypervisors'])

    @property
    def blocks(self):
        '''
        An array of all blocks.  Need to acquire lock when modifying
        the contents.
        '''
        return(self._metadata['blocks'])

    def acquire_lock(self, start_idx=0, end_idx=-1):
        '''
        Acquires lock objects of the specified range of metadata
        blocks of the virtual disk.  If you don't specify any index
        values, the entire metadata blocks are locked.

        start_idx: The first block index of metadata blocks at which
            the lock object is aquired.
        end_idx: The last block index of metadata blocks at which
            the lock object is aquired.

        Return values: This function does not return any values.
        '''
        if end_idx == -1:
            end_idx = (self.size / self.block_size) - 1
        assert start_idx >= 0
        assert end_idx >= start_idx
        assert end_idx < (self.size / self.block_size)

        for blk_idx in range(0, end_idx + 1):
            self._lock[blk_idx].acquire()

    def release_lock(self, start_idx=0, end_idx=-1):
        '''
        Releases lock objects acquired by the acquire_lock method.  If
        you don't specify any index values, the entire metadata blocks
        are released, however if you try to release unlocked block,
        you will receive an assertion.
        
        start_idx: The first block index of metadata blocks at which
            the lock object is aquired.
        end_idx: The last block index of metadata blocks at which
            the lock object is aquired.

        Return values: This function does not return any values.
        '''
        if end_idx == -1:
            end_idx = (self.size / self.block_size) - 1
        assert start_idx >= 0
        assert end_idx >= start_idx
        assert end_idx < (self.size / self.block_size)

        for blk_idx in range(0, end_idx + 1):
            self._lock[blk_idx].release()

    def _set_hypervisor_sync_status(self, hypervisor, sync_status):
        assert (sync_status == UKAI_IN_SYNC
                or sync_status == UKAI_SYNCING
                or sync_status == UKAI_OUT_OF_SYNC)

        self.hypervisors[hypervisor]['sync_status'] = sync_status

    def set_sync_status(self, blk_idx, node, sync_status):
        '''
        Sets the sync_status property of the specidied location of the
        specified block index.

        blk_idx: The index of a block.
        node: The location information specified by the IP address
            of a storage node.
        sync_status: A new synchronization status
            UKAI_IN_SYNC: The block is synchronized.
            UKAI_SYNCING: The block is being synchronized (NOT USED).
            UKAI_OUT_OF_SYNC: The block is not synchronized.

        Return values: This function does not return any values.
        '''
        assert (sync_status == UKAI_IN_SYNC
                or sync_status == UKAI_SYNCING
                or sync_status == UKAI_OUT_OF_SYNC)

        self.blocks[blk_idx][node]['sync_status'] = sync_status

    def get_sync_status(self, blk_idx, node):
        '''
        Returns the sync_status property of the specified location of
        the specified block index.

        blk_idx: The index of a block.
        node: The location information specified by the IP address
            of a storage node.

        Return values: The following values is returned.
            UKAI_IN_SYNC: The block is synchronized.
            UKAI_SYNCING: The block is being synchronized (NOT USED).
            UKAI_OUT_OF_SYNC: The block is not synchronized.
        '''
        return (self.blocks[blk_idx][node]['sync_status'])

    def add_location(self, node, start_idx=0, end_idx=-1,
                     sync_status=UKAI_OUT_OF_SYNC):
        '''
        Adds location information (a node address) to specified range
        of blocks.

        node: the node (currently IPv4 numeric only) to be added.
        start_idx: the first index of the blocks array to add the node.
        end_idx: the end index of the blocks array to add the node.
            When specified -1, the end_block is replaced to the final index
            of the block array.
        sync_status: the initial synchronized status.

        Return values: This function does not return any values.
        '''
        if end_idx == -1:
            end_idx = (self.size / self.block_size) - 1
        assert start_idx >= 0
        assert end_idx >= start_idx
        assert end_idx < (self.size / self.block_size)

        try:
            self.acquire_lock(start_idx, end_idx)

            for blk_idx in range(start_idx, end_idx + 1):
                if node not in self.blocks[blk_idx]:
                    # if there is no node entry, create it.
                    self.blocks[blk_idx][node] = {}
                    self.set_sync_status(blk_idx, node, sync_status)

        finally:
            self.release_lock(start_idx, end_idx)

        self.flush()

    def remove_location(self, node, start_idx=0, end_idx=-1):
        '''
        Removes location information (a node address) from specified
        range of blocks.

        node: the node (currently IPv4 numeric only) to be removed.
        start_idx: the first index of the blocks array to add the node.
        end_idx: the end index of the blocks array to add the node.
            When specified -1, the end_block is replaced to the final index
            of the block array.

        Return values: This function does not return any values.
        '''
        if end_idx == -1:
            end_idx = (self.size / self.block_size) - 1
        assert start_idx >= 0
        assert end_idx >= start_idx
        assert end_idx < (self.size / self.block_size)

        try:
            self.acquire_lock(start_idx, end_idx)

            for blk_idx in range(start_idx, end_idx + 1):
                block = self.blocks[blk_idx]
                has_synced_node = False
                for member_node in block.keys():
                    if member_node == node:
                        continue
                    if (self.get_sync_status(blk_idx, member_node)
                        == UKAI_IN_SYNC):
                        has_synced_node = True
                        break
                if has_synced_node is False:
                    print 'block %d does not have synced block' % blk_idx
                    continue
                if node in block.keys():
                    del block[node]

        finally:
            self.release_lock(start_idx, end_idx)

        self.flush()

    def add_hypervisor(self, hypervisor):
        '''
        Adds a new hypervisor to the list of hypervisors.

        hypervisor: The IP address of a new hypervisor.

        Return values: This function does not return any values.
        '''
        if hypervisor not in self.hypervisors.keys():
            self.hypervisors[hypervisor] = {'sync_status': UKAI_OUT_OF_SYNC}

        self.flush()

    def remove_hypervisor(self, hypervisor):
        '''
        Removed a hypervisor from the list of hypervisors.

        hypervisor: The IP address of the hypervisor to be removed.

        Return values: This function does not return any values.
        '''
        if hypervisor in self.hypervisors.keys():
            del self.hypervisors[hypervisor]

        self.flush()

if __name__ == '__main__':
    UKAIMetadataCreate('test', 1000000, 100000, '192.168.100.1',
                       '192.168.100.100')
    meta = UKAIMetadata('test', None, UKAIConfig())
    print 'metadata:', meta._metadata
    print 'name:', meta.name
    print 'size:', meta.size
    print 'block_size:', meta.block_size
    print 'block[0]:', meta.blocks[0]
    print 'block[3]:', meta.blocks[3]

    for blk_idx in range(0, meta.size / meta.block_size):
        for node in meta.blocks[blk_idx].keys():
            if node == '192.168.100.100':
                meta.set_sync_status(blk_idx, node, UKAI_OUT_OF_SYNC)
    meta.flush()
    for blk_idx in range(0, meta.size / meta.block_size):
        for node in meta.blocks[blk_idx].keys():
            if node == '192.168.100.100':
                meta.set_sync_status(blk_idx, node, UKAI_IN_SYNC)
    meta.flush()

    meta.add_location('192.168.100.101')
    print meta.blocks

    meta.remove_location('192.168.100.101')
    print meta.blocks
