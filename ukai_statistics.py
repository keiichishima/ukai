# Copyright 2013 IIJ Innovation Institute Inc. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY IIJ INNOVATION INSTITUTE INC. ``AS IS'' AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL IIJ INNOVATION INSTITUTE INC. OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
# OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
# OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

'''
The ukai_statistics.py module provides statistics structure of the
UKAI disk I/O.
'''

from ukai_config import UKAIConfig

# The unit of I/O histogram.
UKAI_HISTOGRAM_UNIT = 512

# The dictionary object of the list of UKAIImageStatistics instances.
UKAIStatistics = {}

class UKAIImageStatistics(object):
    '''
    The UKAIImageStatistics class contains statistics information of a
    specific UKAI disk image.
    '''
    def __init__(self):
        '''
        Initializes an instance.  If you need a per block statistics
        information section, you need to set UKAIConfig['block_stats']
        to True, before creating an instance.
        '''
        self._block_stats_enabled = False
        if 'block_stats' in UKAIConfig:
            self._block_stats_enabled = UKAIConfig['block_stats']

        self._stats = {}
        self._stats['descriptor'] = -1

        # total I/O statistics.
        self._init_io_stats(self._stats)

        # per block statistics.  enabled when
        # UKAIConfig['block_stats'] is True.
        self._stats['blocks'] = {}

        # histogram based on the I/O size.
        self._stats['histogram'] = {}
        self._stats['histogram']['read'] = {}
        self._stats['histogram']['write'] = {}

    @property
    def descriptor(self):
        '''
        The file descriptor of this disk image.  -1 if the disk image
        is closed.
        '''
        return(self._stats['descriptor'])
    @descriptor.setter
    def descriptor(self, value):
        self._stats['descriptor'] = value

    @property
    def stats(self):
        '''
        The statistics information represented as a Python dictionary.
        '''
        return (self._stats)

    def read_op(self, pieces):
        '''
        Updates statistics for a read operation.

        pieces: The block index, offset, and size structure defined as
            follows.

                (block index, start position, length)

            The format is the same format generated by the
            UKAIData._gather_pieces() method.
        '''
        total_size = 0
        for piece in pieces:
            blk_idx = piece[0]
            size = piece[2]
            total_size += size
            if self._block_stats_enabled is True:
                if blk_idx not in self._stats['blocks']:
                    self._stats['blocks'][blk_idx] = {}
                    self._init_io_stats(self._stats['blocks'][blk_idx])
                self._stats['blocks'][blk_idx]['read_bytes'] += size
                self._stats['blocks'][blk_idx]['read_ops'] += 1
        self._stats['read_bytes'] += total_size
        self._stats['read_ops'] += 1
        self._update_histogram(self._stats['histogram']['read'], total_size)

    def write_op(self, pieces):
        '''
        Updates statistics for a write operation.

        pieces: The block index, offset, and size structure defined as
            follows.

                (block index, start position, length)

            The format is the same format generated by the
            UKAIData._gather_pieces() method.
        '''
        total_size = 0
        for piece in pieces:
            blk_idx = piece[0]
            size = piece[2]
            total_size += size
            if self._block_stats_enabled is True:
                if blk_idx not in self._stats['blocks']:
                    self._stats['blocks'][blk_idx] = {}
                    self._init_io_stats(self._stats['blocks'][blk_idx])
                self._stats['blocks'][blk_idx]['write_bytes'] += size
                self._stats['blocks'][blk_idx]['write_ops'] += 1
        self._stats['write_bytes'] += total_size
        self._stats['write_ops'] += 1
        self._update_histogram(self._stats['histogram']['write'], total_size)

    def _init_io_stats(self, stats):
        stats['read_bytes'] = 0
        stats['read_ops'] = 0
        stats['write_bytes'] = 0
        stats['write_ops'] = 0

    def _update_histogram(self, hist, size):
        index = size / UKAI_HISTOGRAM_UNIT
        label = index
        if label not in hist:
            hist[label] = 0
        hist[label] += 1
    
