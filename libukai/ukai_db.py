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

import json
import threading

import redis

import kazoo.client

class UKAIDB(object):
    def __init__(self):
        self._servers = None
        self._client = None

    def connect(self, config):
        assert False

    def put_metadata(self, image_name, metadata):
        assert False

    def get_metadata(self, image_name):
        assert False

    def delete_metadata(self, image_name):
        assert False

    def join_reader(self, image_name, node):
        assert False

    def leave_reader(self, image_name, node):
        assert False

    def get_readers(self, image_name):
        assert False

    def get_image_names(self):
        assert False

UKAI_REDIS_DB_LOCKS_DIR    = '/ukai/metadata/locks'
UKAI_REDIS_DB_CONTENTS_DIR = '/ukai/metadata/contents'
UKAI_REDIS_DB_READERS_DIR  = '/ukai/metadata/readers'
UKAI_REDIS_DB_WRITERS_DIR  = '/ukai/metadata/writers'
class UKAIRedisDB(UKAIDB):
    def __init__(self):
        super(UKAIRedisDB, self).__init__()

    def connect(self, config):
        self._servers = config.get('metadata_servers')
        self._conn_pool = redis.ConnectionPool(host=self._servers)
        self._client = redis.Redis(connection_pool=self._conn_pool)

    def put_metadata(self, image_name, metadata):
        contents_file = UKAI_REDIS_DB_CONTENTS_DIR + '/' + image_name
        lock_file = UKAI_REDIS_DB_LOCKS_DIR + '/' + image_name
        with self._client.lock(lock_file):
            self._client.set(contents_file, json.dumps(metadata))

    def get_metadata(self, image_name):
        contents_file = UKAI_REDIS_DB_CONTENTS_DIR + '/' + image_name
        lock_file = UKAI_REDIS_DB_LOCKS_DIR + '/' + image_name
        ret = None
        with self._client.lock(lock_file):
            ret_json = self._client.get(contents_file)
            if ret_json is not None:
                ret = json.loads(ret_json)
        return ret

    def delete_metadata(self, image_name):
        contents_file = UKAI_REDIS_DB_CONTENTS_DIR + '/' + image_name
        lock_file = UKAI_REDIS_DB_LOCKS_DIR + '/' + image_name
        with self._client.lock(lock_file):
            if self._client.exists(contents_file) is False:
                return
            self._client.delete(contents_file)

    def join_reader(self, image_name, node):
        readers_file = UKAI_REDIS_DB_READERS_DIR + '/' + image_name
        lock_file = UKAI_REDIS_DB_LOCKS_DIR + '/' + image_name
        with self._client.lock(lock_file):
            readers = []
            readers_json = self._client.get(readers_file)
            if readers_json is not None:
                readers = json.loads(readers_json)
            if node not in readers:
                readers.append(node)
            self._client.set(readers_file, json.dumps(readers))

    def leave_reader(self, image_name, node):
        readers_file = UKAI_ZK_DB_READERS_DIR + '/' + image_name
        lock_file = UKAI_REDIS_DB_LOCKS_DIR + '/' + image_name
        with self._client.lock(lock_file):
            if self._client.exists(readers_file) is None:
                return
            readers_json = self._client.get(readers_file)
            readers = json.loads(readers_json)
            if node in readers:
                readers.remove(node)
                if len(readers) == 0:
                    self._client.delete(readers_file)
                else:
                    self._client.set(readers_file, json.dumps(readers))

    def get_readers(self, image_name):
        readers_file = UKAI_REDIS_DB_READERS_DIR + '/' + image_name
        lock_file = UKAI_REDIS_DB_LOCKS_DIR + '/' + image_name
        with self._client.lock(lock_file):
            readers = []
            readers_json = self._client.get(readers_file)
            if readers_json is not None:
                readers = json.loads(readers_json)
            return readers

    def get_image_names(self):
        keys = self._client.keys(UKAI_REDIS_DB_CONTENTS_DIR + '*')
        return [image_name[len(UKAI_REDIS_DB_CONTENTS_DIR) + 1:]
                for image_name in keys]

UKAI_ZK_DB_LOCKS_DIR    = '/ukai/metadata/locks'
UKAI_ZK_DB_CONTENTS_DIR = '/ukai/metadata/contents'
UKAI_ZK_DB_READERS_DIR  = '/ukai/metadata/readers'
UKAI_ZK_DB_WRITERS_DIR  = '/ukai/metadata/writers'
class UKAIZooKeeperDB(UKAIDB):
    '''The UKAIZooKeeperDB class provides an interface class to the
    ZooKeeper cluster.

    The resource layout in the cluster is as follows.

    /metadata/locks/IMAGE_NAMES
      IMAGE_NAMES are lock objects.
    /metadata/contents/IMAGE_NAMES
      each IMAGE_NAME has a JSON style metadata string.
    /metadata/readers/IMAGE_NAMES
      each IMAGE_NAME contains a list of IP addresses who open the disk
      image with a read right.
    /metadata/writers/IMAGE_NAMES
      each IMAGE_NAME contains an IP address who opens the disk image
      with a writing right.
    '''
    def __init__(self):
        super(UKAIZooKeeperDB, self).__init__()
        self._lock = threading.Lock()

    def connect(self, config):
        self._servers = config.get('metadata_servers')
        self._client = kazoo.client.KazooClient(hosts=self._servers)
        self._client.start()

        self._client.ensure_path(UKAI_ZK_DB_LOCKS_DIR)
        self._client.ensure_path(UKAI_ZK_DB_CONTENTS_DIR)
        self._client.ensure_path(UKAI_ZK_DB_READERS_DIR)
        self._client.ensure_path(UKAI_ZK_DB_WRITERS_DIR)

    def put_metadata(self, image_name, metadata):
        contents_file = UKAI_ZK_DB_CONTENTS_DIR + '/' + image_name
        try:
            self._lock.acquire()
            lock = self._client.Lock(UKAI_ZK_DB_LOCKS_DIR,
                                     image_name)
            with lock:
                if self._client.exists(contents_file) is None:
                    self._client.create(contents_file)
                self._client.set(contents_file, json.dumps(metadata))
        finally:
            self._lock.release()

    def get_metadata(self, image_name):
        contents_file = UKAI_ZK_DB_CONTENTS_DIR + '/' + image_name
        ret = None
        try:
            self._lock.acquire()
            lock = self._client.Lock(UKAI_ZK_DB_LOCKS_DIR,
                                     image_name)
            with lock:
                if self._client.exists(contents_file) is not None:
                    ret_json = self._client.get(contents_file)[0]
                    ret = json.loads(ret_json)
        finally:
            self._lock.release()
        return ret

    def delete_metadata(self, image_name):
        contents_file = UKAI_ZK_DB_CONTENTS_DIR + '/' + image_name
        try:
            self._lock.acquire()
            lock = self._client.Lock(UKAI_ZK_DB_LOCKS_DIR,
                                     image_name)
            with lock:
                if self._client.exists(contents_file) is None:
                    return
                self._client.delete(contents_file)
        finally:
            self._lock.release()

    def join_reader(self, image_name, node):
        readers_file = UKAI_ZK_DB_READERS_DIR + '/' + image_name
        try:
            self._lock.acquire()
            lock = self._client.Lock(UKAI_ZK_DB_LOCKS_DIR, image_name)
            with lock:
                readers = []
                if self._client.exists(readers_file) is None:
                    self._client.create(readers_file)
                else:
                    readers_json = self._client.get(readers_file)[0]
                    readers = json.loads(readers_json)
                if node not in readers:
                    readers.append(node)
                self._client.set(readers_file, json.dumps(readers))
        finally:
            self._lock.release()

    def leave_reader(self, image_name, node):
        readers_file = UKAI_ZK_DB_READERS_DIR + '/' + image_name
        try:
            self._lock.acquire()
            lock = self._client.Lock(UKAI_ZK_DB_LOCKS_DIR, image_name)
            with lock:
                if self._client.exists(readers_file) is None:
                    return
                readers_json = self._client.get(readers_file)[0]
                readers = json.loads(readers_json)
                if node in readers:
                    readers.remove(node)
                    if len(readers) == 0:
                        self._client.delete(readers_file)
                    else:
                        self._client.set(readers_file, json.dumps(readers))
        finally:
            self._lock.release()

    def get_readers(self, image_name):
        readers_file = UKAI_ZK_DB_READERS_DIR + '/' + image_name
        try:
            self._lock.acquire()
            lock = self._client.Lock(UKAI_ZK_DB_LOCKS_DIR, image_name)
            with lock:
                readers = []
                if self._client.exists(readers_file) is not None:
                    readers_json = self._client.get(readers_file)[0]
                    readers = json.loads(readers_json)
                return readers
        finally:
            self._lock.release()

    def get_image_names(self):
        try:
            self._lock.acquire()
            return self._client.get_children(UKAI_ZK_DB_CONTENTS_DIR)
        finally:
            self._lock.release()

# ukai_db_client = UKAIZooKeeperDB()
ukai_db_client = UKAIRedisDB()

if __name__ == '__main__':
    from ukai_config import UKAIConfig
    config = UKAIConfig()
#    db = UKAIZooKeeperDB()
    db = UKAIRedisDB()
    db.connect(config)
    db.put_metadata('test', 'hoge')
    print db.get_metadata('test')
    print db.get_image_names()
    db.delete_metadata('test')
    print db.get_readers('test')
    db.join_reader('test', '192.168.1.1')
    db.join_reader('test', '192.168.1.2')
    print db.get_readers('test')
    db.leave_reader('test', '192.168.1.1')
    db.leave_reader('test', '192.168.1.2')
    print db.get_readers('test')
