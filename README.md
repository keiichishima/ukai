![UKAI Logo](ukai_logo.png)

# UKAI: A Location Aware Distributed Storage for Virtual Machine Disk Images

## Copyright Notice

Copyright 2013, 2014
IIJ Innovation Institute Inc. All rights reserved.

Redistribution and use in source and binary forms, with or
without modification, are permitted provided that the following
conditions are met:

* Redistributions of source code must retain the above copyright
  notice, this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above
  copyright notice, this list of conditions and the following
  disclaimer in the documentation and/or other materials
  provided with the distribution.

THIS SOFTWARE IS PROVIDED BY IIJ INNOVATION INSTITUTE INC. ``AS
IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
SHALL IIJ INNOVATION INSTITUTE INC. OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.


## Introduction

UKAI is an implementation of the concept of 'Centrally Controllable
Distributed Local Storage' as a virtual machine disk image storage.
UKAI provides a filesystem interface to a hypervisor.  A hypervisor
uses the files stored in the UKAI filesystem as a virtual machine disk
image files.  The UKAI filesystem is not a POSIX filesystem.  It only
provides limited (but enough) set of filesystem operations to a
hypervisor for a virtual machine disk image operation.

UKAI is a kind of distributed storage system.  Unlike other major
distributed file/storage systems, the UKAI filesystem is *NOT* an
*AUTONOMOUS* distributed system.  It is a fully manual controlled
distributed filesystem.  You must configure which file should be
stored on which storage node.  In other words, you have full control
on data placement/replacement which is sometimes important when naive
management is required.

For example, assuming that you have geographically distributed
datacenters for virtualization services.  In that case, you may want
to migrate a virtual machie from one hypervisor to a different
hypervisor which is located far place geographically.  The disk image
of the virtual machine must be accessible from any location, but
probably you want some locallity to achieve better disk I/O
performance, or to avoid network access failure to a remote storage.
Using the UKAI filesystem, you can collect your virtual machine disk
image data to UKAI storage nodes located near to the running virtual
machine.


## Installation

UKAI is a Python program built on top of the [FUSE][] mechanism.  The
following software modules are required to run the UKAI filesystem.

* [Python][python]: You know.
* [FUSE][fuse]: Filesystem in Userspace.
* [fusepy][fusepy]: A Python module that provides a simple interface
  to FUSE.
* [netifaces][netifaces]: A portable library to access network
  interfaces from Python.
* [Redis][redis]: A fast key-value service.

In some environment (e.g. Ubuntu), you may need to join the 'fuse'
group and may need to configure the '/etc/fuse.conf' file to include
the following line to disclose the filesystem to other users than the
user running the UKAI filesystem.

    user_allow_other

Run the `setup.py` script to install the UKAI package and related
scripts.

    $ sudo python setup.py install

The script installs the ukai modules to the third party package
location defined in your Python environment, and installs scripts
to your local install prefix.


## Configuration

Before running the UKAI filesystem, you need to create a config
file at `/etc/ukai/config`.  The file is a kind of a JSON file.
The following parameters can be configured.

* `id`: IPv4 address of a local node.
* `metadata_servers`: The list of addresses of metadata servers
  that keep disk metadata information.  At this moment, only one
  address can be specified.  You need to prepare a Redis server
  with this addresses.
  Example:
    "metadata_servers":"172.16.0.1"
* `data_root`: The path where virtual machine disk image data is
  stored.
* `blockname_format`: The filename format of each piece of blocks.
* `core_server`: The address of the UKAI server.
* `core_port`: The port number of the UKAI server.
* `create_default`: This is a JSON dictionary key to specify
  parameters when creating a new disk image.
  * `block_size`: The default block size of a newly created disk
    image.

The below is a sample configuration file.

    {
        "id": "10.0.0.5",
        "metadata_servers": "10.0.10.1",
        "core_server": "10.0.0.5",
        "core_port": 22221,
        "data_root": "/var/ukai/data",
        "blockname_format": "%08d",
        "ifaddr_cache": true
    }


## Start the UKAI filesystem

To start the UKAI filesystem, you call the `ukai_server` command
installed in your local executable directory (e.g. `/usr/local/bin/`).
You need to run a UKAI server on all the nodes you store your disk
image data and hypervisors as well.

    $ sudo ukai_server

Next, you need to expose the storage system by mounting the data to a
directory somewhere by using the `ukai_fuse` command.

    $ sudo ukai_fuse /ukai


## Prepare a Disk Image

The metadata of a disk image is represented in a JSON format like
below.

    {
        "name": "image01",
        "size": 8000000000,
        "used_size": 8000000000,
        "block_size": 5000000,
        "hypervisors": {
            "192.0.2.100": {
                "sync_status": 0
            }
        },
        "blocks": [
            {
                "192.0.2.100": {
                    "sync_status": 0
                }
            },
            {
                "192.0.2.100": {
                    "sync_status": 0
                }
            }
            (continue 1598 times)
        ]
    }

The `name` key is a name of the disk image.  The `size` key is the
total size of the disk image.  In the above example, 8GB disk is
defined.  The `block_size` key is the size of a block.  The disk image
will be divided into pieces of files based on the value of the
`block_size` key.  The `hypervisors` key is a list of hypervisors on
which a virtual machine that uses this disk image runs.  If you are
planning migration of a virtual machine, you need to list all the
possible destination hypervisors in this list.  The `blocks` key is a
list of location information of each block.  Each block can have
multiple UKAI remote sotrage endpoint.  The 'sync_status' value shows
the status if the block data of the node is in-sync or out-of-sync.  0
means in-sync, and 2 means out-of-sync.

A metadata file can be created with the `ukai_admin` command.  For
example, to generate the same disk image as in the example above,
issue the following command.

    $ ukai_admin create_image -s 8000000000 -b 5000000 -h 192.0.2.100 -l 192.0.2.100 image01

The metadata information is stored on a metadata server on which you
specified by the `metadata_servers` parameter in the config file.


## Using as a Disk Image

The image file can be used directly as a disk image file.  The
following is an example of the libvirt style disk definition.

    <disk type='file' device='disk'>
      <driver name='qemu' type='raw'/>
      <source file='/ukai/image01'/>
      <target dev='vda' bus='virtio'/>
      <alias name='virtio-disk0'/>
      <address type='pci' domain='0x0000' bus='0x00' slot='0x05' function='0x0'/>
    </disk>


## Note about Migration

When you want to migrate a virtual machine from one hypervisor to
another hypervisor which is using the UKAI system, you must run the
UKAI system on both hypervisors.


## Utility Commands

To manage UKAI disk images, you need to use the `ukai_admin` command.
The command has several subcommands based on the type of operations.


### Create a Disk Image

The `create_image` subcommand generates a virtual disk image.  Before
using this command, you have to start a UKAI server.

    Usage: ukai_admin create_image -s SIZE -b BLOCK_SIZE -h HYPERVISOR -l LOCATION IMAGE_NAME


### Destroy a Disk Image

The `destroy_image` subcommand destroys a virtual disk image.

    Usage: ukai_admin destroy_image IMAGE_NAME


### List images

The `get_image_names` subcommand will print all the virtual disk images
curently registered in the UKAI filesystem.

    Usage: ukai_admin get_image_names


### Get Image Information

The `get_image_info` command displays information of a virtual disk.
You can view the metadata information (name, size, block_size, and
location information) of the specified virtual disk.

    Usage: ukai_admin get_image_info IMAGE_NAME


### Add Location Information

The `add_location` subcommand adds a new location to an existing
virtual disk image.  The initial synchronization status of the new
location is set to out-of-sync.

    Usage: ukai_admin add_location IMAGE_NAME LOCATION


### Remove Location Information

The `remove_location` subcommand removes location information from an
existing virtual disk image.  If the location is the only location
that have a in-sync state in a block, then the removal will not
executed.

    Usage: ukai_admin remove_location IMAGE_NAME LOCATION


### Synchronize locations

The `synchronize` subcommand synchronizes the out-of-sync data to the
latest in-sync data.  Since synchronize operation takes some time, you
can specify the range of blocks to synchronize with parameters.

    Usage: ukai_admin synchronize [-s START_BLOCK] [-e END_BLOCK] IMAGE_NAME

If you ommit the `-s` parameter then `0` is assumed.  If you ommit the
`-e` parameter, then the last block number is automatically specified.


### Get a List of Failure Nodes

The `get_error_state` subcommand displays the list of nodes which are
not available.  The list does not mean all the other nodes specified
in virtual disk images are available.  Some nodes may also be
unavailable.  When the UKAI system tries to contact them, then they
will also be listed in the list.

    Usage: ukai_admin get_error_state


### Get statistics

The `get_statistics` subcommand shows the I/O statistics of a
specified virtual disk image.

    Usage: ukai_admin get_statistics IMAGE_NAME


## Publication

* Keiichi Shima, "UKAI: Centrally Controllable Distributed Local
  Storage for Virtual Machine Disk Images", In Proceedings IEEE
  Globecom 2013 Workshop on Cloud Computing Systems, Networks, and
  Applications (CCSNA), December 9, 2013.
  <http://member.wide.ad.jp/~shima/publications/20131209-ccsna-ukai.pdf>

__________________________________________________________
[python]: http://www.python.org/
  "Python Programming Language"
[fuse]: http://fuse.sourceforge.net/
  "FUSE: Filesystem in Userspace"
[fusepy]: https://github.com/terencehonles/fusepy/
  "A Python module that provides a simple interface to FUSE and MacFUSE"
[netifaces]: http://alastairs-place.net/projects/netifaces/
  "Portable access to network interfaces from Python"
[redis]: http://redis.io/
  "Redis: an open source, BSD licensed, advanced key-value cache and
  store."
