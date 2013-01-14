
Homepage:
    http://www.steve.org.uk/Software/redisfs/

Mercurial Repository:
    http://redisfs.repository.steve.org.uk/



Introduction
------------

Redis is a high-performance key/value store which allows storing
keys, and their values, in a fast memory-backed store.

Intuitively it might seem that basing a filesystem around a simple
key=value store isn't such a practical endeavour, but happily redis
also has several useful abilities built-in, including native support
for other data-structures including:

* sets.
* hashes.
* lists.

Using only key&value pairs, and the notion of a set, we can implement
a simple filesystem, which is ready for replication and snapshotting.



Storage
-------

This filesystem makes use of two of redis's abilities:

* KEY storage.
* SET storage.

Our filesystem is built around the notion that a directory contains
entries, and these entries are members of a set named after the parent
directory.

For example consider the directory tree which contains two entries:

1. /mnt/redis/foo/
2. /mnt/redis/README

For each filesystem entry, be it a file, symlink, or a directory, we
allocate a unique identifier which we called an "INODE number".
This may be used to store, retrieve, and search for information:

   INODE:1:UID   -> The owner of the inode with ID 1.
   INODE:1:GID   -> The owner of the inode with ID 1.
   INODE:1:SIZE  -> The size of the object with inode #1.
   INODE:1:NAME  -> The name of the object with inode #1.
   ..

So we might see this:

   INODE:1:NAME => "foo"
   INODE:1:MODE => "0755"
   INODE:1:GID  => "0"
   INODE:1:UID  => "0"
   INODE:1:TYPE => "dir"
   ..

Similarly the entry for "/README" might look like this:

   INODE:2:NAME => "README"
   INODE:2:MODE => "0644"
   INODE:2:GID  => "0"
   INODE:2:UID  => "0"
   INODE:2:TYPE => "file"

The actual contents of a directory are stored in a set, which has
a name based upon the inode of the parent directory.  For example:

   SMEMBERS DIRENT:3 -> { "5", "6" }


In actual fact we add a prefix to each key and set name, which allows
multiple filesystems to be mounted at the same time - and which is
the key to our snapshotting facility.


Getting Started
---------------

To build the code, assuming you have the required build dependencies
installed, you should merely need to run:

     make
     make test

Once built the software can be installed via:

     make install

It is possible to run the filesystem without having installed the
software, via:

     # ./src/redisfs
Connecting to redis server localhost:6379 and mounting at /mnt/redis.

By default this will attempt to connect to a redis server running upon
the same host - if you wish to connect to a remote machine please execute:

     # ./src/redisfs --host remote.example.org [--port=6379]


Steve
--
