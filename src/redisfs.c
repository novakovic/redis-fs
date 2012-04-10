/* redisfs.c -- Simple redis-based filesystem.
 *
 *
 * Copyright (c) 2010-2011, Steve Kemp <steve@steve.org.uk>
 * All rights reserved.
 *
 * http://steve.org.uk/
 *
 */



/**
 *  This filesystem is written using FUSE and is pretty basic.  There
 * is support for all the common operations which you might expect
 * with the exception that hard-links are not supported.
 *
 * (Symbolic links / symlinks are supported though.)
 *
 *
 * FILES
 *
 *********
 *
 *  Each file which is created is allocated a unique numeric identifier,
 * and from that we store the meta-data of the entry itself inside redis keys.
 *
 *  For example the file "/etc/passwd" might have the identifier "6".
 * which would lead to keys such as:
 *
 * SKX:INODE:6:NAME   => "passwd"
 * SKX:INODE:6:TYPE   => "file"
 * SKX:INODE:6:MODE   => "644"
 * SKX:INODE:6:GID    => "0"
 * SKX:INODE:6:UID    => "0"
 * SKX:INODE:6:SIZE   => "1688"
 * SKX:INODE:6:ATIME  => "1234567"
 * SKX:INODE:6:CTIME  => "1234567"
 * SKX:INODE:6:MTIME  => "1234567"
 * SKX:INODE:6:LINK   => 1   [symlink count]
 * SKX:INODE:6:TARGET => ""   [symlink destination]
 *
 *  (Here "SKX:" is the key-prefix.  We need to allow this such that
 * more than one filesystem may be mounted against a single redis-server.)
 *
 *
 * Directories
 *
 **************
 *
 *  Handling directories is a little more complex, and we rely upon
 * redis "sets" to do the heavy lifting.  Rather than have directories
 * be based upon filesystem paths we handle them in a similar fashion to
 * directory entries.
 *
 *  Given a directory "/foo" we find the INODE for that entry, and
 * assuming that is "43" then the elements of that directory are
 * stored in the set:
 *
 *  SKX:DIRENT:43
 *
 *
 * </overview>
 *
 */

#define FUSE_USE_VERSION 26


#include <fuse.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <net/if.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <getopt.h>
#include <stdarg.h>


#include "hiredis.h"
#include "pathutil.h"




/**
 * The mount-point of the filesystem.
 *
 */
char _g_mount[200] = { "/mnt/redis" };


/**
 * Global prefix for all keys we set inside redis.
 *
 */
char _g_prefix[10] = { "skx" };


/**
 * Mutex for safety.
 */
pthread_mutex_t _g_lock = PTHREAD_MUTEX_INITIALIZER;


/**
 * Handle to the redis server.
 */
redisContext *_g_redis = NULL;


/**
 * The host and port of the redis server we're connecting to.
 */
int _g_redis_port = 6379;
char _g_redis_host[100] = { "localhost" };


/**
 * Are we running with --debug in play?
 */
int _g_debug = 0;


/**
 * Are we running with --fast in play?
 */
int _g_fast = 0;


/**
 * Is our file-system (intentionally) read-only?
 */
int _g_read_only = 0;




/**
 * If our service isn't alive then connect to it.
 */
void
redis_alive()
{
    struct timeval timeout = { 1, 5000000 };    // 1.5 seconds
    redisReply *reply = NULL;

    /**
     * If we have a handle see if it is alive.
     */
    if (_g_redis != NULL)
    {
        reply = redisCommand(_g_redis, "PING");

        if ((reply != NULL) &&
            (reply->str != NULL) && (strcmp(reply->str, "PONG") == 0))
        {
            freeReplyObject(reply);
            return;
        }
        else
        {
            if (reply != NULL)
                freeReplyObject(reply);
        }
    }

    /**
     * OK we have no handle, create a connection to the server.
     */
    _g_redis = redisConnectWithTimeout(_g_redis_host, _g_redis_port, timeout);
    if (_g_redis == NULL)
    {
        fprintf(stderr, "Failed to connect to redis on [%s:%d].\n",
                _g_redis_host, _g_redis_port);
        exit(1);
    }
    else
    {
        if (_g_debug)
            fprintf(stderr, "Reconnected to redis server on [%s:%d]\n",
                    _g_redis_host, _g_redis_port);
    }
}


/**
 * Called when our filesystem is created.
 *
 * Create our mutex, and establish a connection to redis.
 */
void *
fs_init()
{
    if (_g_debug)
        fprintf(stderr, "fs_init()\n");

    pthread_mutex_init(&_g_lock, NULL);

    return 0;
}


/**
 * This is called when our filesystem is destroyed.
 *
 * Destroy our mutex.
 */
void
fs_destroy()
{
    if (_g_debug)
        fprintf(stderr, "fs_destroy()\n");

    pthread_mutex_destroy(&_g_lock);
}


/**
 * Get the next INODE number to be used for a new file/directory.
 *
 * Returns -1 on failure.
 */
int
get_next_inode()
{
    redisReply *reply = NULL;
    int val = -1;

    redis_alive();

    reply = redisCommand(_g_redis, "INCR %s:GLOBAL:INODE", _g_prefix);
    if ((reply != NULL) && (reply->type == REDIS_REPLY_INTEGER))
        val = reply->integer;
    freeReplyObject(reply);

    return (val);
}


/**
 * Remove all meta-data associated with an INODE.
 *
 * e.g. size, owner, mtime, ctime.
 */
void
remove_inode(int inode)
{
    redisReply *reply = NULL;

    char *names[] = {
        "DEL %s:INODE:%d:NAME", /* basename of file/dir */
        "DEL %s:INODE:%d:TYPE", /* "FILE", "DIR", or "LINK" */
        "DEL %s:INODE:%d:MODE", /* file mode */
        "DEL %s:INODE:%d:GID",  /* GID of owner */
        "DEL %s:INODE:%d:UID",  /* UID of owner */
        "DEL %s:INODE:%d:ATIME",        /* access-time */
        "DEL %s:INODE:%d:CTIME",        /* create time */
        "DEL %s:INODE:%d:MTIME",        /* modification time */
        "DEL %s:INODE:%d:SIZE", /* size of a file */
        "DEL %s:INODE:%d:DATA", /* data stored in a file */
        "DEL %s:INODE:%d:LINK", /* link-count */
        "DEL %s:INODE:%d:TARGET",       /* destination of symlink */
        NULL
    };

    int i = 0;


    /**
     * Can't happen.
     */
    if (inode < 0)
    {
        fprintf(stderr, "Tried to free an unset inode.\n");
        return;
    }

    redis_alive();

    /**
     * append the deletion commands, in a batch.
     */
    while (names[i] != NULL)
    {
        redisAppendCommand(_g_redis, names[i], _g_prefix, inode);
        i += 1;
    }

    /**
     * Fetch the results back.
     */
    i = 0;
    while (names[i] != NULL)
    {
        redisGetReply(_g_redis, (void **)&reply);
        freeReplyObject(reply);
        i += 1;
    }
}


/**
 * Find the inode for a filesystem entry, by path.
 */
int
find_inode(const char *path)
{
    int val = -1;
    int parent_inode = 0;
    char *parent;
    char *entry;
    redisReply *reply = NULL;

    if (_g_debug)
        fprintf(stderr, "find_inode(%s)\n", path);

    /**
     * Special Case "/" is 99.
     */
    if ((strcmp(path, "/") == 0) && strlen(path) == 1)
        return -99;

    redis_alive();

  /**
   * OK we have a directory entry.
   *
   * We need to find the entries in the parent directory
   * and then we can lookup the entry itself.
   */
    parent = get_parent(path);
    parent_inode = find_inode(parent);
    entry = get_basename(path);


  /**
   * For each entry in the set we need to add the name.
   */
    reply =
        redisCommand(_g_redis, "SMEMBERS %s:DIRENT:%d", _g_prefix,
                     parent_inode);

    char *memcommand = malloc(1048576);
    sprintf(memcommand, "MGET");
    if ((reply != NULL) && (reply->type == REDIS_REPLY_ARRAY))
    {
        int i;
        for (i = 0; i < reply->elements; i++)
        {
            sprintf(memcommand + strlen(memcommand), " %s:INODE:%s:NAME",
                    _g_prefix, reply->element[i]->str);
        }
        redisReply *r = NULL;
        r = redisCommand(_g_redis, memcommand);
        for (i = 0; i < reply->elements; i++)
        {
            if ((r->element[i] != NULL)
                && (strcmp(r->element[i]->str, entry) == 0))
            {
                val = atoi(reply->element[i]->str);
            }
        }
        freeReplyObject(r);
    }

    freeReplyObject(reply);

    free(memcommand);
    free(parent);
    free(entry);

    if (_g_debug)
        fprintf(stderr, "find_inode(%s) -> %d\n", path, val);

    return (val);
}


/**
 * Is the given "thing" a directory?
 */
int
is_directory(const char *path)
{
    int ret = 0;
    redisReply *reply = NULL;

    if (_g_debug)
        fprintf(stderr, "is_directory(%s)\n", path);

    redis_alive();

  /**
   * Find the inode.
   */
    int inode = find_inode(path);
    if (inode == -1)
        return -1;

    reply = redisCommand(_g_redis, "GET %s:INODE:%d:TYPE", _g_prefix, inode);

    if ((reply != NULL) && (reply->type == REDIS_REPLY_STRING)
        && (strcmp(reply->str, "DIR") == 0))
        ret = 1;

    freeReplyObject(reply);

    return (ret);
}


/**
 * Return the number of directory entries a directory has.
 */
int
count_directory_entries(const char *path)
{
    int ret = 0;
    int inode = 0;
    redisReply *reply = NULL;

    if (_g_debug)
        fprintf(stderr, "count_directory_entries(%s)\n", path);

    redis_alive();

  /**
   * Find the inode of the directory.
   */
    inode = find_inode(path);

  /**
   * Now retrieve the entries.
   */
    reply = redisCommand(_g_redis, "SMEMBERS %s:DIRENT:%d", _g_prefix, inode);

    /**
     * If that worked we know the number of elements.
     */
    if ((reply != NULL) && (reply->type == REDIS_REPLY_ARRAY))
    {
        ret = reply->elements;
    }
    freeReplyObject(reply);

    return (ret);
}


/**
 * Our readdir implementation.
 *
 * We have a SET of entries for each directory, stored under a key which
 * is named "$PREFIX:/$PATH".
 *
 * So to read all file entries beneath "/" we'd look for members of the
 * set "SKX:/", and to look for entries stored beneath /tmp we'd search
 * for members of the set "SKX:/tmp".
 *
 */
static int
fs_readdir(const char *path,
           void *buf,
           fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
    redisReply *reply = NULL;
    int i;
    int inode;


    pthread_mutex_lock(&_g_lock);

    if (_g_debug)
        fprintf(stderr, "fs_readdir(%s)\n", path);

    redis_alive();

    /**
     * Add the filesystem entries which always exist.
     */
    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    /**
     * For each entry in the set ..
     */
    inode = find_inode(path);
    if (inode == -1)
    {
        pthread_mutex_unlock(&_g_lock);
        return 0;
    }


    reply = redisCommand(_g_redis, "SMEMBERS %s:DIRENT:%d", _g_prefix, inode);

    char *memcommand = malloc(1048576);
    sprintf(memcommand, "MGET");
    if ((reply != NULL) && (reply->type == REDIS_REPLY_ARRAY))
    {
      /**
       * OK the set exists and has members.
       *
       * We need to iterate over each one, and get the name of the
       * directory entry.
       */
        for (i = 0; i < reply->elements; i++)
        {
            char *name = reply->element[i]->str;
            sprintf(memcommand + strlen(memcommand), " %s:INODE:%s:NAME",
                    _g_prefix, name);
        }
        redisReply *r = NULL;
        r = redisCommand(_g_redis, memcommand);
        for (i = 0; i < reply->elements; i++)
        {

            if ((r->element[i] != NULL))
                filler(buf, strdup(r->element[i]->str), NULL, 0);

        }
        freeReplyObject(r);
    }

    free(memcommand);

    freeReplyObject(reply);

    pthread_mutex_unlock(&_g_lock);
    return 0;
}


/**
 * Get the attributes of each file.
 */
static int
fs_getattr(const char *path, struct stat *stbuf)
{
    int inode;
    redisReply *reply = NULL;

    pthread_mutex_lock(&_g_lock);

    if (_g_debug)
        fprintf(stderr, "fs_getattr(%s);\n", path);

    redis_alive();

    memset(stbuf, 0, sizeof(struct stat));

    /**
     * Handle root directory first, as a special-case.
     */
    if (strcmp(path, "/") == 0)
    {
        stbuf->st_atime = time(NULL);
        stbuf->st_mtime = stbuf->st_atime;
        stbuf->st_ctime = stbuf->st_atime;
        stbuf->st_uid = getuid();
        stbuf->st_gid = getgid();

        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 1;

        pthread_mutex_unlock(&_g_lock);
        return 0;
    }


    /**
     * OK a real lookup.
     */
    inode = find_inode(path);
    if (inode == -1)
    {
      /**
       * File/Directory not found.
       */
        pthread_mutex_unlock(&_g_lock);
        return -ENOENT;
    }


    /**
     * Setup atime, mtime, ctime, and owner/gid.
     */
    redisAppendCommand(_g_redis,
                       "MGET %s:INODE:%d:CTIME %s:INODE:%d:ATIME %s:INODE:%d:MTIME %s:INODE:%d:GID %s:INODE:%d:UID %s:INODE:%d:LINK",
                       _g_prefix, inode, _g_prefix, inode, _g_prefix, inode,
                       _g_prefix, inode, _g_prefix, inode, _g_prefix, inode,
                       _g_prefix, inode);
    redisGetReply(_g_redis, (void **)&reply);
    if ((reply->element[0] != NULL)
        && (reply->element[0]->type == REDIS_REPLY_STRING))
        stbuf->st_ctime = atoi(reply->element[0]->str);
    if ((reply->element[1] != NULL)
        && (reply->element[1]->type == REDIS_REPLY_STRING))
        stbuf->st_atime = atoi(reply->element[1]->str);
    if ((reply->element[2] != NULL)
        && (reply->element[2]->type == REDIS_REPLY_STRING))
        stbuf->st_mtime = atoi(reply->element[2]->str);
    if ((reply->element[3] != NULL)
        && (reply->element[3]->type == REDIS_REPLY_STRING))
        stbuf->st_gid = atoi(reply->element[3]->str);
    if ((reply->element[4] != NULL)
        && (reply->element[4]->type == REDIS_REPLY_STRING))
        stbuf->st_uid = atoi(reply->element[3]->str);
    if ((reply->element[5] != NULL)
        && (reply->element[5]->type == REDIS_REPLY_STRING))
        stbuf->st_nlink = atoi(reply->element[4]->str);
    if ((reply->element[6] != NULL)
        && (reply->element[6]->type == REDIS_REPLY_STRING))
        stbuf->st_ctime = atoi(reply->element[5]->str);
    freeReplyObject(reply);

    /**
     *  Type - batch up some commands here for later use
     */
    reply =
        redisCommand(_g_redis,
                     "MGET %s:INODE:%d:TYPE %s:INODE:%d:MODE %s:INODE:%d:SIZE",
                     _g_prefix, inode, _g_prefix, inode, _g_prefix, inode);
    if ((reply != NULL) && (reply->element[0] != NULL)
        && (reply->element[0]->type == REDIS_REPLY_STRING))
    {

        if ((reply->element[1] != NULL)
            && (reply->element[1]->type == REDIS_REPLY_STRING))
        {
            stbuf->st_mode = atoi(reply->element[1]->str);
        }

        if (strcmp(reply->element[0]->str, "DIR") == 0)
        {
            stbuf->st_mode |= S_IFDIR;
        }
        else if (strcmp(reply->element[0]->str, "LINK") == 0)
        {
            stbuf->st_mode |= S_IFLNK;
            stbuf->st_nlink = 1;
            stbuf->st_size = 0;
        }
        else if (strcmp(reply->element[0]->str, "FILE") == 0)
        {
            if ((reply->element[2] != NULL)
                && (reply->element[2]->type == REDIS_REPLY_STRING))
            {
                if (_g_debug)
                    fprintf(stderr, "found file\n");
                stbuf->st_size = atoi(reply->element[2]->str);
            }
        }
        else
        {
            if (_g_debug)
                fprintf(stderr, "UNKNOWN ENTRY TYPE: %s\n",
                        reply->element[0]->str);
        }
    }
    freeReplyObject(reply);


    pthread_mutex_unlock(&_g_lock);
    return 0;


}


/**
 * Make a directory.
 */
static int
fs_mkdir(const char *path, mode_t mode)
{
    redisReply *reply = NULL;
    char *parent = NULL;
    char *entry = NULL;
    int new_inode = 0;
    int parent_inode = 0;

    pthread_mutex_lock(&_g_lock);

    if (_g_debug)
        fprintf(stderr, "fs_mkdir(%s);\n", path);

    /**
     * If read-only mode is set this must fail.
     */
    if (_g_read_only)
    {
        pthread_mutex_unlock(&_g_lock);
        return -EPERM;
    }

    redis_alive();

    /**
     * We need to create a new INODE number & entry.
     *
     * Once that is done we can create the new entry
     * in the set for the parent directory.
     */
    parent = get_parent(path);
    entry = get_basename(path);
    new_inode = get_next_inode();
    parent_inode = find_inode(parent);

    /**
     * Add the entry to the parent directory.
     */
    redisAppendCommand(_g_redis, "SADD %s:DIRENT:%d %d", _g_prefix,
                       parent_inode, new_inode);

    /**
     * Now populate the new entry.
     */
    redisAppendCommand(_g_redis,
                       "MSET %s:INODE:%d:NAME %s %s:INODE:%d:TYPE DIR %s:INODE:%d:MODE %d %s:INODE:%d:UID %d %s:INODE:%d:GID %d %s:INODE:%d:SIZE %d %s:INODE:%d:CTIME %d %s:INODE:%d:MTIME %d %s:INODE:%d:ATIME %d %s:INODE:%d:LINK 1",
                       _g_prefix, new_inode, entry, _g_prefix, new_inode,
                       _g_prefix, new_inode, mode, _g_prefix, new_inode,
                       fuse_get_context()->uid, _g_prefix, new_inode,
                       fuse_get_context()->gid, _g_prefix, new_inode, 0,
                       _g_prefix, new_inode, time(NULL), _g_prefix, new_inode,
                       time(NULL), _g_prefix, new_inode, time(NULL),
                       _g_prefix, new_inode);
    int i = 0;
    for (i = 0; i < 2; i++)
    {
        redisGetReply(_g_redis, (void **)&reply);
        freeReplyObject(reply);
    }


    free(parent);
    free(entry);

    pthread_mutex_unlock(&_g_lock);
    return 0;
}


/**
 * Remove a directory entry.
 */
static int
fs_rmdir(const char *path)
{
    int parent_inode = 0;
    int inode = 0;
    redisReply *reply = NULL;
    char *parent = NULL;

    pthread_mutex_lock(&_g_lock);

    if (_g_debug)
        fprintf(stderr, "fs_rmdir(%s);\n", path);

    /**
     * If read-only mode is set this must fail.
     */
    if (_g_read_only)
    {
        pthread_mutex_unlock(&_g_lock);
        return -EPERM;
    }

    redis_alive();


    /**
     * Ensure we're working on a directory.
     */
    if (!is_directory(path))
    {
        pthread_mutex_unlock(&_g_lock);
        return -ENOENT;
    }


    /**
     * [1/4] Make sure the directory isn't empty.
     */
    if (count_directory_entries(path) != 0)
    {
        pthread_mutex_unlock(&_g_lock);
        return -ENOTEMPTY;
    }

    /**
     * To remove the entry we need to :
     *
     * [2/4] Find the inode for this entry.
     *
     */
    parent = get_parent(path);
    parent_inode = find_inode(parent);
    inode = find_inode(path);

    if (inode == -1)
    {
        free(parent);
        pthread_mutex_unlock(&_g_lock);
        return -ENOENT;
    }

    /**
     * [3/4] Remove from the directory of the parent.
     */
    reply =
        redisCommand(_g_redis, "SREM %s:DIRENT:%d %d", _g_prefix,
                     parent_inode, inode);
    freeReplyObject(reply);
    free(parent);

    /**
     * [4/4] Remove all meta-data.
     */
    remove_inode(inode);


    pthread_mutex_unlock(&_g_lock);
    return 0;
}


/**
 * Write to a file or path.
 */
static int
fs_write(const char *path,
         const char *buf,
         size_t size, off_t offset, struct fuse_file_info *fi)
{
    redisReply *reply = NULL;

    pthread_mutex_lock(&_g_lock);

    if (_g_debug)
        fprintf(stderr, "fs_write(%s);\n", path);

    /**
     * If read-only mode is set this must fail.
     */
    if (_g_read_only)
    {
        pthread_mutex_unlock(&_g_lock);
        return -EPERM;
    }

    redis_alive();

    int inode = find_inode(path);

    /**
     * Simplest case first.
     */
    if (offset == 0)
    {
        char *mem = malloc(size + 1);
        memcpy(mem, buf, size);

        if (_g_debug)
            fprintf(stderr, "fs_write->simple(%s);\n", path);

      /**
       * MSET inode's size and mtime, delete, set new data
       */
        redisAppendCommand(_g_redis,
                           "MSET %s:INODE:%d:SIZE %d %s:INODE:%d:MTIME %d %s:INODE:%d:DATA %b",
                           _g_prefix, inode, size, _g_prefix, inode,
                           time(NULL), _g_prefix, inode, mem, size);

        redisGetReply(_g_redis, (void **)&reply);
        freeReplyObject(reply);

        free(mem);
    }
    else
    {
        if (_g_debug)
            fprintf(stderr, "fs_write->offsetted(%s);\n", path);

        /**
         * Copy the new data.
         */
        char *mem = malloc(size);
        memcpy(mem, buf, size);

        /**
         * Now increment the size record and store contents.
         */
        redisAppendCommand(_g_redis, "INCRBY %s:INODE:%d:SIZE %d",
                           _g_prefix, inode, size);

        redisAppendCommand(_g_redis, "APPEND %s:INODE:%d:DATA %b",
                           _g_prefix, inode, mem, size);

        //don't store mtime if fast used
        if (!_g_fast)
        {
            redisAppendCommand(_g_redis, "SET %s:INODE:%d:MTIME %d",
                               _g_prefix, inode, time(NULL));
            redisGetReply(_g_redis, (void **)&reply);
            freeReplyObject(reply);
        }

        redisGetReply(_g_redis, (void **)&reply);
        freeReplyObject(reply);
        redisGetReply(_g_redis, (void **)&reply);
        freeReplyObject(reply);


        /**
         * Free the memory.
         */
        free(mem);

    }

    pthread_mutex_unlock(&_g_lock);
    return size;
}


/**
 * Read from a file.
 */
static int
fs_read(const char *path, char *buf, size_t size, off_t offset,
        struct fuse_file_info *fi)
{
    redisReply *reply = NULL;
    size_t sz;

    pthread_mutex_lock(&_g_lock);

    if (_g_debug)
        fprintf(stderr, "fs_read(%s);\n", path);

    redis_alive();

    /**
     * Find the inode.
     */
    int inode = find_inode(path);
    if (inode == -1)
    {
      /**
       * File/Directory not found.
       */
        pthread_mutex_unlock(&_g_lock);
        return -ENOENT;

    }

    /**
     * Get the current file size.
     */
    reply = redisCommand(_g_redis, "GET %s:INODE:%d:SIZE",
                         _g_prefix, inode, size);
    sz = atoi(reply->str);
    freeReplyObject(reply);

    if (sz < size)
        size = sz;
    if (offset + size > sz)
        size = sz - offset;

    /**
     * Get the file contents.
     * this is a pretty bad bottleneck here - it grabs the entire contents and then memcpy's the bit it likes
     *
     */
    reply =
        redisCommand(_g_redis, "GETRANGE %s:INODE:%d:DATA %lu %lu", _g_prefix,
                     inode, offset, size + offset);

    if ((reply != NULL) && (reply->type == REDIS_REPLY_ERROR))
    {
        /**
         * GETRANGE was renamed - so we'll free the previous command
         * and retry under the old name.
         */
        freeReplyObject(reply);

        reply =
            redisCommand(_g_redis, "SUBSTR %s:INODE:%d:DATA %lu %lu",
                         _g_prefix, inode, offset, size + offset);

    }

    /**
     * Copy the data into the callee's buffer.
     */
    if (size > 0)
        memcpy(buf, reply->str, size);

    freeReplyObject(reply);

    pthread_mutex_unlock(&_g_lock);
    return size;
}


/**
 * Create a symlink
 */
static int
fs_symlink(const char *target, const char *path)
{

    redisReply *reply = NULL;
    char *parent = NULL;
    char *entry = NULL;
    int key = 0;
    int parent_inode = 0;

    pthread_mutex_lock(&_g_lock);

    if (_g_debug)
        fprintf(stderr, "fs_symlink(target:%s -> %s);\n", target, path);

    /**
     * If read-only mode is set this must fail.
     */
    if (_g_read_only)
    {
        pthread_mutex_unlock(&_g_lock);
        return -EPERM;
    }

    redis_alive();

    /**
     * We need to create a new INODE number & entry.
     *
     * Once that is done we can create the new entry
     * in the set for the parent directory.
     */
    parent = get_parent(path);
    parent_inode = find_inode(parent);

    entry = get_basename(path);
    key = get_next_inode();


    /**
     * Add the entry to the parent directory.
     */
    redisAppendCommand(_g_redis, "SADD %s:DIRENT:%d %d", _g_prefix,
                       parent_inode, key);

    /**
     * Now populate the new entry.
     */
    redisAppendCommand(_g_redis, "SET %s:INODE:%d:NAME %s",
                       _g_prefix, key, entry);
    redisAppendCommand(_g_redis, "SET %s:INODE:%d:TYPE LINK", _g_prefix, key);
    redisAppendCommand(_g_redis, "SET %s:INODE:%d:TARGET %s",
                       _g_prefix, key, target);
    redisAppendCommand(_g_redis, "SET %s:INODE:%d:MODE %d",
                       _g_prefix, key, 0444);
    redisAppendCommand(_g_redis, "SET %s:INODE:%d:UID %d",
                       _g_prefix, key, fuse_get_context()->uid);
    redisAppendCommand(_g_redis, "SET %s:INODE:%d:GID %d",
                       _g_prefix, key, fuse_get_context()->gid);
    redisAppendCommand(_g_redis, "SET %s:INODE:%d:SIZE %d",
                       _g_prefix, key, 0);
    redisAppendCommand(_g_redis, "SET %s:INODE:%d:CTIME %d",
                       _g_prefix, key, time(NULL));
    redisAppendCommand(_g_redis, "SET %s:INODE:%d:MTIME %d",
                       _g_prefix, key, time(NULL));
    redisAppendCommand(_g_redis, "SET %s:INODE:%d:ATIME %d",
                       _g_prefix, key, time(NULL));
    redisAppendCommand(_g_redis, "SET %s:INODE:%d:LINK 1", _g_prefix, key);

    int i = 0;
    for (i = 0; i < 11; i++)
    {
        redisGetReply(_g_redis, (void **)&reply);
        freeReplyObject(reply);
    }

    free(parent);
    free(entry);

    pthread_mutex_unlock(&_g_lock);
    return 0;
}


/**
 * Read the target of a symlink.
 */
static int
fs_readlink(const char *path, char *buf, size_t size)
{
    int inode;
    redisReply *reply = NULL;

    pthread_mutex_lock(&_g_lock);

    if (_g_debug)
        fprintf(stderr, "fs_readlink(%s);\n", path);

    redis_alive();

    /**
     * To resolve the symlink we must:
     *
     * [1/2] Find the inode for this entry.
     *
     */
    inode = find_inode(path);
    if (inode == -1)
    {
        pthread_mutex_unlock(&_g_lock);
        return -ENOENT;
    }

    /**
     * [2/2] Lookup the "TARGET" data item.
     */
    reply = redisCommand(_g_redis, "GET %s:INODE:%d:TARGET",
                         _g_prefix, inode);

    if ((reply != NULL) && (reply->type == REDIS_REPLY_STRING) &&
        (reply->str != NULL))
    {
        strcpy(buf, (char *)reply->str);
        freeReplyObject(reply);
        pthread_mutex_unlock(&_g_lock);
        return 0;
    }
    freeReplyObject(reply);
    pthread_mutex_unlock(&_g_lock);

    return (-ENOENT);
}


/**
 * This cheats and always returns true.
 *
 * (Without the ability to open you cannot read/write to a file.)
 *
 */
static int
fs_open(const char *path, struct fuse_file_info *fi)
{

    int inode;
    redisReply *reply = NULL;

    if (_g_debug)
        fprintf(stderr, "fs_open(%s);\n", path);


    /**
     * If we're running with --fast just return, and don't
     * update the atime.
     */
    if (_g_fast)
        return 0;

    pthread_mutex_lock(&_g_lock);

  /**
   * Update the access time of a file.
   */
    inode = find_inode(path);
    if (inode == -1)
    {
        pthread_mutex_unlock(&_g_lock);
        return 0;
    }


    reply = redisCommand(_g_redis, "SET %s:INODE:%d:ATIME %d",
                         _g_prefix, inode, time(NULL));
    freeReplyObject(reply);


    pthread_mutex_unlock(&_g_lock);

    return 0;
}


/**
 * Create a new entry with the specified mode.
 */
static int
fs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{

    redisReply *reply = NULL;
    char *parent = NULL;
    char *entry = NULL;
    int key = 0;
    int parent_inode = 0;

    pthread_mutex_lock(&_g_lock);

    if (_g_debug)
        fprintf(stderr, "fs_create(%s);\n", path);

    /**
     * If read-only mode is set this must fail.
     */
    if (_g_read_only)
    {
        pthread_mutex_unlock(&_g_lock);
        return -EPERM;
    }

    redis_alive();

    /**
     * We need to create a new INODE number & entry.
     *
     * Once that is done we can create the new entry
     * in the set for the parent directory.
     */
    parent = get_parent(path);
    parent_inode = find_inode(parent);

    entry = get_basename(path);
    key = get_next_inode();


    /**
     * Add the entry to the parent directory.
     */
    redisAppendCommand(_g_redis, "SADD %s:DIRENT:%d %d", _g_prefix,
                       parent_inode, key);

    /**
     * Now populate the new entry, using MSET
     */
    redisAppendCommand(_g_redis,
                       "MSET %s:INODE:%d:NAME %s %s:INODE:%d:TYPE FILE %s:INODE:%d:MODE %d %s:INODE:%d:UID %d %s:INODE:%d:GID %d %s:INODE:%d:SIZE %d %s:INODE:%d:CTIME %d %s:INODE:%d:MTIME %d %s:INODE:%d:ATIME %d %s:INODE:%d:LINK 1",
                       _g_prefix, key, entry, _g_prefix, key, _g_prefix, key,
                       mode, _g_prefix, key, fuse_get_context()->uid,
                       _g_prefix, key, fuse_get_context()->gid, _g_prefix,
                       key, 0, _g_prefix, key, time(NULL), _g_prefix, key,
                       time(NULL), _g_prefix, key, time(NULL), _g_prefix,
                       key);

    int i = 0;
    for (i = 0; i < 2; i++)
    {
        redisGetReply(_g_redis, (void **)&reply);
        freeReplyObject(reply);
    }

    free(parent);
    free(entry);

    pthread_mutex_unlock(&_g_lock);
    return 0;
}


/**
 * Change the owner of a file/directory.
 */
static int
fs_chown(const char *path, uid_t uid, gid_t gid)
{
    int inode;
    redisReply *reply = NULL;

    pthread_mutex_lock(&_g_lock);

    if (_g_debug)
        fprintf(stderr, "fs_chown(%s);\n", path);

    /**
     * If read-only mode is set this must fail.
     */
    if (_g_read_only)
    {
        pthread_mutex_unlock(&_g_lock);
        return -EPERM;
    }

    redis_alive();

    /**
     * To change the ownership of this file we need to :
     *
     * [1/2] Find the inode for this entry.
     *
     */
    inode = find_inode(path);
    if (inode == -1)
    {
        pthread_mutex_unlock(&_g_lock);
        return -ENOENT;
    }

    /**
     * [2/2] Change the UID, GID, mtime
     */
    reply =
        redisCommand(_g_redis,
                     "MSET %s:INODE:%d:UID %d %s:INODE:%d:GID %d %s:INODE:%d:MTIME %d",
                     _g_prefix, inode, uid, _g_prefix, inode, gid, _g_prefix,
                     inode, time(NULL));
    freeReplyObject(reply);

    /**
     * All done.
     */
    pthread_mutex_unlock(&_g_lock);
    return 0;

}


/**
 * Change the permission(s) of a file/directory.
 */
static int
fs_chmod(const char *path, mode_t mode)
{
    int inode;
    redisReply *reply = NULL;

    pthread_mutex_lock(&_g_lock);

    if (_g_debug)
        fprintf(stderr, "fs_chmod(%s);\n", path);

    /**
     * If read-only mode is set this must fail.
     */
    if (_g_read_only)
    {
        pthread_mutex_unlock(&_g_lock);
        return -EPERM;
    }

    redis_alive();

    /**
     * To change the owner of this file we need to:
     *
     * [1/2] Find the inode for this entry.
     *
     */
    inode = find_inode(path);
    if (inode == -1)
    {
        pthread_mutex_unlock(&_g_lock);
        return -ENOENT;
    }

    /**
     * [2/2] Change the mode
     */
    reply =
        redisCommand(_g_redis,
                     "MSET %s:INODE:%d:MODE %d %s:INODE:%d:MTIME %d",
                     _g_prefix, inode, mode, _g_prefix, inode, time(NULL));
    freeReplyObject(reply);

    /**
     * All done.
     */
    pthread_mutex_unlock(&_g_lock);
    return 0;

}


/**
 * Change the access time of a file.
 */
static int
fs_utimens(const char *path, const struct timespec tv[2])
{
    int inode;
    redisReply *reply = NULL;

    pthread_mutex_lock(&_g_lock);

    if (_g_debug)
        fprintf(stderr, "fs_utimens(%s);\n", path);

    /**
     * If read-only mode is set this must fail.
     */
    if (_g_read_only)
    {
        pthread_mutex_unlock(&_g_lock);
        return -EPERM;
    }

    redis_alive();

    /**
     *
     * 1/2 Find the inode for this entry.
     *
     */
    inode = find_inode(path);
    if (inode == -1)
    {
        pthread_mutex_unlock(&_g_lock);
        return -ENOENT;
    }

    /**
     * [2/2] Change the time
     */
    reply =
        redisCommand(_g_redis,
                     "MSET %s:INODE:%d:ATIME %d %s:INODE:%d:MTIME %d",
                     _g_prefix, inode, tv[0].tv_sec, _g_prefix, inode,
                     tv[1].tv_sec);
    freeReplyObject(reply);

    /**
     * All done.
     */
    pthread_mutex_unlock(&_g_lock);
    return 0;

}


/**
 * Access-test a file.
 *
 * NOP
 */
static int
fs_access(const char *path, int mode)
{
    int inode;
    redisReply *reply = NULL;

    if (_g_debug)
        fprintf(stderr, "fs_access(%s);\n", path);

    /**
     * If we're running with --fast just return, and don't
     * update the atime.
     */
    if (_g_fast)
        return 0;


    pthread_mutex_lock(&_g_lock);

  /**
   * Update the access time of a file.
   */
    inode = find_inode(path);
    if (inode == -1)
    {
        pthread_mutex_unlock(&_g_lock);
        return 0;
    }

    reply = redisCommand(_g_redis, "SET %s:INODE:%d:ATIME %d",
                         _g_prefix, inode, time(NULL));
    freeReplyObject(reply);


    pthread_mutex_unlock(&_g_lock);

    return 0;
}


/**
 * Remove a directory entry.
 */
static int
fs_unlink(const char *path)
{
    int inode;
    redisReply *reply = NULL;
    char *parent = NULL;
    int parent_inode = 0;

    pthread_mutex_lock(&_g_lock);

    if (_g_debug)
        fprintf(stderr, "fs_unlink(%s);\n", path);

    /**
     * If read-only mode is set this must fail.
     */
    if (_g_read_only)
    {
        pthread_mutex_unlock(&_g_lock);
        return -EPERM;
    }

    redis_alive();

    /**
     * To remove the entry we need to :
     *
     * [1/4] Find the inode for this entry.
     *
     */
    inode = find_inode(path);
    if (inode == -1)
    {
        pthread_mutex_unlock(&_g_lock);
        return -ENOENT;
    }

    /**
     * [2/4] Remove from the parent set.
     */
    parent = get_parent(path);
    parent_inode = find_inode(parent);

    reply =
        redisCommand(_g_redis, "SREM %s:DIRENT:%d %d", _g_prefix,
                     parent_inode, inode);
    freeReplyObject(reply);
    free(parent);

    /**
     * [4/4] Remove all meta-data.
     */
    remove_inode(inode);

    pthread_mutex_unlock(&_g_lock);
    return 0;
}


/**
 * Rename a directory entry.
 */
int
fs_rename(const char *old, const char *path)
{
    int old_inode = -1;
    int parent_inode = 0;
    redisReply *reply = NULL;

    pthread_mutex_lock(&_g_lock);

    if (_g_debug)
        fprintf(stderr, "fs_rename(%s,%s);\n", old, path);

    /**
     * If read-only mode is set this must fail.
     */
    if (_g_read_only)
    {
        pthread_mutex_unlock(&_g_lock);
        return -EPERM;
    }

    redis_alive();

    /**
     * To rename the entry we need to :
     *
     *  1. Find the inode for the entry.
     *
     */
    old_inode = find_inode(old);
    if (old_inode == -1)
    {
        pthread_mutex_unlock(&_g_lock);
        return -ENOENT;
    }

    /**
     * Update the name of the key, which is the filename of the
     * directory entry - minus directory suffix.
     */
    char *basename = get_basename(path);
    redisReply *r = NULL;
    r = redisCommand(_g_redis, "SET %s:INODE:%d:NAME %s", _g_prefix,
                     old_inode, basename);
    freeReplyObject(r);
    free(basename);


    /**
     * Find the old parent and remove this file from the set.
     */
    char *parent = get_parent(old);
    parent_inode = find_inode(parent);
    reply =
        redisCommand(_g_redis, "SREM %s:DIRENT:%d %d", _g_prefix,
                     parent_inode, old_inode);
    freeReplyObject(reply);
    free(parent);

    /**
     * Find the new parent - and add this member to the set.
     */
    parent = get_parent(path);
    parent_inode = find_inode(parent);
    reply =
        redisCommand(_g_redis, "SADD %s:DIRENT:%d %d", _g_prefix,
                     parent_inode, old_inode);
    freeReplyObject(reply);
    free(parent);

    pthread_mutex_unlock(&_g_lock);

    return 0;
}


/**
 * Truncate an entry.
 *
 * This just needs to remove the data and reset the size to zero and the
 * MTIME to "now".
 *
 */
static int
fs_truncate(const char *path, off_t size)
{
    int inode;
    redisReply *reply = NULL;

    pthread_mutex_lock(&_g_lock);

    if (_g_debug)
        fprintf(stderr, "fs_truncate(%s);\n", path);


    /**
     * If read-only mode is set this must fail.
     */
    if (_g_read_only)
    {
        pthread_mutex_unlock(&_g_lock);
        return -EPERM;
    }

    redis_alive();

    /**
     * Ensure we're working on a file, not a directory.
     */
    if (is_directory(path))
    {
        pthread_mutex_unlock(&_g_lock);
        return -ENOENT;
    }

    /**
     * To truncate the entry we need to :
     *
     * 1/3 Find the inode for this entry.
     *
     */
    inode = find_inode(path);
    if (inode == -1)
    {
        pthread_mutex_unlock(&_g_lock);
        return -ENOENT;
    }

    /**
     * [2/3] Remove the data associated with the file.
     */
    reply = redisCommand(_g_redis, "DEL %s:INODE:%d:DATA", _g_prefix, inode);
    freeReplyObject(reply);

    /**
     * [3/3] Reset the size & mtime.
     */
    reply =
        redisCommand(_g_redis, "MSET %s:INODE:%d:SIZE 0 %s:INODE:%d:MTIME %d",
                     _g_prefix, inode, _g_prefix, inode, time(NULL));
    freeReplyObject(reply);

    pthread_mutex_unlock(&_g_lock);
    return 0;
}


/**
 * Write our current process ID to a file.
 */
long
writePID(const char *filename)
{
    char buf[20];
    int fd;
    long pid;

    if ((fd = open(filename, O_CREAT | O_TRUNC | O_WRONLY, 0644)) == -1)
        return -1;

    pid = getpid();
    snprintf(buf, sizeof(buf), "%ld", (long)pid);
    if (write(fd, buf, strlen(buf)) != strlen(buf))
    {
        close(fd);
        return -1;
    }

    return pid;
}


/**
 * Show minimal usage information.
 */
int
usage(int argc, char *argv[])
{
    printf("%s - version %s - Filesystem based upon FUSE\n", argv[0],
           VERSION);
    printf("\nOptions:\n\n");
    printf("\t--debug      - Launch with debugging information.\n");
    printf("\t--help       - Show this minimal help information.\n");
    printf("\t--host       - The hostname of the redis server [localhost]\n");
    printf
        ("\t--mount      - The directory to mount our filesystem under [/mnt/redis].\n");
    printf("\t--port       - The port of the redis server [6389].\n");
    printf("\t--prefix     - A string prepended to any Redis key names.\n");
    printf("\t--read-only  - Mount the filesystem read-only.\n");
    printf("\n");

    return 1;
}


static struct fuse_operations redisfs_operations = {
    .chmod = fs_chmod,
    .chown = fs_chown,
    .create = fs_create,
    .getattr = fs_getattr,
    .mkdir = fs_mkdir,
    .read = fs_read,
    .readdir = fs_readdir,
    .readlink = fs_readlink,
    .rename = fs_rename,
    .rmdir = fs_rmdir,
    .symlink = fs_symlink,
    .truncate = fs_truncate,
    .unlink = fs_unlink,
    .utimens = fs_utimens,
    .write = fs_write,


    /*
     *  FAKE: Only update access-time.
     */
    .access = fs_access,
    .open = fs_open,


    /*
     * Mutex setup/cleanup.
     */
    .init = fs_init,
    .destroy = fs_destroy,
};




/**
 *  Entry point to our code.
 *
 *  We support minimal command line parsing, and mostly just perform tests
 * and checks before launching ourself under fuse control.
 *
 */
int
main(int argc, char *argv[])
{
    int c;
    struct stat statbuf;

    /**
     * Args. passed to FUSE's init.
     */
    char *args[] = {
        "fuse-redisfs", _g_mount,
        "-o", "allow_other",
        "-o", "nonempty",
        "-f",
        "-o", "debug",
        NULL
    };


    /**
     * Here we're pointing to only the first few of the options
     * passed to FUSE - ie. we don't count "-o" or "debug".
     *
     * The --debug parameter causes us to increment this counter
     * by two, such that "-o debug" are included and passed to FUSE.
     */
    int args_c = 7;

    /**
     * Parse any command line arguments we might have.
     */
    while (1)
    {
        static struct option long_options[] = {
            {"debug", no_argument, 0, 'd'},
            {"fast", no_argument, 0, 'f'},
            {"help", no_argument, 0, 'h'},
            {"host", required_argument, 0, 's'},
            {"mount", required_argument, 0, 'm'},
            {"port", required_argument, 0, 'P'},
            {"prefix", required_argument, 0, 'p'},
            {"read-only", no_argument, 0, 'r'},
            {"version", no_argument, 0, 'v'},
            {0, 0, 0, 0}
        };
        int option_index = 0;

        c = getopt_long(argc, argv, "s:P:m:p:drhvf", long_options,
                        &option_index);

        /*
         * Detect the end of the options.
         */
        if (c == -1)
            break;

        switch (c)
        {
        case 'v':
            fprintf(stderr,
                    "redisfs - version %s - <http://www.steve.org.uk/Software/redisfs>\n",
                    VERSION);
            exit(0);
        case 'p':
            snprintf(_g_prefix, sizeof(_g_prefix) - 1, "%s", optarg);
            break;
        case 'P':
            _g_redis_port = atoi(optarg);
            break;
        case 'f':
            _g_fast = 1;
            break;
        case 'r':
            _g_read_only = 1;
            break;
        case 's':
            snprintf(_g_redis_host, sizeof(_g_redis_host) - 1, "%s", optarg);
            break;
        case 'm':
            snprintf(_g_mount, sizeof(_g_mount) - 1, "%s", optarg);
            break;
        case 'd':
          /**
           * Now we're passing more args to fuse's init.
           */
            args_c = 9;
            _g_debug += 1;
            break;
        case 'h':
            return (usage(argc, argv));
            break;
        default:
            abort();
        }
    }

    /**
     * Complain if we're not launched as root.
     */
    if (getuid() != 0)
    {
        fprintf(stderr, "You must start this program as root.\n");
        return -1;
    }

  /**
   * Complain if our mount-point isn't a directory.
   */
    if ((stat(_g_mount, &statbuf) != 0) ||
        ((statbuf.st_mode & S_IFMT) != S_IFDIR))
    {
        fprintf(stderr, "%s doesn't exist or isn't a directory!\n", _g_mount);
        return -1;
    }


    /**
     * Write out our pid.
     */
    if (!writePID("/var/run/redisfs.pid"))
    {
        fprintf(stderr, "Writing PID file failed\n");
        return -1;
    }

    /**
     * Show our options.
     */
    printf("Connecting to redis-server %s:%d and mounting at %s.\n",
           _g_redis_host, _g_redis_port, _g_mount);
    printf("The prefix for all key-names is '%s'\n", _g_prefix);

    /**
     * If we're read-only say so.
     */
    if (_g_read_only)
        printf("Filesystem is read-only.\n");


    /**
     * Launch fuse.
     */
    return (fuse_main(args_c, args, &redisfs_operations, NULL));
}
