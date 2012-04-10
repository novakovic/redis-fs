/* redisfs-snapshot.c -- Utility to create a filesystem snapshot
 *
 *
 * Copyright (c) 2011, Steve Kemp <steve@steve.org.uk>
 * All rights reserved.
 *
 * http://steve.org.uk/
 *
 */



/**
 *  Our filesystem is written using FUSE and is pretty basic.
 *
 *  All files, directories, and meta-data are stored in a series of
 * keys with a prefix.
 *
 *  To create a new snapshot we merely clone each key & value which
 * contains our prefix.
 *
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <getopt.h>
#include <stdarg.h>


#include "hiredis.h"



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
 * The prefixes we copy from & to.
 */
char _g_old_prefix[20] = { "skx" };
char _g_new_prefix[20] = { "snapshot" };



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
 * Clone all keys with the given prefix.
 *
 * For each key we find we need to generate teh new name, and copy the
 * contents.
 *
 */
void
clone_keys(char *prefix, char *new_prefix)
{
    redisReply *reply = NULL;
    int additional;


    /**
     * We need to make sure that we have free space to store
     * the key names.
     */
    additional = strlen(new_prefix) - strlen(prefix) + 1;


    /**
     * Ensure we're alive.
     */
    redis_alive();


    /**
     * Wildcard search on the common-prefix.
     */
    reply = redisCommand(_g_redis, "KEYS %s*", prefix);

    if ((reply != NULL) && (reply->type == REDIS_REPLY_ARRAY))
    {
        int i;

        if (_g_debug)
            fprintf(stderr, "Found %d keys\n", (int)reply->elements);

        for (i = 0; i < reply->elements; i++)
        {
            redisReply *r = NULL;

          /**
           * The name of the current key.
           */
            char *old_key = reply->element[i]->str;

            if (_g_debug)
                fprintf(stderr, "Found key: %s\n", old_key);

          /**
           * The name of the new key.
           */
            char *new_key = malloc(reply->element[i]->len + additional);
            sprintf(new_key, "%s%s", new_prefix, old_key + strlen(prefix));

            if (_g_debug)
                fprintf(stderr, "\tcopying to: %s\n", new_key);


            /**
             * Get the type.
             */
            r = redisCommand(_g_redis, "TYPE %s", old_key);
            if (r != NULL)
            {
                if (_g_debug)
                    fprintf(stderr, "\tkey has type '%s'\n", r->str);

                /**
                 * If type is a string ..
                 */
                if (strcmp(r->str, "string") == 0)
                {
                    redisReply *cur = NULL;

                    /**
                     * get the value of the key.
                     */
                    cur = redisCommand(_g_redis, "GET %s", old_key);

                    if ((cur != NULL) && (cur->type == REDIS_REPLY_STRING))
                    {
                      /**
                       * Set the value in the copied key.
                       */
                        redisReply *cp = NULL;

                        cp = redisCommand(_g_redis, "SET %s %b", new_key,
                                          cur->str, cur->len);
                        freeReplyObject(cp);
                    }
                    freeReplyObject(cur);
                }
                else if (strcmp(r->str, "set") == 0)
                {
                  /**
                   * The type is a set - we need to find and clone
                   * each of the members.
                   *
                   * Thankfully the members of a set will be things
                   * like "1", "2", "3" - we don't need to transform
                   * those member names.
                   *
                   * Sometimes I'm lucky by accident, and sometimes by
                   * design.  I will not comment either way ;)
                   *
                   */
                    redisReply *cur = NULL;
                    cur = redisCommand(_g_redis, "SMEMBERS %s", old_key);
                    if (cur != NULL)
                    {
                        int i;

                        if (_g_debug)
                            fprintf(stderr, "\tcloning %d set members\n",
                                    (int)cur->elements);

                        for (i = 0; i < cur->elements; i++)
                        {
                            redisReply *r = NULL;
                            r = redisCommand(_g_redis, "SADD %s %b",
                                             new_key,
                                             cur->element[i]->str,
                                             cur->element[i]->len);
                            freeReplyObject(r);
                        }
                    }
                    freeReplyObject(cur);
                }
                else
                {
                    fprintf(stderr,
                            "The key type '%s' is not one we expect to find.\nAborting\n",
                            r->str);
                }

            }
            freeReplyObject(r);

            /**
             * Free the keyname.
             */
            free(new_key);

        }
    }

    freeReplyObject(reply);

}



/**
 * Show minimal usage information.
 */
int
usage(int argc, char *argv[])
{
    printf("%s - Filesystem based upon FUSE\n", argv[0]);
    printf("\nOptions:\n\n");
    printf("\t--debug      - Launch with debugging information.\n");
    printf("\t--help       - Show this minimal help information.\n");
    printf("\t--host       - The hostname of the redis server [localhost]\n");
    printf("\t--port       - The port of the redis server [6389].\n");
    printf("\t--from       - The prefix we're copying from.\n");
    printf("\t--to         - The prefix we're copying to.\n");
    printf("\n");

    return 1;
}

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

    /**
     * Parse any command line arguments we might have.
     */
    while (1)
    {
        static struct option long_options[] = {
            {"debug", no_argument, 0, 'd'},
            {"help", no_argument, 0, 'h'},
            {"host", required_argument, 0, 's'},
            {"port", required_argument, 0, 'P'},
            {"from", required_argument, 0, 'f'},
            {"to", required_argument, 0, 't'},
            {"version", no_argument, 0, 'v'},
            {0, 0, 0, 0}
        };
        int option_index = 0;

        c = getopt_long(argc, argv, "s:P:f:t:hdv", long_options,
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
                    "redisfs-snapshot - version %s - <http://www.steve.org.uk/Software/redisfs>\n",
                    VERSION);
            exit(0);

        case 'P':
            _g_redis_port = atoi(optarg);
            break;
        case 's':
            snprintf(_g_redis_host, sizeof(_g_redis_host) - 1, "%s", optarg);
            break;
        case 'd':
            _g_debug += 1;
            break;
        case 'f':
            snprintf(_g_old_prefix, sizeof(_g_old_prefix) - 1, "%s", optarg);
            break;
        case 't':
            snprintf(_g_new_prefix, sizeof(_g_new_prefix) - 1, "%s", optarg);
            break;
        case 'h':
            return (usage(argc, argv));
            break;
        default:
            abort();
        }
    }

    /**
     * Show our options.
     */
    printf("Connecting to redis server %s:%d.\n",
           _g_redis_host, _g_redis_port);

    printf("Cloning all keys with prefix '%s' -> '%s'\n",
           _g_old_prefix, _g_new_prefix);

    /**
     * Clone our keys
     */
    clone_keys(_g_old_prefix, _g_new_prefix);

    return 0;
}
