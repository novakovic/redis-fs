/* pathutil.c -- Simple utilities for operating on file paths.
 *
 *
 * Copyright (c) 2010-2011, Steve Kemp <steve@steve.org.uk>
 * All rights reserved.
 *
 * http://steve.org.uk/
 *
 */

#include <stdio.h>
#include <string.h>
#include <malloc.h>

#include "pathutil.h"



/**
 * Find the parent of a directory entry.
 *
 *  e.g.  get_parent( "/etc/passwd" ) => "/etc"
 */
char *
get_parent(const char *path)
{
    char *p = NULL;
    char *parent = NULL;
    int len = 0;

    /**
     * Ensure input is sane.
     */
    if (path == NULL)
        return NULL;

    /**
     * Copy the input.
     */
    len = strlen(path) + 2;
    parent = (char *)malloc(len);
    if (parent == NULL)
        return NULL;

    /**
     * Make sure we're sane.
     */
    memset(parent, '\0', len);
    strcpy(parent, path);

    /**
     * Look for the trailing "/".
     */
    if ((p = strrchr(parent, '/')) != NULL)
        *p = '\0';
    else
    {
        free(parent);
        return NULL;
    }

    /**
     * If we're empty then we can just add "/".
     */
    if (parent[0] == '\0')
        strcpy(parent, "/");

    /**
     * All done.
     */
    return (parent);
}


/**
 * Find the basename of a particular entry.
 *
 *  e.g.  get_basename( "/etc/passwd" ) => "passwd"
 */
char *
get_basename(const char *path)
{
    char *basename = NULL;
    char *p = NULL;
    int len = 0;

    /**
     * Test input is sane.
     */
    if (path == NULL)
        return NULL;

    /**
     * Allocate memory for a copy.
     */
    len = strlen(path) + 2;
    basename = (char *)malloc(len);
    if (basename == NULL)
        return NULL;

    /**
     * Look for right-most "/"
     */
    p = strrchr(path, '/');

    if (p == NULL)
        p = (char *)path;
    else
        p += 1;

    /**
     * Copy from after the char to the start.
     */
    strcpy(basename, p);

    /**
     * Return.
     */
    return (basename);
}
