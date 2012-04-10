/* pathutil.h -- Simple utilities for operating on file paths.
 *
 *
 * Copyright (c) 2010-2011, Steve Kemp <steve@steve.org.uk>
 * All rights reserved.
 *
 * http://steve.org.uk/
 *
 */


#ifndef _PATH_UTIL
#define _PATH_UTIL 1


/**
 * Find the parent of a directory entry.
 *
 *  e.g.  get_parent( "/etc/passwd" ) => "/etc"
 */
char *get_parent(const char *path);

/**
 * Find the basename of a particular entry.
 *
 *  e.g.  get_basename( "/etc/passwd" ) => "passwd"
 */
char *get_basename(const char *path);


#endif /* _PATH_UTIL */
