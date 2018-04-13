/*
 * Copyright (c) 2013-2015, Lawrence Livermore National Security, LLC.
 *   Produced at the Lawrence Livermore National Laboratory
 *   CODE-673838
 *
 * Copyright (c) 2006-2007,2011-2015, Los Alamos National Security, LLC.
 *   (LA-CC-06-077, LA-CC-10-066, LA-CC-14-046)
 *
 * Copyright (2013-2015) UT-Battelle, LLC under Contract No.
 *   DE-AC05-00OR22725 with the Department of Energy.
 *
 * Copyright (c) 2015, DataDirect Networks, Inc.
 * 
 * All rights reserved.
 *
 * This file is part of mpiFileUtils.
 * For details, see https://github.com/hpc/fileutils.
 * Please also read the LICENSE file.
*/

#include <dirent.h>
#include <limits.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h> /* asctime / localtime */

#include <pwd.h> /* for getpwent */
#include <grp.h> /* for getgrent */
#include <errno.h>
#include <string.h>

#include <libgen.h> /* dirname */

#include "libcircle.h"
#include "dtcmp.h"
#include "mfu.h"

/*****************************
 * Global functions used by remove routines
 ****************************/

/* removes name by calling rmdir, unlink, or remove depending
 * on item type */
static void remove_type(char type, const char* name, int traceless)
{

    char *pdir;
    struct timespec times[2];
    int status = -1;

    if (traceless) {
        struct stat st;

        pdir = MFU_STRDUP(name);
        dirname(pdir);
        status = mfu_lstat(pdir, &st);
        if (!status) {
            times[0].tv_sec  = st.st_atime;
            times[1].tv_sec  = st.st_mtime;

#if HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC
            times[0].tv_nsec = st.st_atimespec.tv_nsec;
            times[1].tv_nsec = st.st_mtimespec.tv_nsec;
#elif HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC
            times[0].tv_nsec = st.st_atim.tv_nsec;
            times[1].tv_nsec = st.st_mtim.tv_nsec;
#elif HAVE_STRUCT_STAT_ST_MTIME_N
            times[0].tv_nsec = st.st_atime_n;
            times[1].tv_nsec = st.st_mtime_n;
#elif HAVE_STRUCT_STAT_ST_UMTIME
            times[0].tv_nsec = st.st_uatime * 1000;
            times[1].tv_nsec = st.st_umtime * 1000;
#elif HAVE_STRUCT_STAT_ST_MTIME_USEC
            times[0].tv_nsec = st.st_atime_usec * 1000;
            times[1].tv_nsec = st.st_mtime_usec * 1000;
#else
            times[0].tv_nsec = 0;
            times[1].tv_nsec = 0;
#endif
        }
    }
    /* TODO: don't print message if errno == ENOENT (file already gone) */
    if (type == 'd') {
        int rc = mfu_rmdir(name);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to rmdir `%s' (errno=%d %s)",
                      name, errno, strerror(errno)
                     );
        }
    }
    else if (type == 'f') {
        int rc = mfu_unlink(name);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to unlink `%s' (errno=%d %s)",
                      name, errno, strerror(errno)
                     );
        }
    }
    else if (type == 'u') {
        int rc = remove(name);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to remove `%s' (errno=%d %s)",
                      name, errno, strerror(errno)
                     );
        }
    }
    else {
        /* print error */
        MFU_LOG(MFU_LOG_ERR, "Unknown type=%c name=%s",
                  type, name
                 );
    }

    if (traceless && !status) {
        if(utimensat(AT_FDCWD, pdir, times, AT_SYMLINK_NOFOLLOW) != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to changeback timestamps on %s utime() errno=%d %s",
                pdir, errno, strerror(errno));
        }

        mfu_free(&pdir);
    }
    return;
}

/*****************************
 * Directly remove items in local portion of distributed list
 ****************************/

/* for given depth, just remove the files we know about */
static void remove_direct(mfu_flist list, uint64_t* rmcount)
{
    /* each process directly removes its elements */
    uint64_t idx;
    uint64_t size = mfu_flist_size(list);
    for (idx = 0; idx < size; idx++) {
        /* get name and type of item */
        const char* name = mfu_flist_file_get_name(list, idx);
        mfu_filetype type = mfu_flist_file_get_type(list, idx);

        /* delete item */
        if (type == MFU_TYPE_DIR) {
            remove_type('d', name, 0);
        }
        else if (type == MFU_TYPE_FILE || type == MFU_TYPE_LINK) {
            remove_type('f', name, 0);
        }
        else {
            remove_type('u', name, 0);
        }
    }

    /* report the number of items we deleted */
    *rmcount = size;

    return;
}

/*****************************
 * Distribute items evenly across processes, then remove
 ****************************/

/* given an array of integers and its size, find and return
 * index of first non-zero element, returns -1 if not found */
static int get_first_nonzero(const int* buf, int size)
{
    int i;
    for (i = 0; i < size; i++) {
        if (buf[i] != 0) {
            return i;
        }
    }
    return -1;
}

/* for given depth, evenly spread the files among processes for
 * improved load balancing */
static void remove_spread(mfu_flist flist, uint64_t* rmcount, int traceless)
{
    uint64_t idx;

    /* initialize our remove count */
    *rmcount = 0;

    /* get our rank and number of ranks in job */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* allocate memory for alltoall exchanges */
    size_t bufsize = (size_t)ranks * sizeof(int);
    int* sendcounts = (int*) MFU_MALLOC(bufsize);
    int* sendsizes  = (int*) MFU_MALLOC(bufsize);
    int* senddisps  = (int*) MFU_MALLOC(bufsize);
    int* recvsizes  = (int*) MFU_MALLOC(bufsize);
    int* recvdisps  = (int*) MFU_MALLOC(bufsize);

    /* get number of items */
    uint64_t my_count  = mfu_flist_size(flist);
    uint64_t all_count = mfu_flist_global_size(flist);
    uint64_t offset    = mfu_flist_global_offset(flist);

    /* compute number of bytes we'll send */
    size_t sendbytes = 0;
    for (idx = 0; idx < my_count; idx++) {
        const char* name = mfu_flist_file_get_name(flist, idx);
        size_t len = strlen(name) + 2;
        sendbytes += len;
    }

    /* compute the number of items that each rank should have */
    uint64_t low = all_count / (uint64_t)ranks;
    uint64_t extra = all_count - low * (uint64_t)ranks;

    /* compute number that we'll send to each rank and initialize sendsizes and offsets */
    uint64_t i;
    for (i = 0; i < (uint64_t)ranks; i++) {
        /* compute starting element id and count for given rank */
        uint64_t start, num;
        if (i < extra) {
            num = low + 1;
            start = i * num;
        }
        else {
            num = low;
            start = (i - extra) * num + extra * (low + 1);
        }

        /* compute the number of items we'll send to this task */
        uint64_t sendcnt = 0;
        if (my_count > 0) {
            if (start <= offset && offset < start + num) {
                /* this rank overlaps our range,
                 * and its first element comes at or before our first element */
                sendcnt = num - (offset - start);
                if (my_count < sendcnt) {
                    /* the number the rank could receive from us
                     * is more than we have left */
                    sendcnt = my_count;
                }
            }
            else if (offset < start && start < offset + my_count) {
                /* this rank overlaps our range,
                 * and our first element comes strictly before its first element */
                sendcnt = my_count - (start - offset);
                if (num < sendcnt) {
                    /* the number the rank can receive from us
                     * is less than we have left */
                    sendcnt = num;
                }
            }
        }

        /* record the number of items we'll send to this task */
        sendcounts[i]  = (int) sendcnt;

        /* set sizes and displacements to 0, we'll fix this later */
        sendsizes[i] = 0;
        senddisps[i] = 0;
    }

    /* allocate space */
    char* sendbuf = (char*) MFU_MALLOC(sendbytes);

    /* copy data into buffer */
    int dest = -1;
    int disp = 0;
    for (idx = 0; idx < my_count; idx++) {
        /* get name and type of item */
        const char* name = mfu_flist_file_get_name(flist, idx);
        mfu_filetype type = mfu_flist_file_get_type(flist, idx);

        /* get rank that we're packing data for */
        if (dest == -1) {
            dest = get_first_nonzero(sendcounts, ranks);
            if (dest == -1) {
                /* error */
            }
            /* about to copy first item for this rank,
             * record its displacement */
            senddisps[dest] = disp;
        }

        /* identify region to be sent to rank */
        char* path = sendbuf + disp;

        /* first character encodes item type */
        if (type == MFU_TYPE_DIR) {
            path[0] = 'd';
        }
        else if (type == MFU_TYPE_FILE || type == MFU_TYPE_LINK) {
            path[0] = 'f';
        }
        else {
            path[0] = 'u';
        }

        /* now copy in the path */
        strcpy(&path[1], name);

        /* TODO: check that we don't overflow the int */
        /* add bytes to sendsizes and increase displacement */
        size_t count = strlen(name) + 2;
        sendsizes[dest] += (int) count;
        disp += (int) count;

        /* decrement the count for this rank */
        sendcounts[dest]--;
        if (sendcounts[dest] == 0) {
            dest = -1;
        }
    }

    /* compute displacements */
    senddisps[0] = 0;
    for (i = 1; i < (uint64_t)ranks; i++) {
        senddisps[i] = senddisps[i - 1] + sendsizes[i - 1];
    }

    /* alltoall to specify incoming counts */
    MPI_Alltoall(sendsizes, 1, MPI_INT, recvsizes, 1, MPI_INT, MPI_COMM_WORLD);

    /* compute size of recvbuf and displacements */
    size_t recvbytes = 0;
    recvdisps[0] = 0;
    for (i = 0; i < (uint64_t)ranks; i++) {
        recvbytes += (size_t) recvsizes[i];
        if (i > 0) {
            recvdisps[i] = recvdisps[i - 1] + recvsizes[i - 1];
        }
    }

    /* allocate recvbuf */
    char* recvbuf = (char*) MFU_MALLOC(recvbytes);

    /* alltoallv to send data */
    MPI_Alltoallv(
        sendbuf, sendsizes, senddisps, MPI_CHAR,
        recvbuf, recvsizes, recvdisps, MPI_CHAR, MPI_COMM_WORLD
    );

    /* delete data */
    char* item = recvbuf;
    while (item < recvbuf + recvbytes) {
        /* get item name and type */
        char type = item[0];
        char* name = &item[1];

        /* delete item */
        remove_type(type, name, traceless);

        /* keep tally of number of items we deleted */
        *rmcount++;

        /* go to next item */
        size_t item_size = strlen(item) + 1;
        item += item_size;
    }

    /* free memory */
    mfu_free(&recvbuf);
    mfu_free(&recvdisps);
    mfu_free(&recvsizes);
    mfu_free(&sendbuf);
    mfu_free(&senddisps);
    mfu_free(&sendsizes);
    mfu_free(&sendcounts);

    return;
}

/* we hash file names based on its parent directory to map all
 * files in the same directory to the same process */
static int map_name(mfu_flist flist, uint64_t idx, int ranks, void* args)
{
    /* get name of item */
    const char* name = mfu_flist_file_get_name(flist, idx);

    /* identify rank to send this file to */
    char* dir = MFU_STRDUP(name);
    dirname(dir);
    size_t dir_len = strlen(dir);
    uint32_t hash = mfu_hash_jenkins(dir, dir_len);
    int rank = (int)(hash % (uint32_t)ranks);
    mfu_free(&dir);
    return rank;
}

static void remove_map(mfu_flist list, uint64_t* rmcount)
{
    /* remap files based on parent directory */
    mfu_flist newlist = mfu_flist_remap(list, map_name, NULL);

    /* at this point, we can directly remove files in our list */
    remove_direct(newlist, rmcount);

    /* free list of remapped files */
    mfu_flist_free(&newlist);

    return;
}

/*****************************
 * Globally sort items by filename, then remove,
 * may reduce locking if need to lock by directories
 ****************************/

/* for each depth, sort files by filename and then remove, to test
 * whether it matters to limit the number of directories each process
 * has to reference (e.g., locking) */
static void remove_sort(mfu_flist list, uint64_t* rmcount)
{
    /* bail out if total count is 0 */
    uint64_t all_count = mfu_flist_global_size(list);
    if (all_count == 0) {
        return;
    }

    /* get maximum file name and number of items */
    int chars = (int) mfu_flist_file_max_name(list);
    uint64_t my_count = mfu_flist_size(list);

    /* create key datatype (filename) and comparison op */
    MPI_Datatype dt_key;
    DTCMP_Op op_str;
    DTCMP_Str_create_ascend(chars, &dt_key, &op_str);

    /* create keysat datatype (filename + type) */
    MPI_Datatype types[2], dt_keysat;
    types[0] = dt_key;
    types[1] = MPI_CHAR;
    DTCMP_Type_create_series(2, types, &dt_keysat);

    /* allocate send buffer */
    int sendcount = (int) my_count;
    size_t sendbufsize = (size_t)(sendcount * (chars + 1));
    char* sendbuf = (char*) MFU_MALLOC(sendbufsize);

    /* copy data into buffer */
    char* ptr = sendbuf;
    uint64_t idx;
    for (idx = 0; idx < my_count; idx++) {
        /* encode the filename first */
        const char* name = mfu_flist_file_get_name(list, idx);
        strcpy(ptr, name);
        ptr += chars;

        /* last character encodes item type */
        mfu_filetype type = mfu_flist_file_get_type(list, idx);
        if (type == MFU_TYPE_DIR) {
            ptr[0] = 'd';
        }
        else if (type == MFU_TYPE_FILE || type == MFU_TYPE_LINK) {
            ptr[0] = 'f';
        }
        else {
            ptr[0] = 'u';
        }
        ptr++;
    }

    /* sort items */
    void* recvbuf;
    int recvcount;
    DTCMP_Handle handle;
    DTCMP_Sortz(
        sendbuf, sendcount, &recvbuf, &recvcount,
        dt_key, dt_keysat, op_str, DTCMP_FLAG_NONE, MPI_COMM_WORLD, &handle
    );

    /* delete data */
    int delcount = 0;
    ptr = (char*)recvbuf;
    while (delcount < recvcount) {
        /* get item name */
        char* name = ptr;
        ptr += chars;

        /* get item type */
        char type = ptr[0];
        ptr++;

        /* delete item */
        remove_type(type, name, 0);
        delcount++;
    }

    /* record number of items we deleted */
    *rmcount = (uint64_t) delcount;

    /* free output data */
    DTCMP_Free(&handle);

    /* free our send buffer */
    mfu_free(&sendbuf);

    /* free key comparison operation */
    DTCMP_Op_free(&op_str);

    /* free datatypes */
    MPI_Type_free(&dt_keysat);
    MPI_Type_free(&dt_key);

    return;
}

#if 0
/*****************************
 * Remove items using libcircle for dynamic load balancing
 ****************************/

/* globals needed for libcircle callback routines */
static mfu_flist circle_list; /* list of items we're deleting */
static uint64_t circle_count;   /* number of items local process has removed */

static void remove_create(CIRCLE_handle* handle)
{
    char path[CIRCLE_MAX_STRING_LEN];

    /* enqueues all items at rm_depth to be deleted */
    uint64_t idx;
    uint64_t size = mfu_flist_size(circle_list);
    for (idx = 0; idx < size; idx++) {
        /* get name and type of item */
        const char* name = mfu_flist_file_get_name(circle_list, idx);
        mfu_filetype type = mfu_flist_file_get_type(circle_list, idx);

        /* encode type */
        if (type == MFU_TYPE_DIR) {
            path[0] = 'd';
        }
        else if (type == MFU_TYPE_FILE || type == MFU_TYPE_LINK) {
            path[0] = 'f';
        }
        else {
            path[0] = 'u';
        }

        /* encode name */
        size_t len = strlen(name) + 2;
        if (len <= CIRCLE_MAX_STRING_LEN) {
            strcpy(&path[1], name);
            handle->enqueue(path);
        }
        else {
            MFU_LOG(MFU_LOG_ERR, "Filename longer than %lu",
                      (unsigned long)CIRCLE_MAX_STRING_LEN
                     );
        }
    }

    return;
}

static void remove_process(CIRCLE_handle* handle)
{
    char path[CIRCLE_MAX_STRING_LEN];
    handle->dequeue(path);

    char item = path[0];
    char* name = &path[1];
    remove_type(item, name);
    circle_count++;

    return;
}

/* insert all items to be removed into libcircle for
 * dynamic load balancing */
static void remove_libcircle(mfu_flist list, uint64_t* rmcount)
{
    /* set globals for libcircle callbacks */
    circle_list  = list;
    circle_count = 0;

    /* initialize libcircle */
    CIRCLE_init(0, NULL, CIRCLE_SPLIT_EQUAL | CIRCLE_CREATE_GLOBAL);

    /* set libcircle verbosity level */
    enum CIRCLE_loglevel loglevel = CIRCLE_LOG_WARN;
    if (mfu_debug_level >= MFU_LOG_VERBOSE) {
        //        loglevel = CIRCLE_LOG_INFO;
    }
    CIRCLE_enable_logging(loglevel);

    /* register callbacks */
    CIRCLE_cb_create(&remove_create);
    CIRCLE_cb_process(&remove_process);

    /* run the libcircle job */
    CIRCLE_begin();
    CIRCLE_finalize();

    /* record number of items we deleted */
    *rmcount = circle_count;

    return;
}
#endif

/* TODO: sort w/ spread and synchronization */
/* allreduce to get total count of items */
/* sort by name */
/* alltoall to determine which processes to send / recv from */
/* alltoallv to exchange data */
/* pt2pt with left and right neighbors to determine if they have the same dirname */
/* delete what we can witout waiting */
/* if my right neighbor has same dirname, send it msg when we're done */
/* if my left neighbor has same dirname, wait for msg */

/*****************************
 * Driver functions
 ****************************/

/* removes list of items, sets write bits on directories from
 * top-to-bottom, then removes items one level at a time starting
 * from the deepest */
void mfu_flist_unlink(mfu_flist flist, int traceless)
{
    int level;

    /* wait for all tasks and start timer */
    MPI_Barrier(MPI_COMM_WORLD);
    double start_remove = MPI_Wtime();

    /* split files into separate lists by directory depth */
    int levels, minlevel;
    mfu_flist* lists;
    mfu_flist_array_by_depth(flist, &levels, &minlevel, &lists);

#if 0
    /* dive from shallow to deep, ensure all directories have write bit set */
    for (level = 0; level < levels; level++) {
        /* get list of items for this level */
        mfu_flist list = lists[level];

        /* determine whether we have details at this level */
        int detail = mfu_flist_have_detail(list);

        /* iterate over items and set write bit on directories if needed */
        uint64_t idx;
        uint64_t size = mfu_flist_size(list);
        for (idx = 0; idx < size; idx++) {
            /* check whether we have a directory */
            mfu_filetype type = mfu_flist_file_get_type(list, idx);
            if (type == MFU_TYPE_DIR) {
                /* assume we have to set the bit */
                int set_write_bit = 1;
                if (detail) {
                    mode_t mode = (mode_t) mfu_flist_file_get_mode(list, idx);
                    if (mode & S_IWUSR) {
                        /* we have the mode of the file, and the bit is already set */
                        set_write_bit = 0;
                    }
                }

                /* set the bit if needed */
                if (set_write_bit) {
                    const char* name = mfu_flist_file_get_name(list, idx);
                    int rc = chmod(name, S_IRWXU);
                    if (rc != 0) {
                        MFU_LOG(MFU_LOG_ERR, "Failed to chmod directory `%s' (errno=%d %s)",
                                  name, errno, strerror(errno)
                                 );
                    }
                }
            }
        }

        /* wait for all procs to finish before we start next level */
        MPI_Barrier(MPI_COMM_WORLD);
    }
#endif

    /* now remove files starting from deepest level */
    for (level = levels - 1; level >= 0; level--) {
        double start = MPI_Wtime();

        /* get list of items for this level */
        mfu_flist list = lists[level];

        uint64_t count = 0;
        //remove_direct(list, &count);
        remove_spread(list, &count, traceless);
//        remove_map(list, &count);
//        remove_sort(list, &count);
//        remove_libcircle(list, &count);
//        TODO: remove sort w/ spread

        /* wait for all procs to finish before we start
         * with files at next level */
        MPI_Barrier(MPI_COMM_WORLD);

        double end = MPI_Wtime();

        if (mfu_debug_level >= MFU_LOG_VERBOSE) {
            uint64_t min, max, sum;
            MPI_Allreduce(&count, &min, 1, MPI_UINT64_T, MPI_MIN, MPI_COMM_WORLD);
            MPI_Allreduce(&count, &max, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);
            MPI_Allreduce(&count, &sum, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
            double rate = 0.0;
            if (end - start > 0.0) {
                rate = (double)sum / (end - start);
            }
            double time_diff = end - start;
            if (mfu_rank == 0) {
                printf("level=%d min=%lu max=%lu sum=%lu rate=%f secs=%f\n",
                       (minlevel + level), (unsigned long)min, (unsigned long)max, (unsigned long)sum, rate, time_diff
                      );
                fflush(stdout);
            }
        }
    }

    mfu_flist_array_free(levels, &lists);

    /* wait for all tasks and stop timer */
    MPI_Barrier(MPI_COMM_WORLD);
    double end_remove = MPI_Wtime();

    /* report remove count, time, and rate */
    if (mfu_debug_level >= MFU_LOG_VERBOSE && mfu_rank == 0) {
        uint64_t all_count = mfu_flist_global_size(flist);
        double time_diff = end_remove - start_remove;
        double rate = 0.0;
        if (time_diff > 0.0) {
            rate = ((double)all_count) / time_diff;
        }
        printf("Removed %lu items in %f seconds (%f items/sec)\n",
               all_count, time_diff, rate
              );
    }

    return;
}
