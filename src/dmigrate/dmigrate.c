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

// mpicc -g -O0 -o restripe restripe.c -llustreapi

#include "config.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <limits.h>
#include <getopt.h>
#include <string.h>
#include <inttypes.h>
#include <regex.h>
#include "mpi.h"

#ifdef LUSTRE_SUPPORT
#include <lustre/lustreapi.h>
#endif

#include "mfu.h"

static void print_usage(void)
{
    printf("\n");
    printf("Usage: dmigrate [options] PATH...\n");
    printf("\n");
    printf("Options:\n");
    printf("  -S, --src-pool <ost-pool> - source pool that we want to migrate from\n");
    printf("  -D, --dest-pool <ost-pool> - dest pool that we want to migrate to\n");
    printf("  -v, --verbose             - verbose output\n");
    printf("  -f, --filter <field>:<operator>:<value> - filter output by field, only support size now\n");
    printf("      --filter \"size:>:1T\" \n");
    printf("  -h, --help                - print usage\n");
    printf("\n");
    fflush(stdout);
    return;
}

/* generate a random, alpha suffix */
static void generate_suffix(char *suffix, const int len)
{
    const char set[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    int numchars = len - 1;

    /* first char has to be a dot */
    suffix[0] = '.';

    /* randomly fill in our array with chars */
    for (int i = 1; i < numchars; i++) {
        int set_idx = (double)rand() / RAND_MAX * (sizeof(set) - 1);
        suffix[i] = set[set_idx];
    }

    /* need to be null terminated */
    suffix[len - 1] = '\0';
}

static void generate_pretty_size(char *out, unsigned int length, uint64_t size)
{
    double size_tmp;
    const char* size_units;
    char *unit;
    unsigned int unit_len;

    mfu_format_bytes(size, &size_tmp, &size_units);
    snprintf(out, length, "%.2f %s", size_tmp, size_units);
}

/* print to stdout the stripe size and count of each file in the mfu_flist */
static void stripe_info_report(mfu_flist list)
{
    uint64_t idx;
    uint64_t size = mfu_flist_size(list);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* print header */
    if (rank == 0) {
        printf("%10s %3.3s %8.8s %s\n", "Size", "Cnt", "Str Size", "File Path");
        printf("%10s %3.3s %8.8s %s\n", "----", "---", "--------", "---------");
        fflush(stdout);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    /* print out file info */
    for (idx = 0; idx < size; idx++) {
        mfu_filetype type = mfu_flist_file_get_type(list, idx);

        /* report striping information for regular files only */
        if (type == MFU_TYPE_FILE) {
            const char* in_path = mfu_flist_file_get_name(list, idx);
            uint64_t stripe_size = 0;
            uint64_t stripe_count = 0;
            char filesize[11];
            char stripesize[9];

            /*
             * attempt to get striping info and print it out,
             * skip the file if we can't get the striping info we seek
             */
            if (mfu_get_layout_info(in_path, &stripe_size, &stripe_count, NULL) != 0) {
                continue;
            }

            /* format it nicely */
            generate_pretty_size(filesize, sizeof(filesize), mfu_flist_file_get_size(list, idx));
            generate_pretty_size(stripesize, sizeof(stripesize), stripe_size);

            /* print the row */
            printf("%10.10s %3" PRId64 " %8.8s %s\n", filesize, stripe_count, stripesize, in_path);
            fflush(stdout);
        }
    }
}

/* write a chunk of the file */
static void write_file_chunk(mfu_file_chunk* p, const char* out_path)
{
    size_t chunk_size = 1024*1024;
    uint64_t base = (off_t)p->offset;
    uint64_t file_size = (off_t)p->file_size;
    const char *in_path = p->name;
    uint64_t stripe_size = (off_t)p->length;

    /* if the file size is 0, there's no data to restripe */
    /* if the stripe size is 0, then there's no work to be done */
    if (file_size == 0 || stripe_size == 0) {
        return;
    }

    /* allocate buffer */
    void* buf = MFU_MALLOC(chunk_size);
    if (buf == NULL) {
        printf("Failed to allocate buffer\n");
        fflush(stdout);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* open input file for reading */
    int in_fd = mfu_open(in_path, O_RDONLY);
    if (in_fd < 0) {
        printf("Failed to open input file %s (%s)\n", in_path, strerror(errno));
        fflush(stdout);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* open output file for writing */
    int out_fd = mfu_open(out_path, O_WRONLY);
    if (out_fd < 0) {
        printf("Failed to open output file %s (%s)\n", out_path, strerror(errno));
        fflush(stdout);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* write data */
    uint64_t chunk_id = 0;
    uint64_t stripe_read = 0;
    while (stripe_read < stripe_size) {
        /* determine number of bytes to read */
        /* try to read a full chunk's worth of bytes */
        size_t read_size = chunk_size;

        /* if the stripe doesn't have that much left */
        uint64_t remainder = stripe_size - stripe_read;
        if (remainder < (uint64_t) read_size) {
            read_size = (size_t) remainder;
        }

        /* get byte offset to read from */
        uint64_t offset = base + (chunk_id * chunk_size);
        if (offset < file_size) {
            /* the first byte falls within the file size,
             * now check the last byte */
            uint64_t last = offset + (uint64_t) read_size;
            if (last > file_size) {
                /* the last byte is beyond the end, set read size
                 * to the most we can read */
                read_size = (size_t) (file_size - offset);
            }
        } else {
            /* the first byte we need to read is past the end of
             * the file, so don't read anything */
            read_size = 0;
        }

        /* bail if we don't have anything to read */
        if (read_size == 0) {
            break;
        }

        /* seek to correct spot in input file */
        off_t pos = (off_t) offset;
        off_t seek_rc = mfu_lseek(in_path, in_fd, pos, SEEK_SET);
        if (seek_rc == (off_t)-1) {
            printf("Failed to seek in input file %s (%s)\n", in_path, strerror(errno));
            fflush(stdout);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        /* read chunk from input */
        ssize_t nread = mfu_read(in_path, in_fd, buf, read_size);

        /* check for errors */
        if (nread < 0) {
            printf("Failed to read data from input file %s (%s)\n", in_path, strerror(errno));
            fflush(stdout);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        /* check for short reads */
        if (nread != read_size) {
            printf("Got a short read from input file %s\n", in_path);
            fflush(stdout);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        /* seek to correct spot in output file */
        seek_rc = mfu_lseek(out_path, out_fd, pos, SEEK_SET);
        if (seek_rc == (off_t)-1) {
            printf("Failed to seek in output file %s (%s)\n", out_path, strerror(errno));
            fflush(stdout);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        /* write chunk to output */
        ssize_t nwrite = mfu_write(out_path, out_fd, buf, read_size);

        /* check for errors */
        if (nwrite < 0) {
            printf("Failed to write data to output file %s (%s)\n", out_path, strerror(errno));
            fflush(stdout);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        /* check for short reads */
        if (nwrite != read_size) {
            printf("Got a short write to output file %s\n", out_path);
            fflush(stdout);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        /* go on to the next chunk in this stripe, we assume we
         * read the whole chunk size, if we didn't it's because
         * the stripe is smaller or we're at the end of the file,
         * but in either case we're done so it doesn't hurt to
         * over estimate in this calculation */
        stripe_read += (uint64_t) chunk_size;
        chunk_id++;
    }

    /* close files */
    mfu_fsync(out_path, out_fd);
    mfu_close(out_path, out_fd);
    mfu_close(in_path, in_fd);

    /* free buffer */
    mfu_free(&buf);
}

static bool valid_filter_operator(const char *operator)
{
    int len = strlen(operator);

    if (len == 1) {
        if (!strcmp(operator, "=") ||
            !strcmp(operator, "<") ||
            !strcmp(operator, ">"))
            return true;
        else
            return false;
    } else if (len == 2) {
        if (!strcmp(operator, ">=") ||
            !strcmp(operator, "<="))
            return true;
        else
            return false;
    }

    return false;
}

static int filter_parse(struct mfu_filter *filter, const char *string)
{
    char *ptr;
    uint64_t value;
    char *str;
    int status = 0;
    regmatch_t pmatch[100];
    regex_t reg;
    char * pattern = "([A-Za-z]+):([>|<|=|>=|<=]+):([A-Za-z0-9]+)";
    int i = 0;
    char buf[100];
    int res = regcomp(&reg, pattern, REG_EXTENDED | REG_NEWLINE);

    if(res) {
        return -1;
    }

    str = MFU_STRDUP(string);

    status = regexec(&reg, str, sizeof(pmatch)/sizeof(regmatch_t), pmatch, 0);
    if (status == REG_NOMATCH) {
        status = -1;
        goto out;
    }

    strncpy(buf, str + pmatch[1].rm_so, pmatch[1].rm_eo - pmatch[1].rm_so);
    buf[pmatch[1].rm_eo - pmatch[1].rm_so] = '\0';
    strncpy(filter->field, buf, 15);

    strncpy(buf, str + pmatch[2].rm_so, pmatch[2].rm_eo - pmatch[2].rm_so);
    buf[pmatch[2].rm_eo - pmatch[2].rm_so] = '\0';

    if (!valid_filter_operator(buf)) {
        status = -1;
        goto out;
    }
    strncpy(filter->operator, buf, 7);

    strncpy(buf, str + pmatch[3].rm_so, pmatch[3].rm_eo - pmatch[3].rm_so);
    buf[pmatch[3].rm_eo - pmatch[3].rm_so] = '\0';
    if (mfu_abtoull(buf, &value) != MFU_SUCCESS) {
        status = -1;
        goto out;
    }
    filter->value = value;
out:
    regfree(&reg);
    mfu_free(&str);
    return status;
}


int main(int argc, char* argv[])
{
    MPI_Init(&argc, &argv);
    mfu_init();

    /* get our rank and number of ranks in the job */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    uint64_t idx;
    int option_index = 0;
    int usage = 0;
    int report = 0;
    int verbose = 0;
    unsigned int numpaths = 0;
    char* filter_string = NULL;
    char* src_pool = NULL;
    char* dest_pool = NULL;
    struct mfu_filter *size_filter = NULL;
    mfu_param_path* paths = NULL;

    /* default to 1MB stripe size, stripe across all OSTs, and all files are candidates */
    int stripes = -1;
    uint64_t stripe_size = 1048576;
    uint64_t min_size = 0;

    static struct option long_options[] = {
        {"src-pool", 1, 0, 'S'},
        {"dest-pool",1, 0, 'D'},
        {"filter",   1, 0, 'f'},
        {"help",     0, 0, 'h'},
        {"report",   0, 0, 'r'},
        {0, 0, 0, 0}
    };

    while (1) {
        int c = getopt_long(argc, argv, "S:D:f:rhv",
                    long_options, &option_index);

        if (c == -1) {
            break;
        }

        switch (c) {
            case 'f':
                filter_string = MFU_STRDUP(optarg);
                break;
            case 'S':
                src_pool = MFU_STRDUP(optarg);
                break;
            case 'D':
                dest_pool = MFU_STRDUP(optarg);
                break;
            case 'r':
                /* report striping info */
		report = 1;
                break;
            case 'v':
                /* verbose output */
                verbose = 1;
                break;
            case 'h':
                /* display usage */
                usage = 1;
                break;
            case '?':
                /* display usage */
                usage = 1;
                break;
            default:
                if (rank == 0) {
                    printf("?? getopt returned character code 0%o ??\n", c);
                }
        }
    }

    /* paths to walk come after the options */
    if (optind < argc) {
        /* determine number of paths specified by user */
        numpaths = argc - optind;

        /* allocate space for each path */
        paths = (mfu_param_path*) MFU_MALLOC((size_t)numpaths * sizeof(mfu_param_path));

        /* process each path */
        char** p = &argv[optind];
        mfu_param_path_set_all((uint64_t)numpaths, (const char**)p, paths);
        optind += numpaths;
    } else {
        usage = 1;
    }

    if (filter_string != NULL) {
        size_filter = (struct mfu_filter *) MFU_MALLOC(sizeof(struct mfu_filter));
        if (filter_parse(size_filter, filter_string) != 0) {
            if (rank == 0) {
                printf("Failed to parse filter string: %s\n", filter_string);
                fflush(stdout);
                usage = 1;
            }
        }
    }

    /* if we need to print usage, print it and exit */
    if (usage) {
        if (rank == 0) {
            print_usage();
        }

        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    /* nothing to do if lustre support is disabled */
#ifndef LUSTRE_SUPPORT
    if (rank == 0) {
        printf("Lustre support is disabled.\n");
        fflush(stdout);
    }

    MPI_Abort(MPI_COMM_WORLD, 1);
#endif

    /* stripe count must be -1 for all available or greater than 0 */
    if (stripes < -1) {
        if (rank == 0) {
            printf("Stripe count must be -1 for all servers, 0 for lustre file system default, or a positive value\n");
            fflush(stdout);
        }

        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* lustre requires stripe sizes to be aligned */
    if (stripe_size > 0 && stripe_size % 65536 != 0) {
        if (rank == 0) {
            printf("Stripe size must be a multiple of 65536\n");
            fflush(stdout);
        }

        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* TODO: verify the src/dest pool */

    /* walk list of input paths and stat as we walk */
    mfu_flist flist = mfu_flist_new();
    mfu_flist_walk_param_paths(numpaths, paths, 1, 0, flist);

    /* filter down our list to files which don't meet our striping requirements */
    mfu_flist filtered = mfu_flist_filter(flist, size_filter, src_pool);
    mfu_flist_free(&flist);

    MPI_Barrier(MPI_COMM_WORLD);

    /* report the file size and stripe count of all files we found */
    if (report) {
        /* report the files in our filtered list */
        stripe_info_report(filtered);

        /* free the paths and our list */
        mfu_flist_free(&filtered);
        mfu_param_path_free_all(numpaths, paths);
        mfu_free(&paths);

        /* finalize */
        mfu_finalize();
        MPI_Finalize();   return 0;
        return 0;
    }

    /* generate a global suffix for our temp files and have each node check it's list */
    char suffix[8];
    uint64_t retry;

    /* seed our random number generator */
    srand(time(NULL));

    /* keep trying to make a valid random suffix...*/
    do {
        uint64_t attempt = 0;

        /* make rank 0 responsible for generating a random suffix */
        if (rank == 0) {
            generate_suffix(suffix, sizeof(suffix));
        }

        /* broadcast the random suffix to all ranks */
        MPI_Bcast(suffix, sizeof(suffix), MPI_CHAR, 0, MPI_COMM_WORLD);

        /* check that the file doesn't already exist */
        uint64_t size = mfu_flist_size(filtered);
        for (idx = 0; idx < size; idx++) {
            char temp_path[PATH_MAX];
            strcpy(temp_path, mfu_flist_file_get_name(filtered, idx));
            strcat(temp_path, suffix);
            if(!mfu_access(temp_path, F_OK)) {
                /* the file already exists */
                attempt = 1;
                break;
            }
        }

        /* do a reduce to figure out if a rank has a file collision */
        MPI_Allreduce(&attempt, &retry, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);
    } while(retry != 0);

    uint64_t size = mfu_flist_size(filtered);
    /* create new files so we can restripe */
    for (idx = 0; idx < size; idx++) {
        char temp_path[PATH_MAX];
        strcpy(temp_path, mfu_flist_file_get_name(filtered, idx));
        strcat(temp_path, suffix);
        /* create a striped file at the temp file path */
        mfu_create_striped_file(temp_path, stripe_size, stripes, dest_pool);
    }
   
    MPI_Barrier(MPI_COMM_WORLD);

    /* found a suffix, now we need to break our files into chunks based on stripe size */
    mfu_file_chunk* file_chunks = mfu_file_chunk_list_alloc(filtered, stripe_size);
    mfu_file_chunk* p = file_chunks;
    while(p != NULL) {
        char temp_path[PATH_MAX];
        strcpy(temp_path, p->name);
        strcat(temp_path, suffix);

        /* write each chunk in our list */
        write_file_chunk(p, temp_path);
        p = p->next;
    }

    MPI_Barrier(MPI_COMM_WORLD);

    /* remove input file and rename temp file */
    for (idx = 0; idx < size; idx++) {
        mode_t mode = (mode_t) mfu_flist_file_get_mode(filtered, idx);
        const char *in_path = mfu_flist_file_get_name(filtered, idx);
        char out_path[PATH_MAX];
        strcpy(out_path, in_path);
        strcat(out_path, suffix);

        /* change the mode of the newly restriped file to be the same as the old one */
        if (mfu_chmod(out_path, (mode_t) mode) != 0) {
            printf("Failed to chmod file %s (%s)", out_path, strerror(errno));
            fflush(stdout);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        /* unlink the old file */
        if (mfu_unlink(in_path) != 0) {
            printf("Failed to remove input file %s\n", in_path);
            fflush(stdout);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        /* rename the new, restriped file to the old name */
        if (rename(out_path, in_path) != 0) {
            printf("Failed to rename file %s to %s\n", out_path, in_path);
            fflush(stdout);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    /* wait for everyone to finish */
    MPI_Barrier(MPI_COMM_WORLD);

    /* free the chunk list, filtered list, path parameters */
    mfu_file_chunk_list_free(&file_chunks);
    mfu_flist_free(&filtered);
    mfu_param_path_free_all(numpaths, paths);
    mfu_free(&paths);
    mfu_free(&filter_string);
    mfu_free(&src_pool);
    mfu_free(&dest_pool);
    mfu_free(&size_filter);

    mfu_finalize();
    MPI_Finalize();

    return 0;
}
