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

#include <stdio.h>
#include <getopt.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <inttypes.h>
#include <unistd.h>
#include <errno.h>

#include <sys/types.h>     
#include <sys/socket.h>     
#include <netinet/in.h>     
#include <arpa/inet.h>   
#include <sys/epoll.h>  
#include <signal.h>
#include <syslog.h>

#include "mpi.h"
#include "libcircle.h"
#include "mfu.h" 

#define EPOLL_PORT 8000

#define BUFFER_SIZE 256 
#define MAX_EVENTS 10  
  
#include <sys/select.h>
#include <sys/param.h>   
#include <sys/types.h>   
#include <sys/stat.h>

#include <json-c/json.h>
#include <microhttpd.h>

#define PORT            8888
#define POSTBUFFERSIZE  32768
#define MAXNAMESIZE     20
#define MAXANSWERSIZE   512

#ifndef PATH_MAX
#define PATH_MAX	256
#endif

#define GET             0
#define POST            1

#define KEY_CONTENT_TYPE     "Content-Type"
#define KEY_CONTENT_JSON     "application/json"

struct MHD_Daemon *g_daemon;

struct connection_info_struct
{
	int connectiontype;
	char *answerstring;
	struct MHD_PostProcessor *postprocessor;
};

const char *askpage = "<html><body>\
                       mpifileutils http daemon<br>\
                       <form action=\"/namepost\" method=\"post\">\
                       <input name=\"name\" type=\"text\"\
                       <input type=\"submit\" value=\" Send \"></form>\
                       </body></html>";

const char *greetingpage =
	"<html><body><h1>Welcome, mpifileutils!</center></h1></body></html>";

const char *errorpage =
       "<html><body><h1>This doesn't seem to be right.</center></h1></body></html>";

#define HTTP_RESPONSE_400_ERRREQ		\
    "{\"status\":{\"code\":\"400\","		\
    "\"description\":\"Invalid json format\"}}"

#define HTTP_RESPONSE_404_NOTFOUND		\
    "{\"status\":{\"code\":\"404\","		\
    "\"description\":\"Resource Not Found\"}}"

#define HTTP_RESPONSE_201_CREATED		\
    "{\"status\":{\"code\":\"201\","		\
    "\"description\":\"Resource Created\"}}"

#define HTTP_RESPONSE_200_OK			\
    "{\"status\":{\"code\":\"200\","		\
    "\"description\":\"OK\"}}"

char flist_name[BUFFER_SIZE*2];
char destpath_name[BUFFER_SIZE];



int connect_index = 0;

char *json_get_value(const json_object *obj, char *name)
{
    if (!obj)
        return NULL;

    json_object_object_foreach(obj, key, val) {
        if (!strncmp(name, key, strlen(name))) {
            json_type type = json_object_get_type(val);

            if (type == json_type_string)
                return (char *)json_object_get_string(val);
            MFU_LOG(MFU_LOG_ERR, "Invalid value format, expected is string!\n");
        }
    }
    return NULL;
}

char *random_uuid(char buf[37])
{
    const char *c = "89ab";
    char *p = buf;
    int n;
    for( n = 0; n < 16; ++n )
    {
        int b = rand()%255;
        switch( n )
        {
            case 6:
                sprintf(p, "4%x", b%15 );
            break;
            case 8:
                sprintf(p, "%c%x", c[rand()%strlen(c)], b%15 );
            break;
            default:
                sprintf(p, "%02x", b);
            break;
        }
 
        p += 2;
        switch( n )
        {
            case 3:
            case 5:
            case 7:
            case 9:
                *p++ = '-';
                break;
        }
    }
    *p = 0;
    return buf;
}

char *generate_flist_file_name(char *flist_name)
{
    char guid[37];

    random_uuid(guid);

    sprintf(flist_name, "%s/%s-%d",destpath_name, guid, connect_index);

    return flist_name;
}

static int encode_fname(char* buf)
{
    char* ptr = buf;
    struct stat st;
    size_t len;
    int ret;

    ret = lstat(buf, &st);
    if (ret < 0)
        return ret;

    len = strlen(buf);
    ptr += len;

    *ptr = '|';
    ptr++;

    if (S_ISREG(st.st_mode)) {
        *ptr = 'F';
    }
    else if (S_ISDIR(st.st_mode)) {
        *ptr = 'D';
    }
    else if (S_ISLNK(st.st_mode)) {
        *ptr = 'L';
    }
    else {
        *ptr = 'U';
    }
    ptr++;

    *ptr = '\n';
    return 0;
}

int store_fnames_into_flist(char *fname_buff, char *flist_name)
{
    int fd = open(flist_name, O_RDWR|O_CREAT);
    char tmp_name[BUFFER_SIZE];

    if (fd < 0)
        return -1;

    char*token = strtok(fname_buff, ",");

    while (token != NULL) {
        memset(tmp_name, 0, BUFFER_SIZE);
        strcpy(tmp_name, token);
        if (encode_fname(tmp_name) < 0)
            continue;
        write(fd, tmp_name, strlen(tmp_name));
        token = strtok(NULL, ",");
    }
    close(fd);
    return 0;
}

int get_input_flist(json_object *obj)
{
    char *value;


    value = json_get_value(obj, "flist");
    if (value && strlen(value)) {
        char *buff = malloc(BUFFER_SIZE*BUFFER_SIZE);
        char name[BUFFER_SIZE*2];
        int ret;

        memset(buff, 0, BUFFER_SIZE*BUFFER_SIZE);
        connect_index++;

        generate_flist_file_name(name);

        strncpy(buff, value, strlen(value));

        ret = store_fnames_into_flist(buff, name);
        if (!ret)
            strncpy(flist_name, name, strlen(name));

        free(buff);
        return ret;
    }
    
    return -1;
}

static int check_json_content(void *cls, enum MHD_ValueKind kind, 
                              const char *key, const char *value)
{
    int *has_json = cls;

    if (strncmp(key, KEY_CONTENT_TYPE, strlen(KEY_CONTENT_TYPE)) == 0 &&
        strncmp(value, KEY_CONTENT_JSON, strlen(KEY_CONTENT_JSON)) == 0)
        *has_json = 1;

    return MHD_YES;
}

static int send_page(struct MHD_Connection *connection, const char *page)
{
    int ret;
    struct MHD_Response *response;

    response =
        MHD_create_response_from_buffer(strlen(page), (void *)page,
                                        MHD_RESPMEM_PERSISTENT);
    if (!response)
        return MHD_NO;

    ret = MHD_queue_response(connection, MHD_HTTP_OK, response);
    MHD_destroy_response(response);

    return ret;
}

static void request_completed(void *cls, struct MHD_Connection *connection,
                              void **con_cls, enum MHD_RequestTerminationCode toe)
{
    struct connection_info_struct *con_info = *con_cls;

    if (con_info == NULL)
        return;

    if (con_info->answerstring)
        free(con_info->answerstring);

    free(con_info);
    *con_cls = NULL;
}

static int answer_to_connection(void *cls, struct MHD_Connection *connection,
                                const char *url, const char *method,
                                const char *version, const char *upload_data,
                                size_t *upload_data_size, void **con_cls)
{
    const char * page = cls;
    int has_json = 0;

    MHD_get_connection_values(connection, MHD_HEADER_KIND,
                              &check_json_content, &has_json);

    if (*con_cls == NULL) {
        struct connection_info_struct *con_info;

        con_info = malloc(sizeof(struct connection_info_struct));
        if (con_info == NULL)
            return MHD_NO;

        con_info->answerstring = NULL;

        if (!strcmp(method, "POST"))
            con_info->connectiontype = POST;
        else
            con_info->connectiontype = GET;

        *con_cls = (void *)con_info;

        return MHD_YES;
    }

    if (!strcmp(method, "GET")) {
        if (has_json) {
            MFU_LOG(MFU_LOG_ERR, "Invalid form for GET!\n");
            return send_page(connection, errorpage);
        }

        return send_page(connection, askpage);
    }

    if (!strcmp (method, "POST")) {
        struct connection_info_struct *con_info = *con_cls;

        if (*upload_data_size) {
            con_info->answerstring = malloc(*upload_data_size + 1);
            strncpy(con_info->answerstring, upload_data, *upload_data_size);
            con_info->answerstring[*upload_data_size] = 0;
            *upload_data_size = 0;

            return MHD_YES;
        } else if (has_json) {
            int ret;

            MFU_LOG(MFU_LOG_INFO, "Post data: %s\n", con_info->answerstring);
            json_object *obj = json_tokener_parse(con_info->answerstring);
            ret = get_input_flist(obj);
            json_object_put(obj);
            if (!ret)
                return send_page(connection, HTTP_RESPONSE_200_OK);
        }
        MFU_LOG(MFU_LOG_ERR, "Invalid post format:[%s]\n", con_info->answerstring);
        return send_page(connection, HTTP_RESPONSE_400_ERRREQ);
    }

    return send_page(connection, HTTP_RESPONSE_404_NOTFOUND);
}

/** Print a usage message. */
void print_usage(void)
{
    printf("\n");
    printf("Usage: dcp [options] source target\n");
    printf("       dcp [options] source ... target_dir\n");
    printf("\n");
    printf("Options:\n");
    /* printf("  -d, --debug <level> - specify debug verbosity level (default info)\n"); */
#ifdef LUSTRE_SUPPORT
    /* printf("  -g, --grouplock <id> - use Lustre grouplock when reading/writing file\n"); */
#endif
    printf("  -i, --input <file>  - read source list from file\n");
    printf("  -p, --preserve      - preserve permissions, ownership, timestamps, extended attributes\n");
    printf("  -s, --synchronous   - use synchronous read/write calls (O_DIRECT)\n");
    printf("  -S, --sparse        - create sparse files when possible\n");
    printf("  -v, --verbose       - verbose output\n");
    printf("  -D, --daemon        - run as daemon\n");
    printf("  -h, --help          - print usage\n");
    printf("\n");
    fflush(stdout);
}

void do_copy_with_flist(const char *inputname, mfu_flist flist, int numpaths_src,
                        mfu_param_path* paths, const mfu_param_path* destpath,
                        mfu_copy_opts_t* mfu_copy_opts)
{
    struct mfu_flist_skip_args skip_args;

    /* otherwise, read list of files from input, but then stat each one */
    mfu_flist input_flist = mfu_flist_new();
    mfu_flist_read_cache(inputname, input_flist);

    skip_args.numpaths = numpaths_src;
    skip_args.paths = paths;
    mfu_flist_stat(input_flist, flist, mfu_input_flist_skip, (void *)&skip_args);
    mfu_flist_free(&input_flist);

    /* copy flist into destination */ 
    mfu_flist_copy(flist, numpaths_src, paths, destpath, mfu_copy_opts);

    mfu_flist_free(&flist);
    flist = mfu_flist_new();
}

int main(int argc, \
         char** argv)
{
    /* initialize MPI */
    MPI_Init(&argc, &argv);
    mfu_init();

    /* get our rank */
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    /* pointer to mfu_copy opts */
    mfu_copy_opts_t mfu_cp_opts; 
    mfu_copy_opts_t* mfu_copy_opts = &mfu_cp_opts; 
    
    /* By default, show info log messages. */
    /* we back off a level on CIRCLE verbosity since its INFO is verbose */
    CIRCLE_loglevel CIRCLE_debug = CIRCLE_LOG_WARN;
    mfu_debug_level = MFU_LOG_INFO;

    /* Set default chunk size */
    uint64_t chunk_size = (1*1024*1024);
    mfu_copy_opts->chunk_size = chunk_size ;

    /* By default, don't have iput file. */
    char* inputname = NULL;

    /* By default, don't bother to preserve all attributes. */
    mfu_copy_opts->preserve = 0;

    /* Lustre grouplock ID */
    mfu_copy_opts->grouplock_id; 

    /* By default, don't use O_DIRECT. */
    mfu_copy_opts->synchronous = 0;

    /* By default, don't use sparse file. */
    mfu_copy_opts->sparse = 0; 

    /* Daemon mode */
    int run_as_daemon = 0;

    int option_index = 0;
    static struct option long_options[] = {
        {"debug"                , required_argument, 0, 'd'},
        {"grouplock"            , required_argument, 0, 'g'},
        {"input"                , required_argument, 0, 'i'},
        {"preserve"             , no_argument      , 0, 'p'},
        {"synchronous"          , no_argument      , 0, 's'},
        {"sparse"               , no_argument      , 0, 'S'},
        {"verbose"              , no_argument      , 0, 'v'},
        {"daemon"               , no_argument      , 0, 'D'},
        {"help"                 , no_argument      , 0, 'h'},
        {0                      , 0                , 0, 0  }
    };

    /* Parse options */
    int usage = 0;
    while(1) {
        int c = getopt_long(
                    argc, argv, "d:g:hi:pusSvD",
                    long_options, &option_index
                );

        if (c == -1) {
            break;
        }

        switch(c) {
            case 'd':
                if(strncmp(optarg, "fatal", 5) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_FATAL;
                    mfu_debug_level = MFU_LOG_FATAL;
                    if(rank == 0) {
                        MFU_LOG(MFU_LOG_INFO, "Debug level set to: fatal");
                    }
                }
                else if(strncmp(optarg, "err", 3) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_ERR;
                    mfu_debug_level = MFU_LOG_ERR;
                    if(rank == 0) {
                        MFU_LOG(MFU_LOG_INFO, "Debug level set to: errors");
                    }
                }
                else if(strncmp(optarg, "warn", 4) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_WARN;
                    mfu_debug_level = MFU_LOG_WARN;
                    if(rank == 0) {
                        MFU_LOG(MFU_LOG_INFO, "Debug level set to: warnings");
                    }
                }
                else if(strncmp(optarg, "info", 4) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_WARN; /* we back off a level on CIRCLE verbosity */
                    mfu_debug_level = MFU_LOG_INFO;
                    if(rank == 0) {
                        MFU_LOG(MFU_LOG_INFO, "Debug level set to: info");
                    }
                }
                else if(strncmp(optarg, "dbg", 3) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_DBG;
                    mfu_debug_level = MFU_LOG_DBG;
                    if(rank == 0) {
                        MFU_LOG(MFU_LOG_INFO, "Debug level set to: debug");
                    }
                }
                else {
                    if(rank == 0) {
                        MFU_LOG(MFU_LOG_INFO, "Debug level `%s' not recognized. " \
                            "Defaulting to `info'.", optarg);
                    }
                }
                break;
#ifdef LUSTRE_SUPPORT
            case 'g':
                mfu_copy_opts->grouplock_id = atoi(optarg);
                if(rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "groulock ID: %d.",
                        mfu_copy_opts->grouplock_id);
                }
                break;
#endif
            case 'i':
                inputname = MFU_STRDUP(optarg);
                if(rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "Using input list.");
                }
                break;
            case 'p':
                mfu_copy_opts->preserve = 1;
                if(rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "Preserving file attributes.");
                }
                break;
            case 's':
                mfu_copy_opts->synchronous = 1;
                if(rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "Using synchronous read/write (O_DIRECT)");
                }
                break;
            case 'S':
                mfu_copy_opts->sparse = 1;
                if(rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "Using sparse file");
                }
                break;
            case 'v':
                mfu_debug_level = MFU_LOG_VERBOSE;
                break;
            case 'D':
                run_as_daemon = 1;
                break;
            case 'h':
                usage = 1;
                break;
            case '?':
                usage = 1;
                break;
            default:
                if(rank == 0) {
                    printf("?? getopt returned character code 0%o ??\n", c);
                }
        }
    }

    /* paths to walk come after the options */
    int numpaths = 0;
    int numpaths_src = 0;
    mfu_param_path* paths = NULL;
    if (optind < argc) {
        /* determine number of paths specified by user */
        numpaths = argc - optind;

        /* allocate space for each path */
        paths = (mfu_param_path*) MFU_MALLOC((size_t)numpaths * sizeof(mfu_param_path));

        /* process each path */
        char** argpaths = &argv[optind];
        mfu_param_path_set_all(numpaths, argpaths, paths);

        /* advance to next set of options */
        optind += numpaths;
    
        /* the last path is the destination path, all others are source paths */
        numpaths_src = numpaths - 1;
    }

    if (numpaths_src == 0) {
        if(rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "No source path found, at least one");
            print_usage();
        }

        mfu_param_path_free_all(numpaths, paths);
        mfu_free(&paths);
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    /* last item in the list is the destination path */
    const mfu_param_path* destpath = &paths[numpaths - 1];
    
    /* Parse the source and destination paths. */
    int valid, copy_into_dir;
    mfu_param_path_check_copy(numpaths_src, paths, destpath, &valid, &copy_into_dir);
    mfu_copy_opts->copy_into_dir = copy_into_dir; 
    /* exit job if we found a problem */
    if(!valid) {
        if(rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Exiting run");
        }
        mfu_param_path_free_all(numpaths, paths);
        mfu_free(&paths);
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    /* create an empty file list */
    mfu_flist flist = mfu_flist_new();

    if (run_as_daemon) {
        strcpy(destpath_name, destpath->target);

        while (1) {
            if (rank == 0) {
                memset(flist_name, 0, BUFFER_SIZE);

                if (!g_daemon) {
                    g_daemon = MHD_start_daemon(MHD_USE_SELECT_INTERNALLY|MHD_USE_DEBUG,
                                              PORT, NULL, NULL,&answer_to_connection,
                                              (void *)greetingpage,
                                              MHD_OPTION_NOTIFY_COMPLETED,
                                              request_completed, NULL,
                                              MHD_OPTION_END);
                }

                if (g_daemon == NULL) {
                    snprintf(flist_name, 4, "NONE");
                } else {
                    int get_flist = 0;
 
                    while (1) {
                        if (strlen(flist_name) > 0)
                            break;
                        else
                            sleep(1);
                    }
                }
            }

            MPI_Bcast(flist_name, BUFFER_SIZE, MPI_CHAR, 0, MPI_COMM_WORLD);
            MPI_Barrier(MPI_COMM_WORLD);

            if (!strncmp(flist_name, "NONE", 4))
                break;

            do_copy_with_flist(flist_name, flist, numpaths_src, paths,
                               destpath, mfu_copy_opts);

        }
     } else {
        if (inputname == NULL) {
            /* walk paths and fill in file list */
            int walk_stat = 1;
            int dir_perm  = 0;

            mfu_flist_walk_param_paths(numpaths_src, paths, walk_stat, dir_perm, flist);
        } else { 
            struct mfu_flist_skip_args skip_args;

            /* otherwise, read list of files from input, but then stat each one */
            mfu_flist input_flist = mfu_flist_new();
            mfu_flist_read_cache(inputname, input_flist);

            skip_args.numpaths = numpaths_src;
            skip_args.paths = paths;
            mfu_flist_stat(input_flist, flist, mfu_input_flist_skip, (void *)&skip_args);
            mfu_flist_free(&input_flist);
        }

        /* copy flist into destination */ 
        mfu_flist_copy(flist, numpaths_src, paths, destpath, mfu_copy_opts);
    }
    
    /* free the file list */
    mfu_flist_free(&flist);

    /* free the path parameters */
    mfu_param_path_free_all(numpaths, paths);

    /* free memory allocated to hold params */
    mfu_free(&paths);

    /* free the input file name */
    mfu_free(&inputname);

    /* shut down MPI */
    mfu_finalize();
    MPI_Finalize();

    return 0;
}
