/*
 * graphke.h
 * Include everything from other headers
 *
 *  Created on: Oct 22, 2014
 *      Author: vinh
 */

#ifndef GRAPHKE_H_
#define GRAPHKE_H_

#include <db.h>
#include <stdlib.h>
#include <stdio.h>
#include <dirent.h>
#include <string.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <assert.h>
#include <pthread.h>
#include <regex.h>
#include <common.h>
#include <pqueue.h>

#define UPDATES_PER_BULK_PUT 10000
#define PTHREAD_NUM 8
#define PTHREAD_ENABLED 1
#define PTHREAD_DISABLED 0
#define MMAP_ENABLED 1
#define ID_TYPE_FORMAT "%lld"

typedef long long id_type;

typedef struct po_id POID;
typedef struct po_str POSTR;
typedef struct spo_id SPOID;
typedef struct spo_str SPOSTR;
typedef pthread_mutex_t mutex_t;

struct po_id{
	id_type predicate;
	id_type object;
};

struct po_str{
	char* predicate;
	char* object;
};

struct spo_id{
	id_type subject;
	id_type predicate;
	id_type object;
};

struct spo_str{
	char* subject;
	char* predicate;
	char* object;
};

enum TYPE{
    CHAR_P,
    INT,
    ID_TYPE,
    ID_TYPE_P,
    PO_ID,
    PO_STR,
    SPO_ID,
    SPO_STR
};

#define	mutex_init(m, attr)     pthread_mutex_init((m), (attr))
#define	mutex_lock(m)           pthread_mutex_lock(m)
#define	mutex_unlock(m)         pthread_mutex_unlock(m)

/* Input specified for the program*/
mutex_t thread_node_id_lock;
mutex_t merge_nodes_lock;
mutex_t read_dict_db_lock;
mutex_t read_rdict_db_lock;
mutex_t read_data_db_lock;
mutex_t read_data_s2po_db_lock;
mutex_t read_data_s2pnp2o_db_lock;

/* Global variables */
DB_ENV *env_home_p;
DB *config_db_p;
DB *dict_db_p;
DB *dict_pe_p;
DB *rdict_db_p;
DB *data_db_p;
DB *data_s2po_db_p;
DB *data_s2pnp2o_db_p;
DB *stat_s2po_db_p;
DB *stat_s2pnp2o_db_p;

DBTYPE db_type;
char *env_home;
char *db_home;
char *db_name;
char *config_db;
char *dict_db;
char *rdict_db;
char *data_db;
char *data_s2po_db;
char *data_s2pnp2o_db;
char *stat_s2po_db;
char *stat_s2pnp2o_db;

char *config_file;
char *db_file;
char *dict_file;
char *stat_file;

int load_threads;
int tmp_dbs_enabled;
char **files_p;
int num_files;
int debug;

/* Configuring databases and environments*/
int init_dbs(char*, u_int32_t);
int init_tmp_db();
void remove_tmp_db(DB *tmp_db);
DBTYPE get_dbtype(char*);


/* Temp database*/
DB_ENV *tmp_env_p;
DB *tmp_db_p;
char *tmp_db_name;
char *tmp_env_home;

/* For loading data to db */
int load_dir(char* rdf_dfile, char *, int, int);
void* load_dict_pthread(void* thread_id);
int start_load_dict_thread(int);
void* load_data_db_pthread(void* thread_id);
int start_load_data_thread(int);
int gen_stat_db(DB*, DB*);
int gen_reverse_dict_db(DB *, DB *);
int merge_dict_dbs(DB *, DB *);
int put_tmp_db(DB*, id_type, id_type, int);

/* Common database retrieval functions */
char* lookup_id_reverse(DB*, id_type id);
id_type lookup_id(DB *, char*);
id_type lookup_stat(DB *db, id_type id);

void db_op(char*, int, char*);
long get_db_size(DB *);
int print_db(DB *, enum TYPE ktype, enum TYPE vtype, int);
void print_dbt(void *, enum TYPE ktype);
char* poid_to_string(POID *poid);
char* postr_to_string(POSTR *postr);
char* spoid_to_string(SPOID *spoid);
char* spostr_to_string(SPOSTR *spostr);


/* For graph algorithms*/
/*Graph algo functions*/
int find_shortest_path(char*, char*);
int start_finding(id_type, id_type);
int explore_neighbors_loop(DB*, DB *dbp, id_type startid, id_type endid, id_type id, int distance, pri_queue pqueue, int sink_excluded);
void show_path(DB*, id_type, id_type);
int get_duplicate_values(DB*, id_type, enum TYPE, enum TYPE, int, pri_queue);


#endif /* GRAPHKE_H_ */
