/*
 * GraphKE

 * Copyrights 2014 Vinh Nguyen, Wright State University, USA.
 *
 */


#ifndef RDF_GRAPH_H_
#define RDF_GRAPH_H_

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
#include <uthash.h>

#ifndef DEBUG
#define DEBUG 1
#endif

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
mutex_t write_dict_db_lock;
mutex_t write_rdict_db_lock;
mutex_t write_data_db_lock;
mutex_t write_data_s2po_db_lock;
mutex_t write_data_s2pnp2o_db_lock;

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

/* For one round loading */
DB* tmp_id_mappings_p;
DB* tmp_data_s2po_p;

void* load_one_round_pthread(void* thread_id);
int start_load_one_round_thread(int);
int resolve_ids(DB*, DB*, DB*);

/* End one round loading*/


/* Configuring databases and environments*/
int init_dbs(char*, u_int32_t);
DBTYPE get_dbtype(char*);
int close_db(DB *);
int init_tmp_db(DB*, char*, char*);
int remove_tmp_db(DB*, char*);


/* For loading data to db */
int load_dir(char* rdf_dfile, char *, int, int);
void* load_dict_pthread(void* thread_id);
int start_load_dict_thread(int);
void* load_data_db_pthread(void* thread_id);
int start_load_data_thread(int);
int gen_stat_db(DB*, DB*);
int gen_reverse_dict_db(DB *, DB *);
int merge_dict_dbs(DB *, DB *);
int extract_term(char *buf, long start, long end, char*);


/* Common database retrieval functions */
char* lookup_id_reverse(DB*, id_type id);
id_type lookup_id(DB *, char*);
long get_db_size(DB *);
int print_db(DB *, enum TYPE ktype, enum TYPE vtype, int);
void print_dbt(void *, enum TYPE ktype);
char* poid_to_string(POID *poid);
char* postr_to_string(POSTR *postr);
char* spoid_to_string(SPOID *spoid);
char* spostr_to_string(SPOSTR *spostr);


/* For graph algorithms*/


#endif /* RDFGRAPH_H_ */
