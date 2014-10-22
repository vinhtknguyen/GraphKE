/*
 * gen_dict_gz.c
 * Using zlib to read the RDF file.
 *  Created on: Jun 30, 2014
 *      Author: Vinh Nguyen
 */

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
#include <pqueue.h>

#define UPDATES_PER_BULK_PUT 10000
#define PTHREAD_NUM 8
#define PTHREAD_ENABLED 1
#define PTHREAD_DISABLED 0
#define MMAP_ENABLED 1
#define mix(a,b,c) \
		{ \
	a -= b; a -= c; a ^= (c>>43); \
	b -= c; b -= a; b ^= (a<<9); \
	c -= a; c -= b; c ^= (b>>8); \
	a -= b; a -= c; a ^= (c>>38); \
	b -= c; b -= a; b ^= (a<<23); \
	c -= a; c -= b; c ^= (b>>5); \
	a -= b; a -= c; a ^= (c>>35); \
	b -= c; b -= a; b ^= (a<<49); \
	c -= a; c -= b; c ^= (b>>11); \
	a -= b; a -= c; a ^= (c>>12); \
	b -= c; b -= a; b ^= (a<<18); \
	c -= a; c -= b; c ^= (b>>22); \
		}

typedef pthread_mutex_t mutex_t;
typedef long long id_type;
char *id_type_format = "%lld";

typedef struct po_id POID;
typedef struct po_str POSTR;
typedef struct spo_id SPOID;
typedef struct spo_str SPOSTR;
typedef struct node NODE;
struct node{
	id_type nodeid;
	int distance;
};

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

enum DBP{
	DICT,
	RDICT,
	DS2PO,
	SS2PO
};

#define	mutex_init(m, attr)     pthread_mutex_init((m), (attr))
#define	mutex_lock(m)           pthread_mutex_lock(m)
#define	mutex_unlock(m)         pthread_mutex_unlock(m)

/* Input specified for the program*/
long cur_triple_id = 1;
id_type cur_node_id = 1;
int merge_lock = 0;
mutex_t thread_node_id_lock;
mutex_t merge_nodes_lock;
mutex_t read_dict_db_lock;
mutex_t read_rdict_db_lock;
mutex_t read_data_db_lock;
mutex_t read_data_s2po_db_lock;
mutex_t read_data_s2pnp2o_db_lock;

mutex_t populate_pri_queue_lock;
mutex_t populate_visited_map_lock;

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

char *env_home;
char *config_db;
char *dict_db;
char *rdict_db;
char *data_db;
char *data_s2po_db;
char *data_s2pnp2o_db;
char *stat_s2po_db;
char *stat_s2pnp2o_db;

char *db_home;
char *config_file;
char *db_file;
char *dict_file;
char *stat_file;

char* db_type;
int load_threads = PTHREAD_NUM;
int tmp_dbs_enabled = 0;
char **files_p;
int num_files;
int debug;

/* Temp database*/
DB_ENV *tmp_env_p;
DB *tmp_db_p;
char *tmp_db_name;
char *tmp_env_home;

/* Function declarations*/
u_int32_t hash(const void*, unsigned, u_int32_t);
long get_db_size(DB *);
int print_db(DB *, enum TYPE ktype, enum TYPE vtype, int);
void db_op(char*, int, char*);
void print_dbt(void *, enum TYPE ktype);
int init_dbs(u_int32_t);
int init_config_db(char*, char*, u_int32_t, u_int32_t);
int start_dbs(char*, char*, u_int32_t, u_int32_t);
int get_configuration(char*, char*, u_int32_t, u_int32_t);
char *get_config(DB*, char*);
DBTYPE get_dbtype(char*);

int init_tmp_db();
void remove_tmp_db(DB *tmp_db);
int put_tmp_db(DB*, id_type, id_type, int);
char* lookup_id_reverse(DB*, id_type id);
id_type lookup_id(DB *, char*);
id_type lookup_stat(DB *db, id_type id);
int get_duplicate_values(DB*, id_type, enum TYPE, enum TYPE, int, pri_queue);

/*Graph algo*/
int find_shortest_path(char*, char*);
int start_finding(id_type, id_type);
int explore_neighbors_loop(DB*, DB *dbp, id_type startid, id_type endid, id_type id, int distance, pri_queue pqueue, int sink_excluded);
void show_path(DB*, id_type, id_type);
char* poid_to_string(POID *poid);
char* postr_to_string(POSTR *postr);
char* spoid_to_string(SPOID *spoid);
char* spostr_to_string(SPOSTR *spostr);
/* Function implementation */
char* remove_newline(char*);
void timeval_print(struct timeval*);
int timeval_subtract(struct timeval *result, struct timeval *t2, struct timeval *t1);

char* poid_to_string(POID *poid){
	char p[256], o[256];
	sprintf(p, id_type_format, poid->predicate);
	sprintf(o, id_type_format, poid->object);
	char* ret = malloc(strlen(p) + strlen(o) + 2);
	strcpy(ret, p);
	strcat(ret, " ");
	strcat(ret, o);
	return ret;
}

char* postr_to_string(POSTR *postr){
	char* ret = malloc(strlen(postr->predicate) + strlen(postr->object) + 2);
	strcpy(ret, postr->predicate);
	strcat(ret, " ");
	strcat(ret, postr->object);
	return ret;
}

char* spoid_to_string(SPOID *spoid){
	char s[256], p[256], o[256];
	sprintf(s, id_type_format, spoid->subject);
	sprintf(p, id_type_format, spoid->predicate);
	sprintf(o, id_type_format, spoid->object);
	char* ret = malloc(strlen(s) + strlen(p) + strlen(o) + 3);
	strcpy(ret, s);
	strcat(ret, " ");
	strcat(ret, p);
	strcat(ret, " ");
	strcat(ret, o);
	return ret;
}

char* spostr_to_string(SPOSTR *spostr){
	char* ret = malloc(strlen(spostr->subject) + strlen(spostr->predicate) + strlen(spostr->object) + 3);
	strcpy(ret, spostr->subject);
	strcat(ret, "\t");
	strcat(ret, spostr->predicate);
	strcat(ret, "\t");
	strcat(ret, spostr->object);
	return ret;
}

void print_header(){
	puts("---------------------------------------------------------");
	puts("\tGraph engine for knowledge management");
	puts("\tCopyrights 2014 by Vinh Nguyen, vinh@knoesis.org.");
	puts("---------------------------------------------------------");
}

void print_usage(){
	puts("\nSelect one of the following commands:\n");
	puts(" print_db\t Print existing databases");
	puts(" size \t\t Getting the size of existing databases");
	puts(" neighbors\t Find all the outgoing nodes");
	puts(" path\t\t Find the shortest path between two nodes");
	puts("");
}


char* get_current_dir(){
	char *ret;
	if (debug == 1) printf("Looking for current directory:\n");
	char cwd[1024];
	if (getcwd(cwd, sizeof(cwd)) != NULL){
		ret = malloc(sizeof(cwd) + 1);
		strcpy(ret, cwd);
		if (debug == 1) printf("Found:%s\n", ret);
	}
	else
		perror("getcwd() error");
	return ret;
}

int main(int argc, char *argsv[]) {

	/* Input from user*/
	print_header();
	char *db_file_str = argsv[1];
	debug = atoi(argsv[2]);
	char *db_home_str = get_current_dir();

	if (debug == 1) printf("Current directory is:%s\n", db_home_str);

	if (db_home_str != NULL){
		printf("Connecting to database %s ...\n", db_file_str);
		int ret = test_existing_config_db(db_home_str, db_file_str);
		if (ret < 0){
			printf("Cannot access %s db. Please wait a few second and try it again.\n", db_file_str);
		} else {
			u_int32_t db_flags = DB_THREAD | DB_RDONLY;
			u_int32_t env_flags = DB_JOINENV | DB_INIT_LOCK | /* Initialize the locking subsystem */
					DB_INIT_MPOOL | /* Initialize the memory pool (in-memory cache) */
					//				DB_SYSTEM_MEM | /* Region files are backed by heap memory.  */
					DB_THREAD; /* Cause the environment to be free-threaded */

			ret = init_variables(db_home_str, db_home_str, db_file_str, "hash");
			if (ret < 0){
				printf("Cannot open the databases %s .\n", db_file_str);
			} else {
				start_dbs(db_home_str, db_file_str, db_flags, env_flags);
				char *buffer = NULL, *para = NULL;  // getline will alloc
				int bytes_read = 1;
				size_t buffsize = 100;
				print_usage();
				printf(">");
				bytes_read = getline(&buffer, &buffsize, stdin);
				int stop = 0;
				while( bytes_read != -1 && stop == 0) /* break with ^D or ^Z */
				{
					buffer = remove_newline(buffer);
					if (debug == 1) printf("Got it:%s:\n", buffer);
					if ((para = strstr(buffer, "path")) != NULL){

						char *start = NULL, *end = NULL;
						// Getting the input
						printf("start node:");
						bytes_read = getline(&start, &buffsize, stdin);
						start = remove_newline(start);
						if (debug == 1) printf("Start at %s:\n", start);

						printf("end node:");
						bytes_read = getline(&end, &buffsize, stdin);
						end = remove_newline(end);

						if (debug == 1) printf("End at %s:\n", end);
						//						if (debug == 1) printf("Finding the shortest path from %s to %s\n", start, end);

						struct timeval tvBegin, tvEndDict, tvEndReverse, tvDiff;
						gettimeofday(&tvBegin, NULL);
						if (debug == 1) timeval_print(&tvBegin);


						find_shortest_path(start, end);

						//end of generating dictionary
						gettimeofday(&tvEndDict, NULL);
						if (debug == 1) timeval_print(&tvEndDict);

						// diff
						timeval_subtract(&tvDiff, &tvEndDict, &tvBegin);
						printf("Total: %ld.%06ld sec\n", (long int)tvDiff.tv_sec,
								(long int)tvDiff.tv_usec);
					} else if ((para = strstr(buffer, "neighbors")) != NULL){
						printf("node:");
						char *node = NULL;
						bytes_read = getline(&node, &buffsize, stdin);
						node = remove_newline(node);
						if (debug == 1) printf("Getting neighbors for %s:\n", node);
						struct timeval tvBegin, tvEndDict, tvEndReverse, tvDiff;
						gettimeofday(&tvBegin, NULL);
						if (debug == 1) timeval_print(&tvBegin);

						get_neighbors_id(node);

						//end of generating dictionary
						gettimeofday(&tvEndDict, NULL);
						if (debug == 1) timeval_print(&tvEndDict);

						// diff
						timeval_subtract(&tvDiff, &tvEndDict, &tvBegin);
						printf("Total: %ld.%06ld sec\n", (long int)tvDiff.tv_sec,
								(long int)tvDiff.tv_usec);
					} else if (strstr(buffer, "get_neighbors_str") != NULL){

					} else if ((para = strstr(buffer, "print_db")) != NULL){
						char *db = NULL, *size_str = NULL;
						printf("db name [dict rdict data_s2po stat_s2po]:");
						bytes_read = getline(&db, &buffsize, stdin);
						db = remove_newline(db);

						printf("how many items:");
						bytes_read = getline(&size_str, &buffsize, stdin);
						size_str = remove_newline(size_str);

						if (debug == 1) printf("Printing %s items in db %s:\n", size_str, db);
						struct timeval tvBegin, tvEndDict, tvEndReverse, tvDiff;
						gettimeofday(&tvBegin, NULL);
						if (debug == 1) timeval_print(&tvBegin);

						int size = atoi(size_str);
						db_op(db, size, "print_db");

						//end of generating dictionary
						gettimeofday(&tvEndDict, NULL);
						if (debug == 1) timeval_print(&tvEndDict);

						// diff
						timeval_subtract(&tvDiff, &tvEndDict, &tvBegin);
						printf("Total: %ld.%06ld sec\n", (long int)tvDiff.tv_sec,
								(long int)tvDiff.tv_usec);
					} else if (strstr(buffer, "size") != NULL){
						char *db = NULL;
						printf("db name [dict rdict data_s2po stat_s2po]:");
						bytes_read = getline(&db, &buffsize, stdin);
						db = remove_newline(db);

						struct timeval tvBegin, tvEndDict, tvEndReverse, tvDiff;
						gettimeofday(&tvBegin, NULL);

						db_op(db, 0, "size");

						//end of generating dictionary
						gettimeofday(&tvEndDict, NULL);
						if (debug == 1) timeval_print(&tvEndDict);

						// diff
						timeval_subtract(&tvDiff, &tvEndDict, &tvBegin);
						printf("Total: %ld.%06ld sec\n", (long int)tvDiff.tv_sec,
								(long int)tvDiff.tv_usec);
//						printf("Size: %d\n", get_db_size(dict_db_p));
					} else if (strstr(buffer, "quit") != NULL){
						printf("quitting ...");
						stop = 1;
						break;
					} else {
						print_usage();
					}
					printf(">");
					buffer = NULL;
					bytes_read = getline(&buffer, &buffsize, stdin);
				}
				free(db_home_str);
				close_dbs();
				printf(" done.\nGood bye.\n");
			}
		}
	}
	// begin

	return 0;
	err:
	close_dbs();
}

void db_op(char *db, int size, char* op){
	DB *dbp;
	if (strcmp(db, "dict") == 0){
		dbp = dict_db_p;
		if (strcmp(op, "print_db") == 0){
			print_db(dbp, CHAR_P, ID_TYPE, size);
		} else {
			printf("Size: %ld\n", get_db_size(dbp));
		}
	} else if (strcmp(db, "rdict") == 0){
		dbp = rdict_db_p;
		if (strcmp(op, "print_db") == 0){
			print_db(dbp, ID_TYPE, CHAR_P, size);
		} else {
			printf("Size: %ld\n", get_db_size(dbp));
		}
	} else if (strcmp(db, "ds2po") == 0){
		dbp = data_s2po_db_p;
		if (strcmp(op, "print_db") == 0){
			print_db(dbp, ID_TYPE, PO_ID, size);
		} else {
			printf("Size: %ld\n", get_db_size(dbp));
		}
	} else if (strcmp(db, "ss2po") == 0){
		dbp = stat_s2po_db_p;
		if (strcmp(op, "print_db") == 0){
			print_db(dbp, ID_TYPE, ID_TYPE, size);
		} else {
			printf("Size: %ld\n", get_db_size(dbp));
		}
	} else {
		dbp = data_s2po_db_p;
		if (strcmp(op, "print_db") == 0){
			print_db(dbp, ID_TYPE, PO_ID, size);
		} else {
			printf("Size: %ld\n", get_db_size(dbp));
		}
	}
}

char* remove_newline(char *s)
{
	int len = strlen(s);

	if (len > 0 && s[len-1] == '\n')  // if there's a newline
		s[len-1] = '\0';          // truncate the string

	return s;
}

int get_neighbors_id(char *node){
	int ret;
	// Look up for id
	id_type nodeid = lookup_id(dict_db_p, node);
	if (debug == 1) {
		printf("Getting neighbors for: ");
		printf(id_type_format, nodeid);
		printf(" ...\n");
	}

	if (nodeid < 0){
		printf("%s does not exist\n", node);
		ret = -1;
		return ret;
	}

	if (ret >= 0){

		pri_queue pqueue = priq_new(0);
		int sink_excluded = 0;
		ret = get_duplicate_values(data_s2po_db_p, nodeid, ID_TYPE, PO_ID, sink_excluded, pqueue);
		int i;
		pri_type c, p;
		while ((c = (id_type)priq_pop(pqueue, &p))){
			printf("Popped item: ");
			printf(id_type_format, c);
			printf(" with pri: ");
			printf(pri_type_format, p);
			printf("\n");
		}
	}

	return ret;
}

int start_finding(id_type startid, id_type endid){
	int ret;

	int sink_excluded = 1;

	if (debug == 1) printf("Initialize tmp_db\n");
//	DB* tmp_db;
	init_tmp_db();
	pri_queue pqueue = priq_new(0);
	id_type id;
	pri_type dis = 0;

	id_type nodeid_stat = lookup_stat(stat_s2po_db_p, startid);
	if (nodeid_stat < 0){
		printf("No path found.\n");
		return -1;
	} else {
		if (debug == 1) printf("Putting startid into the queue and the map\n");
		priq_push(pqueue, (void*)startid, dis);
		ret = put_tmp_db(tmp_db_p, startid, startid, (int)dis);
	}

	if (debug == 1) printf("Start finding\n");
	while ((id = (id_type)priq_pop(pqueue, &dis))){
		// Not yet visited
//		if (debug == 1){
//			printf("Exploring node: ");
//			printf(id_type_format, id);
//			printf(" with pri: %d", dis);
//			printf("\n");
//		}
		ret = explore_neighbors_loop(tmp_db_p, data_s2po_db_p, startid, endid, id, (int)dis, pqueue, sink_excluded);
		if (ret == 1){
			break;
		}
//		if (debug == 1){
//			printf("Popped item: ");
//			printf(id_type_format, id);
//			printf(" with pri: %d", dis);
//			printf("\n");
//		}
		// Get the set of (predicate, object) pairs
	}
	// Print the path
	show_path(tmp_db_p, startid, endid);
	printf("\n");

	// Freeing the variables
	free(pqueue->buf);
	free(pqueue);
	remove_tmp_db(tmp_db_p);
	return 0;
}

int find_shortest_path(char *start, char *end){
	int ret = 1;

	// Look up for id
	id_type startid = lookup_id(dict_db_p, start);
	if (startid < 0){
		printf("%s does not exist\n", start);
		ret = -1;
	}
	id_type endid = lookup_id(dict_db_p, end);
	if (endid < 0){
		printf("%s does not exist\n", end);
		ret = -1;
	}
	if (debug == 1) {
		printf("Finding the shortest path from ");
		printf(id_type_format, startid);
		printf(" to ");
		printf(id_type_format, endid);
		printf("...\n");
	}

	if (ret >= 0){
		// Searching for shortest path
		start_finding(startid, endid);
	} else {
		if (debug == 1) printf("Invalid input\n");
	}
	return ret;
}


/*
 * Given a tring, find its id
 */

id_type lookup_id(DB *db, char *str){
	int ret;
	DBT key, value;
	id_type id;

	memset(&key, 0, sizeof(DBT));
	memset(&value, 0, sizeof(DBT));

	key.size = sizeof(char) * (strlen(str) + 1);
	key.data = str;

	value.flags = DB_DBT_MALLOC;

	//	if (debug == 1) printf("Lookup id for:%s\n",str);
	ret = db->get(db, 0, &key, &value, 0);
	//	if (debug == 1) printf(".....:");

	if (ret == DB_NOTFOUND){
		id = -1;
	} else {
		id = *(id_type*) value.data;
		free(value.data);
		//		if (debug == 1) printf("Found:");
		//		if (debug == 1) printf(id_type_format, id);
		//		if (debug == 1) printf("\n");
	}
	return id;
}


void show_path(DB* tmp_db, id_type startid, id_type endid){


	// print previous id of endid
	DBT key, value;
	memset(&key, 0, sizeof(DBT));
	memset(&value, 0, sizeof(DBT));
	key.data = &endid;
	key.size = sizeof(id_type);
	key.flags = DB_DBT_MALLOC;
	value.flags = DB_DBT_MALLOC;
//	if (debug == 1) printf("Path: \n");
	int ret = tmp_db->get(tmp_db, 0, &key, &value, 0);
	if (ret == 0){
		NODE *prevnode = (NODE*)value.data;
		if (prevnode->nodeid != startid){
			show_path(tmp_db, startid, prevnode->nodeid);
		} else {
			// print this id first
			printf(id_type_format, startid);
			printf("\t");
		}
	}
	// print this id in the last
	printf(id_type_format, endid);
	printf("\t");
	free(value.data);
}

int put_tmp_db(DB* dbp, id_type id, id_type prev, int dis){
	int ret = -1;
	int update = 0;

	DBT key, value, tmp;
	memset(&key, 0, sizeof(DBT));
	memset(&value, 0, sizeof(DBT));
	memset(&tmp, 0, sizeof(DBT));

	id_type keyid = id;
	key.size = sizeof(id_type);
	key.flags = DB_DBT_MALLOC;
	key.data = &keyid;

	tmp.flags = DB_DBT_MALLOC;

	//	tmpnode.flags = DB_DBT_MALLOC;
//	if (debug == 1) {
//		printf("Getting key: ");
//		printf(id_type_format, id);
//		printf("\t previd: ");
//		printf(id_type_format, prev);
//		printf("\t dis: %d\n", dis);
//	}
//
	ret = dbp->get(dbp, 0, &key, &tmp, 0);

	if (ret == DB_NOTFOUND){
		update = 1;
	} else if (ret == 0){
		NODE *tmpnode = (NODE*) tmp.data;
		if (dis < tmpnode->distance){
			update = 1;
		}
		free(tmp.data);
	} else {
		return -1;
	}
	if (update == 1){
		NODE *prevnode = malloc(sizeof(NODE));
		prevnode->nodeid = prev;
		prevnode->distance = dis;
		value.data = prevnode;
		value.size = sizeof(NODE);
		value.flags = DB_DBT_MALLOC;
		ret = dbp->put(dbp, 0, &key, &value, 0);
		free(prevnode);
		if (debug == 1) {
			printf("Putting key: ");
			printf(id_type_format, id);
			printf("\t previd: ");
			printf(id_type_format, prev);
			printf("\t dis: %d\n", dis);
		}
	}
	return update;
}

int init_tmp_db(){
	int ret;

//	tmp_env_home = malloc(strlen(env_home) + 5);
//	strcpy(tmp_env_home, env_home);
//	strcat(tmp_env_home, "/tmp");

	/* Create the environment */
//	ret = db_env_create(&tmp_env_p, 0);
//	if (ret != 0) {
//		fprintf(stderr, "Error creating environment handle: %s\n",
//				db_strerror(ret));
//		goto err;
//	}
//
//	u_int32_t env_flags = DB_CREATE | /* Create the environment if it does not exist */
//			DB_INIT_LOCK | /* Initialize the locking subsystem */
//			DB_INIT_MPOOL | /* Initialize the memory pool (in-memory cache) */
//			DB_PRIVATE | /* Region files are backed by heap memory.  */
//			DB_THREAD; /* Cause the environment to be free-threaded */
//
//	/*
//	 * Specify the size of the in-memory cache.
//	 */
//	ret = tmp_env_p->set_cachesize(tmp_env_p, 0, 512 * 1024 * 1024, 1);
//	if (ret != 0) {
//		fprintf(stderr, "Error increasing the cache size: %s\n",
//				db_strerror(ret));
//		goto err;
//	}
//
//	/* Now actually open the environment */
//	ret = tmp_env_p->open(tmp_env_p, tmp_env_home, env_flags, 0);
//	if (ret != 0) {
//		fprintf(stderr, "Error opening environment: %s\n", db_strerror(ret));
//		goto err;
//	}

	ret = db_create(&tmp_db_p, env_home_p, 0);
	if (ret != 0) {
		fprintf(stderr, "Error creating database: %s\n", db_strerror(ret));
		goto err;
	}
//	tmp_db->set_priority(tmp_db, DB_PRIORITY_VERY_HIGH);

	tmp_db_name = "tmp2";
//	tmp_db_file = malloc(strlen(db_file) + 5);
//	strcpy(tmp_db_file, db_file);
//	strcat(tmp_db_file, "_tmp");

	if (debug == 1) printf("Opening tmp database %s\n", tmp_db_name);
	/* Now open the persistent database for the dictionary */
	ret = tmp_db_p->open(tmp_db_p, /* Pointer to the database */
			0, /* Txn pointer */
			0, /* prog_db_file File name */
			tmp_db_name, /* Logical db name */
			DB_HASH, /* Database type (using btree) */
			DB_CREATE | DB_THREAD, /* Open flags */
			0); /* File mode. Using defaults */

	if (ret != 0) {
		fprintf(stderr, "Error opening database: %s\n", db_strerror(ret));
		goto err;
	}
	if (debug == 1) printf("Done opening tmp database\n");

	return ret;
	err:
	ret = tmp_db_p->close(tmp_db_p, 0);
	if (ret != 0) {
		fprintf(stderr, "%s database close failed.\n", db_strerror(ret));
		goto err;
	}
	u_int32_t tmp;
	ret = env_home_p->dbremove(env_home_p, 0, 0, tmp_db_name, 0);
	if (ret != 0) {
		fprintf(stderr, "%s database truncate failed.\n", db_strerror(ret));
		goto err;
	}
}

void remove_tmp_db(DB *tmp_db){
	if (tmp_db != NULL){
		if (debug == 1) printf("Removing tmp db\n");
		int ret = tmp_db->close(tmp_db, 0);
		u_int32_t tmp;
		ret = env_home_p->dbremove(env_home_p, 0, 0, tmp_db_name, 0);
	}
}

id_type lookup_stat(DB *db, id_type id){
	id_type ret;
	DBT key, value;

	memset(&key, 0, sizeof(DBT));
	memset(&value, 0, sizeof(DBT));

	key.size = sizeof(id_type);
	key.data = &id;
	key.flags = DB_DBT_MALLOC;

	value.flags = DB_DBT_MALLOC;

	//	if (debug == 1) {
	//		printf("Lookup id for:");
	//		printf(id_type_format, id);
	//		printf("\n");
	//	}
	ret = db->get(db, 0, &key, &value, 0);
	//	if (debug == 1) printf(".....:");

	if (ret == DB_NOTFOUND){
		id = -1;
	} else {
		id = *(id_type*) value.data;
		free(value.data);
		//				if (debug == 1) printf("Found:");
		//				if (debug == 1) printf(id_type_format, id);
		//				if (debug == 1) printf("\n");
	}
	return id;
}


char *lookup_id_reverse(DB *db, id_type id){
	int ret;
	DBT key, value;

	char *str = NULL;
	memset(&key, 0, sizeof(DBT));
	memset(&value, 0, sizeof(DBT));

	key.size = sizeof(id_type);
	key.data = &id;

	key.flags = DB_DBT_MALLOC;
	value.flags = DB_DBT_MALLOC;


	ret = db->get(db, 0, &key, &value, 0);

	str = (char*) value.data;
	return str;
}

DBTYPE get_dbtype(char *db_type_str){
	if (debug == 1) printf("DB type:%s:vs:%s.\n", db_type_str, db_type);
	if (strcmp(db_type_str, "btree") == 0){
		return DB_BTREE;
	} else {
		return DB_HASH;
	}
}


/*
 * This simply counts the number of records contained in the
 * database and returns the result. You can use this function
 * in three ways:
 *
 * First call it with an active txn handle (this is what the
 *  example currently does).
 *
 * Secondly, configure the cursor for uncommitted reads.
 *
 * Third, call count_records AFTER the writer has committed
 *    its transaction.
 *
 * If you do none of these things, the writer thread will
 * self-deadlock.
 *
 * Note that this function exists only for illustrative purposes.
 * A more straight-forward way to count the number of records in
 * a database is to use DB->stat() or DB->stat_print().
 */

long get_db_size(DB *dbp) {
	DBT key, value;
	DBC *cursorp;
	long count, ret;

	cursorp = NULL;
	count = 0;

	/* Get the cursor */
	ret = dbp->cursor(dbp, 0, &cursorp, 0);
	if (ret != 0) {
		dbp->err(dbp, ret, "count_records: cursor open failed.");
		goto cursor_err;
	}

	/* Get the key DBT used for the database read */
	memset(&key, 0, sizeof(DBT));
	memset(&value, 0, sizeof(DBT));
	do {
		ret = cursorp->get(cursorp, &key, &value, DB_NEXT);
		switch (ret) {
		case 0:
			count++;
			break;
		case DB_NOTFOUND:
			break;
		default:
			dbp->err(dbp, ret, "Count records unspecified error");
			goto cursor_err;
		}
	} while (ret == 0);

	cursor_err: if (cursorp != NULL) {
		ret = cursorp->close(cursorp);
		if (ret != 0) {
			dbp->err(dbp, ret, "count_records: cursor close failed.");
		}
	}

	return (count);
}

u_int32_t hash(const void* buffer, unsigned length, u_int32_t init){
	char* key = (char*)(buffer);
	u_int32_t a=0x9E3779B9,b=0x9E3779B9,c=init;
	// Hash the main part
	while (length>=12) {
		a += (u_int32_t)(key[0])|(u_int32_t)(key[1]<<8)|(u_int32_t)(key[2]<<16)|(u_int32_t)(key[3]<<24);
		b += (u_int32_t)(key[4])|(u_int32_t)(key[5]<<8)|(u_int32_t)(key[6]<<16)|(u_int32_t)(key[7]<<24);
		c += (u_int32_t)(key[8])|(u_int32_t)(key[9]<<8)|(u_int32_t)(key[10]<<16)|(u_int32_t)(key[11]<<24);
		mix(a,b,c);
		key += 12; length-=12;
	}
	// Hash the tail (max 11 bytes)
	c += length;
	switch (length) {
	case 11: c += (u_int32_t)(key[10]<<24); break;
	case 10: c += (u_int32_t)(key[9]<<16); break;
	case 9:  c += (u_int32_t)(key[8]<<8); break;
	case 8:  b += (u_int32_t)(key[7]<<24); break;
	case 7:  b += (u_int32_t)(key[6]<<16); break;
	case 6:  b += (u_int32_t)(key[5]<<8); break;
	case 5:  b += (u_int32_t)(key[4]); break;
	case 4:  a += (u_int32_t)(key[3]<<24); break;
	case 3:  a += (u_int32_t)(key[2]<<16); break;
	case 2:  a += (u_int32_t)(key[1]<<8); break;
	case 1:  a += (u_int32_t)(key[0]); break;
	default: break;
	}
	mix(a,b,c);

	return c;
}


/*
 * Printing the first n elements of the db
 */

void print_dbt(void* data, enum TYPE type){
	char *ret;
	switch (type){
	case CHAR_P:
		printf("%s", (char*)data);
		break;
	case INT:
		printf("%d", *(int*)data);
		break;
	case ID_TYPE:
		printf(id_type_format, *(id_type*)data);
		break;
	case PO_ID:
		ret = poid_to_string((POID*)data);
		printf("%s", ret);
		free(ret);
		break;
	case PO_STR:
		ret = postr_to_string((POSTR*)data);
		printf("%s", ret);
		free(ret);
		break;
	case SPO_ID:
		ret = spoid_to_string((SPOID*)data);
		printf("%s", ret);
		free(ret);
		break;
	case SPO_STR:
		ret = spostr_to_string((SPOSTR*)data);
		printf("%s", ret);
		free(ret);
		break;
	default:
		printf("%s", (char*) data);
		break;
	}
}

int explore_neighbors_loop(DB* tmp_db, DB *dbp, id_type startid, id_type endid, id_type id, int distance, pri_queue pqueue, int sink_excluded){
	DBT key, value;
	DBC *cursorp;
	int count, ret, ret1;
	id_type predicate, object, predicate_stat, object_stat;

	cursorp = NULL;
	count = 0;
	/* Get the cursor */
	ret = dbp->cursor(dbp, 0, &cursorp, 0);
	if (ret != 0) {
		dbp->err(dbp, ret, "count_records: cursor open failed.");
		goto cursor_err;
	}

	memset(&key, 0, sizeof(DBT));
	memset(&value, 0, sizeof(DBT));
	key.data = &id;
	key.size = sizeof(id_type);
	key.flags = DB_DBT_MALLOC;
	value.flags = DB_DBT_MALLOC;

	ret = cursorp->c_get(cursorp, &key, &value, DB_SET);
	do {
		/* Get the key DBT used for the database read */
		//		if (debug == 1){
		//			printf("value:");
		//			print_dbt((void*)key.data, ktype);
		//			printf("\t");
		//			print_dbt((void*)value.data, vtype);
		//			printf("\n");
		//		}
		switch (ret) {
		case 0:
			count++;
			POID *poid = (POID*)value.data;
			predicate = poid->predicate;
			if (predicate == endid){
				ret1 = put_tmp_db(tmp_db, object, predicate, distance + 2);
				printf("Found shortest path of length %d\n", distance + 1);
//				show_path(tmp_db_p, startid, id);
//				printf(id_type_format, id);
//				printf("\t");
//				printf(id_type_format, predicate);
				return 1;
			}
			if (predicate % 2 == 0 && sink_excluded == 1){
				ret1 = put_tmp_db(tmp_db, predicate, id, distance + 1);
				if (ret1 == 1){
					// Shorter distance found
					priq_push(pqueue, (void*)predicate, distance + 1);
				}
//				if (debug == 1){
//					printf("Value:");
//					printf(id_type_format, predicate);
//					printf("\n");
//				}
			}

			object = poid->object;
			if (object == endid){
				ret1 = put_tmp_db(tmp_db, object, predicate, distance + 2);
				printf("Found shortest path of length %d\n", distance + 2);
//				show_path(tmp_db_p, startid, id);
//				printf(id_type_format, id);
//				printf("\t");
				return 1;
			}
			if (object % 2 == 0 && sink_excluded == 1){
				ret1 = put_tmp_db(tmp_db, object, predicate, distance + 2);
				if (ret1 == 1){
					// Shorter distance found
					priq_push(pqueue, (void*)object, distance + 2);
				}
//				if (debug == 1){
//					printf("Value:");
//					printf(id_type_format, object);
//					printf("\n");
//				}
			}

			break;
		case DB_NOTFOUND:
			break;
		default:
			if (debug == 1) printf("error setting db_set\n");
			goto cursor_err;
		}
		//		memset(&value, 0, sizeof(DBT));
		ret = cursorp->c_get(cursorp, &key, &value, DB_NEXT_DUP);

	} while (ret == 0);

	cursorp->close(cursorp);

	return (count);
	cursor_err: if (cursorp != NULL) {
		ret = cursorp->close(cursorp);
	}

}

int get_duplicate_values(DB *dbp, id_type id, enum TYPE ktype, enum TYPE vtype, int sink_excluded, pri_queue pq){
	DBT key, value;
	DBC *cursorp;
	int count, ret;
	id_type predicate, object, predicate_stat, object_stat;

	cursorp = NULL;
	count = 0;
	/* Get the cursor */
	ret = dbp->cursor(dbp, 0, &cursorp, 0);
	if (ret != 0) {
		dbp->err(dbp, ret, "count_records: cursor open failed.");
		goto cursor_err;
	}

	memset(&key, 0, sizeof(DBT));
	memset(&value, 0, sizeof(DBT));
	key.data = &id;
	key.size = sizeof(id_type);
	key.flags = DB_DBT_MALLOC;
	value.flags = DB_DBT_MALLOC;

	ret = cursorp->c_get(cursorp, &key, &value, DB_SET);
	do {
		/* Get the key DBT used for the database read */
		//		if (debug == 1){
		//			printf("value:");
		//			print_dbt((void*)key.data, ktype);
		//			printf("\t");
		//			print_dbt((void*)value.data, vtype);
		//			printf("\n");
		//		}
		switch (ret) {
		case 0:
			count++;
			POID *poid = (POID*)value.data;
			predicate = poid->predicate;
			if (predicate % 2 == 0){
				predicate_stat = lookup_stat(stat_s2po_db_p, predicate);
				if (sink_excluded == 1 && predicate_stat > 0){
					priq_push(pq, (void*)predicate, predicate_stat);
					if (debug == 1){
						printf("Value:");
						printf(id_type_format, predicate);
						printf("\t with pri:");
						printf(id_type_format, predicate_stat);
						printf("\n");
					}
				} else if (sink_excluded == 0){
					if (debug == 1){
						printf("Value:");
						printf(id_type_format, predicate);
						printf("\n");
					}
				}
			} else if (sink_excluded == 0){
				if (debug == 1){
					printf("Value:");
					printf(id_type_format, predicate);
					printf("\n");
				}
			}

			object = poid->object;
			if (object % 2 == 0){
				object_stat = lookup_stat(stat_s2po_db_p, object);
				if (sink_excluded == 1 && object_stat > 0){
					priq_push(pq, (void*)object, object_stat);
					if (debug == 1){
						printf("Value:");
						printf(id_type_format, object);
						printf("\t with pri:");
						printf(id_type_format, object_stat);
						printf("\n");
					}
				} else if (sink_excluded == 0){
					if (debug == 1){
						printf("Value:");
						printf(id_type_format, object);
						printf("\n");
					}
				}
			} else  if (sink_excluded == 0){
				if (debug == 1){
					printf("Value:");
					printf(id_type_format, object);
					printf("\n");
				}
			}


			break;
		case DB_NOTFOUND:
			break;
		default:
			if (debug == 1) printf("error setting db_set\n");
			goto cursor_err;
		}
		//		memset(&value, 0, sizeof(DBT));
		ret = cursorp->c_get(cursorp, &key, &value, DB_NEXT_DUP);

	} while (ret == 0);

	cursorp->close(cursorp);

	return (count);
	cursor_err: if (cursorp != NULL) {
		ret = cursorp->close(cursorp);
	}

}

int print_db(DB *dbp, enum TYPE ktype, enum TYPE vtype, int n) {
	DBT key, value, value_dup;
	DBC *cursorp;
	int count, ret1, ret2;

	cursorp = NULL;
	count = 0;

	/* Get the cursor */
	ret1 = dbp->cursor(dbp, 0, &cursorp, 0);
	if (ret1 != 0) {
		dbp->err(dbp, ret1, "count_records: cursor open failed.");
		goto cursor_err;
	}

	do {
		/* Get the key DBT used for the database read */
		memset(&key, 0, sizeof(DBT));
		memset(&value, 0, sizeof(DBT));
		memset(&value_dup, 0, sizeof(DBT));
		ret1 = cursorp->c_get(cursorp, &key, &value, DB_NEXT);
		switch (ret1) {
		case 0:
			count++;
			print_dbt((void*)key.data, ktype);
			printf("\t");
			print_dbt((void*)value.data, vtype);
			printf("\n");
			break;
		case DB_NOTFOUND:
			break;
		default:
			dbp->err(dbp, ret1, "Count records unspecified error");
			goto cursor_err;
		}
	} while (ret1 == 0 & count < n);

	cursor_err: if (cursorp != NULL) {
		ret1 = cursorp->close(cursorp);
		if (ret1 != 0) {
			dbp->err(dbp, ret1, "count_records: cursor close failed.");
		}
	}

	return (count);
}

int close_dbs() {
	int ret, ret_t;
	if (debug == 1) printf("Closing databases and environment ...");
	/* Close our database handle, if it was opened. */

	if (dict_db_p != NULL) {
		ret_t = dict_db_p->close(dict_db_p, 0);
		if (ret_t != 0) {
			fprintf(stderr, "%s database close failed.\n", db_strerror(ret_t));
			ret = ret_t;
		}
	}

	if (rdict_db_p != NULL) {
		ret_t = rdict_db_p->close(rdict_db_p, 0);
		if (ret_t != 0) {
			fprintf(stderr, "%s database close failed.\n", db_strerror(ret_t));
			ret = ret_t;
		}
	}

	if (data_db_p != NULL) {
		ret_t = data_db_p->close(data_db_p, 0);
		if (ret_t != 0) {
			fprintf(stderr, "%s database close failed.\n", db_strerror(ret_t));
			ret = ret_t;
		}
	}

	if (data_s2po_db_p != NULL) {
		ret_t = data_s2po_db_p->close(data_s2po_db_p, 0);
		if (ret_t != 0) {
			fprintf(stderr, "%s database close failed.\n", db_strerror(ret_t));
			ret = ret_t;
		}
	}
	if (data_s2pnp2o_db_p != NULL) {
		ret_t = data_s2pnp2o_db_p->close(data_s2pnp2o_db_p, 0);
		if (ret_t != 0) {
			fprintf(stderr, "%s database close failed.\n", db_strerror(ret_t));
			ret = ret_t;
		}
	}

	if (stat_s2po_db_p != NULL) {
		ret_t = stat_s2po_db_p->close(stat_s2po_db_p, 0);
		if (ret_t != 0) {
			fprintf(stderr, "%s database close failed.\n", db_strerror(ret_t));
			ret = ret_t;
		}
	}
	if (stat_s2pnp2o_db_p != NULL) {
		ret_t = stat_s2pnp2o_db_p->close(stat_s2pnp2o_db_p, 0);
		if (ret_t != 0) {
			fprintf(stderr, "%s database close failed.\n", db_strerror(ret_t));
			ret = ret_t;
		}
	}

	/* Close our environment, if it was opened. */
	if (env_home_p != NULL) {
		ret_t = env_home_p->close(env_home_p, 0);
		if (ret_t != 0) {
			fprintf(stderr, "environment close failed: %s\n",
					db_strerror(ret_t));
			ret = ret_t;
		}
	}
	free(db_home);
	free(env_home);
	free(db_file);
	free(dict_file);
	free(stat_file);
	free(dict_db);
	free(rdict_db);
	free(data_s2po_db);
	free(data_s2pnp2o_db);
	free(stat_s2po_db);
	free(stat_s2pnp2o_db);
	if (debug == 1) printf("Done.\n");
	return ret;
}

int get_configuration(char* env_home_in, char *db_file_in, u_int32_t db_flags, u_int32_t env_flags){
	int ret;

	ret = init_config_db(env_home_in, db_file_in, db_flags, env_flags);

	if (ret == 0){
		env_home = get_config(config_db_p, "env_home");
		if (debug == 1) printf("Env_home:%s:.\n", env_home);
		db_home = get_config(config_db_p, "db_home");
		if (debug == 1) printf("db_home:%s:.\n", env_home);
		db_file = get_config(config_db_p, "db_file");
		db_type = get_config(config_db_p, "db_type");
		dict_file = get_config(config_db_p, "dict_file");
		dict_file = get_config(config_db_p, "stat_file");
		dict_db = get_config(config_db_p, "dict_db");
		rdict_db = get_config(config_db_p, "rdict_db");
		data_s2po_db = get_config(config_db_p, "data_s2po_db");
		data_s2pnp2o_db = get_config(config_db_p, "data_s2pnp2o_db");
		stat_s2po_db = get_config(config_db_p, "stat_s2po_db");
		stat_s2po_db = get_config(config_db_p, "stat_s2pnp2o_db");

		ret = init_dbs(db_flags);
	} else {
		if (debug == 1) printf("Failed to initialize environment.\n");
	}
	return ret;
}

char *get_config(DB *dbp, char*para_name){
	DBT key, value;
	int ret;
	memset(&key, 0, sizeof(DBT));
	memset(&value, 0, sizeof(DBT));
	key.data = para_name;
	key.size = sizeof(char) * (strlen(para_name) + 1);
	ret = dbp->get(dbp, 0, &key, &value, 0);
	return (char*)value.data;

}

int init_variables(char *env_home_str, char *db_home_str, char *db_file_str, char* db_type_str) {
	/* Global variables */
	int ret;
	db_type = db_type_str;
	db_home = malloc(strlen(db_home_str) + 1);
	strcpy(db_home, db_home_str);

	env_home = malloc(strlen(env_home_str) + 1);
	strcpy(env_home, env_home_str);

	db_file = malloc(strlen(db_home) + strlen(db_file_str) + 5);
	strcpy(db_file, db_home);
	strcat(db_file, "/");
	strcat(db_file, db_file_str);
	strcat(db_file, "_db");
	if (debug == 1) printf("data file: %s\n", db_file);

	dict_file = malloc(strlen(env_home) + strlen(db_file_str) + 7);
	strcpy(dict_file, env_home);
	strcat(dict_file, "/");
	strcat(dict_file, db_file_str);
	strcat(dict_file, "_dict");
	if (debug == 1) printf("dict file: %s\n", dict_file);

	stat_file = malloc(strlen(db_home) + strlen(db_file_str) + 7);
	strcpy(stat_file, db_home);
	strcat(stat_file, "/");
	strcat(stat_file, db_file_str);
	strcat(stat_file, "_stat");
	if (debug == 1) printf("stat file: %s\n", stat_file);

	rdict_db = malloc(strlen(db_file) + 7);
	strcpy(rdict_db, db_file);
	strcat(rdict_db, "_rdict");

	data_s2po_db = malloc(strlen(db_file) + 7);
	strcpy(data_s2po_db, db_file);
	strcat(data_s2po_db, "_s2po");

	data_s2pnp2o_db = malloc(strlen(db_file) + 9);
	strcpy(data_s2pnp2o_db, db_file);
	strcat(data_s2pnp2o_db, "_s2pnp2o");

	stat_s2po_db = malloc(strlen(stat_file) + 7);
	strcpy(stat_s2po_db, db_file);
	strcat(stat_s2po_db, "_s2po");

	stat_s2pnp2o_db = malloc(strlen(stat_file) + 9);
	strcpy(stat_s2pnp2o_db, stat_file);
	strcat(stat_s2pnp2o_db, "_s2pnp2o");

	config_file = malloc(strlen(env_home_str) + strlen(db_file_str) + 9);
	strcpy(config_file, env_home_str);
	strcat(config_file, "/");
	strcat(config_file, db_file_str);
	strcat(config_file, "_config");
	if (debug == 1) printf("config file: %s\n", config_file);

	config_db = malloc(strlen(db_file_str) + 8);
	strcpy(config_db, db_file_str);
	strcat(config_db, "_config");

}

int start_dbs(char *db_home, char* db_file, u_int32_t flags, u_int32_t env_flags){
	int ret = init_config_db(db_home, db_file, flags, env_flags);
	ret = init_dbs(flags);
	return ret;
}

int test_existing_config_db(char *env_home_str, char* db_file_str){
	char *config_db_str = malloc(strlen(db_file_str) + 8);
	strcpy(config_db_str, db_file_str);
	strcat(config_db_str, "_config");

	char *config_file_str = malloc(strlen(env_home_str) + strlen(db_file_str) + 9);
	strcpy(config_file_str, env_home_str);
	strcat(config_file_str, "/");
	strcat(config_file_str, db_file_str);
	strcat(config_file_str, "_config");

	int ret;
	if( exists(config_file_str) != 0 ) {
		// file exists
		env_home = env_home_str;
		config_db = config_db_str;
		config_file = config_file_str;
		ret = init_variables(env_home, env_home, db_file_str, "hash");
//		return ret;
	} else {
		// file doesn't exist
		ret = -1;
	}
	free(config_db_str);
	free(config_file_str);
	return ret;

}

int exists(const char *fname)
{
	FILE *file;
	if (file = fopen(fname, "r"))
	{
		fclose(file);
		return 1;
	}
	return 0;
}

int init_config_db(char* env_home, char* db_file, u_int32_t flags, u_int32_t env_flags){
	int ret;
	if (debug == 1) printf("config file:%s: exists\n", config_file);
	if (debug == 1) printf("config db:%s: exists\n", config_db);

	/* Create the environment */
	ret = db_env_create(&env_home_p, 0);
	if (ret != 0) {
		fprintf(stderr, "Error creating environment handle: %s\n",
				db_strerror(ret));
		goto err;
	}

	//	env_flags = DB_CREATE | /* Create the environment if it does not exist */
	//			DB_INIT_LOCK | /* Initialize the locking subsystem */
	//			DB_INIT_MPOOL | /* Initialize the memory pool (in-memory cache) */
	//			DB_PRIVATE | /* Region files are backed by heap memory.  */
	//			DB_THREAD; /* Cause the environment to be free-threaded */

	/*
	 * Specify the size of the in-memory cache.
	 */
	ret = env_home_p->set_cachesize(env_home_p, 0, 512 * 1024 * 1024, 1);
	if (ret != 0) {
		fprintf(stderr, "Error increasing the cache size: %s\n",
				db_strerror(ret));
		goto err;
	}

	/* Now actually open the environment */
	ret = env_home_p->open(env_home_p, env_home, env_flags, 0);
	if (ret != 0) {
		fprintf(stderr, "Error opening environment: %s\n", db_strerror(ret));
		goto err;
	}


	if (debug == 1) printf("Done opening environment\n");

	/* Storing global environment configuraion in a file */

	/* Initialize the DB handle */
	ret = db_create(&config_db_p, env_home_p, 0);
	if (ret != 0) {
		fprintf(stderr, "Error opening environment: %s\n", db_strerror(ret));
		goto err;
	}
	/* Now open the persistent database for the dictionary */
	ret = config_db_p->open(config_db_p, /* Pointer to the database */
			0, /* Txn pointer */
			config_file, /* prog_db_file File name */
			config_db, /* Logical db name */
			DB_HASH, /* Database type (using btree) */
			flags, /* Open flags */
			0); /* File mode. Using defaults */

	if (ret != 0) {
		fprintf(stderr, "Error opening database:%s:%s:%s:\n", config_file, config_db, db_strerror(ret));
		goto err;
	}

	if (debug == 1) printf("Done initializing the environment\n");

	return (ret);
	err:
	/* Close our database handle, if it was opened. */
	close_dbs();
	exit(0);
}

int init_dbs(u_int32_t flags) {
	/* Global variables */

	DBTYPE dbtype = get_dbtype(db_type);

	int ret, ret_t;
	u_int32_t env_flags;
	//	DB_ENV *env_tmp;
	//	ret = db_env_create(&env_tmp, 0);
	//	/* Remove the existing environment */
	//	ret = env_tmp->remove(env_tmp, db_home, DB_FORCE);

	/* Building dictionary */
	if (debug == 1) printf("Opening the databases ...\n");

	/* Initialize the DB handle */
	ret = db_create(&dict_db_p, env_home_p, 0);
	if (ret != 0) {
		fprintf(stderr, "Error creating database: %s\n", db_strerror(ret));
		goto err;
	}

	/* Setting up the hash function for the dictionary */
	dict_db_p->set_h_hash = hash;

	dict_db = malloc(strlen(db_file) + 6);
	strcpy(dict_db, db_file);
	strcat(dict_db, "_dict");

	/* Now open the persistent database for the dictionary */
	ret = dict_db_p->open(dict_db_p, /* Pointer to the database */
			0, /* Txn pointer */
			dict_file, /* prog_db_file File name */
			dict_db, /* Logical db name */
			dbtype, /* Database type (using btree) */
			flags, /* Open flags */
			0); /* File mode. Using defaults */

	if (ret != 0) {
		fprintf(stderr, "Error opening database: %s:%s\n",dict_db, db_strerror(ret));
		goto err;
	}


	/* Initialize the DB handle */
	ret = db_create(&rdict_db_p, env_home_p, 0);
	if (ret != 0) {
		fprintf(stderr, "Error opening environment: %s\n", db_strerror(ret));
		goto err;
	}
	/* Now open the persistent database for the dictionary */
	ret = rdict_db_p->open(rdict_db_p, /* Pointer to the database */
			0, /* Txn pointer */
			dict_file, /* prog_db_file File name */
			rdict_db, /* Logical db name */
			dbtype, /* Database type (using btree) */
			flags, /* Open flags */
			0); /* File mode. Using defaults */

	if (ret != 0) {
		fprintf(stderr, "Error opening database: %s:%s\n", rdict_db, db_strerror(ret));
		goto err;
	}

	/* Initialize the DB handle */
	ret = db_create(&data_db_p, env_home_p, 0);
	if (ret != 0) {
		fprintf(stderr, "Error opening environment: %s\n", db_strerror(ret));
		goto err;
	}

	/* Now open the persistent database for the dictionary */
	ret = data_db_p->open(data_db_p, /* Pointer to the database */
			0, /* Txn pointer */
			db_file, /* prog_db_file File name */
			db_file, /* Logical db name */
			dbtype, /* Database type (using btree) */
			flags, /* Open flags */
			0); /* File mode. Using defaults */

	if (ret != 0) {
		fprintf(stderr, "Error opening database: %s:%s\n",db_file, db_strerror(ret));
		goto err;
	}
	if (debug == 1) printf("Done.\n");

	/* Initialize the DB handle */
	ret = db_create(&data_s2po_db_p, env_home_p, 0);
	if (ret != 0) {
		fprintf(stderr, "Error opening environment: %s\n", db_strerror(ret));
		goto err;
	}

	data_s2po_db_p->set_flags(data_s2po_db_p, DB_DUP);

	/* Now open the persistent database for the dictionary */
	ret = data_s2po_db_p->open(data_s2po_db_p, /* Pointer to the database */
			0, /* Txn pointer */
			db_file, /* prog_db_file File name */
			data_s2po_db, /* Logical db name */
			dbtype, /* Database type (using btree) */
			flags, /* Open flags */
			0); /* File mode. Using defaults */

	if (ret != 0) {
		fprintf(stderr, "Error opening database: %s:%s\n",data_s2po_db, db_strerror(ret));
		goto err;
	}

	/* Initialize the DB handle */
	ret = db_create(&data_s2pnp2o_db_p, env_home_p, 0);
	if (ret != 0) {
		fprintf(stderr, "Error opening environment: %s\n", db_strerror(ret));
		goto err;
	}

	data_s2pnp2o_db_p->set_flags(data_s2pnp2o_db_p, DB_DUP);

	/* Now open the persistent database for the dictionary */
	ret = data_s2pnp2o_db_p->open(data_s2pnp2o_db_p, /* Pointer to the database */
			0, /* Txn pointer */
			db_file, /* prog_db_file File name */
			data_s2pnp2o_db, /* Logical db name */
			dbtype, /* Database type (using btree) */
			flags, /* Open flags */
			0); /* File mode. Using defaults */

	if (ret != 0) {
		fprintf(stderr, "Error opening database: %s:%s\n",data_s2pnp2o_db, db_strerror(ret));
		goto err;
	}


	/* Initialize the DB handle */
	ret = db_create(&stat_s2po_db_p, env_home_p, 0);
	if (ret != 0) {
		fprintf(stderr, "Error opening environment: %s\n", db_strerror(ret));
		goto err;
	}

	stat_s2po_db_p->set_flags(stat_s2po_db_p, DB_DUP);

	/* Now open the persistent database for the dictionary */
	ret = stat_s2po_db_p->open(stat_s2po_db_p, /* Pointer to the database */
			0, /* Txn pointer */
			stat_file, /* prog_db_file File name */
			stat_s2po_db, /* Logical db name */
			dbtype, /* Database type (using btree) */
			flags, /* Open flags */
			0); /* File mode. Using defaults */

	if (ret != 0) {
		fprintf(stderr, "Error opening database: %s:%s\n",stat_s2po_db, db_strerror(ret));
		goto err;
	}

	/* Initialize the DB handle */
	ret = db_create(&stat_s2pnp2o_db_p, env_home_p, 0);
	if (ret != 0) {
		fprintf(stderr, "Error opening environment: %s\n", db_strerror(ret));
		goto err;
	}

	stat_s2pnp2o_db_p->set_flags(stat_s2pnp2o_db_p, DB_DUP);

	/* Now open the persistent database for the dictionary */
	ret = stat_s2pnp2o_db_p->open(stat_s2pnp2o_db_p, /* Pointer to the database */
			0, /* Txn pointer */
			stat_file, /* prog_db_file File name */
			stat_s2pnp2o_db, /* Logical db name */
			dbtype, /* Database type (using btree) */
			flags, /* Open flags */
			0); /* File mode. Using defaults */

	if (ret != 0) {
		fprintf(stderr, "Error opening database: %s:%s\n",stat_s2pnp2o_db, db_strerror(ret));
		goto err;
	}
	if (debug == 1) printf("Done.\n");

	return (ret);

	err:
	/* Close our database handle, if it was opened. */
	close_dbs();
	exit(0);
}

int timeval_subtract(struct timeval *result, struct timeval *t2,
		struct timeval *t1) {
	long int diff = (t2->tv_usec + 1000000 * t2->tv_sec) - (t1->tv_usec + 1000000 * t1->tv_sec);
	result->tv_sec = diff / 1000000;
	result->tv_usec = diff % 1000000;

	return (diff < 0);
}

void timeval_print(struct timeval *tv) {
	char buffer[30];
	time_t curtime;

	printf("%ld.%06ld", (long int)tv->tv_sec, (long int)tv->tv_usec);
	curtime = tv->tv_sec;
	strftime(buffer, 30, "%m-%d-%Y  %T", localtime(&curtime));
	printf(" = %s.%06ld\n", buffer, (long int)tv->tv_usec);
}
