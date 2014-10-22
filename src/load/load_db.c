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
char *prog_dict_pe_db = "DICT_PE_DB";
char *prog_dict_im_db = "DICT_IM_DB";
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
char *db_type;
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

int load_threads = PTHREAD_NUM;
int tmp_dbs_enabled = 0;
char **files_p;
int num_files;
int debug;

/* Function declarations*/
u_int32_t hash(const void*, unsigned, u_int32_t);
void timeval_print(struct timeval*);
int timeval_subtract(struct timeval *result, struct timeval *t2, struct timeval *t1);
long get_db_size(DB *);
int print_db(DB *, enum TYPE ktype, enum TYPE vtype, int);
void print_dbt(void *, enum TYPE ktype);
int gen_reverse_dict_db(DB *, DB *);
int merge_dict_dbs(DB *, DB *);
int init_dbs(u_int32_t);
int start_dbs(char*, char*, u_int32_t, u_int32_t);
int init_config_db(char*, char*, u_int32_t, u_int32_t);
int set_config(DB*, char*, char*);
char* get_config(DB*, char*);
int set_configuration(DB*);
int get_configuration(char*, char*, u_int32_t, u_int32_t);
DBTYPE get_dbtype(char*);

int load_dir(char* rdf_dfile, char *, int nthreads);
int load_dict_pthread(void* thread_id);
int start_load_dict_thread(int);
int load_data_db_pthread(void* thread_id);
int start_load_data_thread(int);
int gen_stat_db(DB*, DB*);

char* lookup_id_reverse(DB*, id_type id);
id_type lookup_id(DB *, char*);


char* poid_to_string(POID *poid);
char* postr_to_string(POSTR *postr);
char* spoid_to_string(SPOID *spoid);
char* spostr_to_string(SPOSTR *spostr);
/* Function implementation */

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


int main(int argc, char *argsv[]) {

	/* Input from user*/
	char *db_file = argsv[1];
	char *rdf_datadir = argsv[2]; /* Full path to the data directory */
	char *parser_type = argsv[3];
	int load_threads = atoi(argsv[4]);
	char *env_home = argsv[5];
	char *db_home = argsv[6];
	char *db_type = argsv[7];
	debug = atoi(argsv[8]);

	if (debug == 1) printf("Threads to be created: %d\n", load_threads);

	if (debug == 1) printf("running init_dbs\n");
	init_variables(env_home, db_home, db_file, db_type);
	u_int32_t flags = DB_THREAD | DB_CREATE;
	u_int32_t env_flags = DB_CREATE | DB_INIT_LOCK | /* Initialize the locking subsystem */
			DB_INIT_MPOOL | /* Initialize the memory pool (in-memory cache) */
//			DB_SYSTEM_MEM | /* Region files are backed by heap memory.  */
			DB_THREAD; /* Cause the environment to be free-threaded */

	start_dbs(env_home, db_file, flags, env_flags);

	// begin
	struct timeval tvBegin, tvEndDict, tvEndReverse, tvDiff;
	gettimeofday(&tvBegin, NULL);
	if (debug == 1) timeval_print(&tvBegin);

	// Generate a dictionary
	if (debug == 1) printf("Starting loading ... \n");

	load_dir(rdf_datadir, parser_type, load_threads);

	if (debug == 1) printf("Done.");

	//end of generating dictionary
	gettimeofday(&tvEndDict, NULL);
	if (debug == 1) timeval_print(&tvEndDict);

	// diff
	timeval_subtract(&tvDiff, &tvEndDict, &tvBegin);
	printf("Total: %ld.%06ld sec\n", (long int)tvDiff.tv_sec,
			(long int)tvDiff.tv_usec);

	return 0;
}


/*
 * Read multiple files from the given directory, one thread per file
 */

int load_dir(char *rdf_datadir, char *parser_type, int threads) {
	if (debug == 1) printf("Start load_dir_nooverwrite.\n");
	struct timeval tvBegin, tvEnd, tvDiff;

	if (threads > 0){
		tmp_dbs_enabled = 1;
	} else {
		tmp_dbs_enabled = 0;
	}

	DIR *d;
	struct dirent *entry;
	int i = 0, err, count = 0;
	void *status;

	d = opendir(rdf_datadir);
	if (d) {
		while ((entry = readdir(d)) != NULL) {
			if (entry->d_type == DT_REG){
				if (debug == 1) printf("File %d: %s\n", count, entry->d_name);
				count++;
			}
		}
		closedir(d);
	}
	num_files = count;

	char *files[num_files];
	d = opendir(rdf_datadir);
	i = 0;
	if (d) {
		while ((entry = readdir(d)) != NULL) {
			if (entry->d_type == DT_REG){
				files[i] = malloc(strlen(entry->d_name) + strlen(rdf_datadir) + 2);
				strcpy(files[i], rdf_datadir);
				strcat(files[i], "/");
				strcat(files[i], entry->d_name);
				//				strcpy(files[i], '\0');
				i++;
			}
		}
		closedir(d);
	}

	files_p = files;

	/* Start loading the dictionary */
	printf("Starting generating the dictionary ... \n");
	start_load_dict_thread(threads);
	printf("Done generating the dictionary.\n");

	double size = get_db_size(dict_db_p);
	printf("Dictionary size: %d\n", (int) size);
	if (debug == 1) print_db(dict_db_p, CHAR_P, ID_TYPE, 100);

	printf("Starting generating the reverse dictionary ... ");
	gen_reverse_dict_db(dict_db_p, rdict_db_p);
	printf("Done generating the reverse dictionary.\n");


	size = get_db_size(rdict_db_p);
	printf("Reverse dictionary size: %d\n", (int) size);
	if (debug == 1) print_db(rdict_db_p, ID_TYPE, CHAR_P, 100);

	printf("Starting loading data ...\n");
	start_load_data_thread(threads);
	printf("Done loading data.\n");

	size = get_db_size(data_s2po_db_p);
	printf("Data db size: %d\n", (int) size);
	if (debug == 1) print_db(data_s2po_db_p, ID_TYPE, PO_ID, 100);

	printf("Starting generating statistics ...\n");
	gen_stat_db(data_s2po_db_p, stat_s2po_db_p);
	printf("Done generating stat.\n");

	size = get_db_size(stat_s2po_db_p);
	printf("Stat_s2np_db size: %d\n", (int) size);
	if (debug == 1) print_db(stat_s2po_db_p, ID_TYPE, ID_TYPE, 100);

	/* Free the file array*/
	for (i = 0; i < num_files; i++){
		free(files[i]);
	}

	set_configuration(config_db_p);
	size = get_db_size(config_db_p);
	printf("Config_db size: %d\n", (int) size);
	if (debug == 1) print_db(config_db_p, CHAR_P, CHAR_P, 100);

	if (debug == 1) printf("Done with the files.\n");
	close_dbs();
	return (0);

}

int
start_load_dict_thread(int threads){
	/*
	 * Thread configuration
	 */
	/* Initialize a mutex. Used to help provide thread ids. */
	(void)mutex_init(&read_dict_db_lock, NULL);
	(void)mutex_init(&merge_nodes_lock, NULL);
	pthread_attr_t attr;
	/* Initialize and set thread detached attribute */
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	if (debug == 1) printf("Start creating threads \n");

	pthread_t threadids[threads];
	int err, i = 0;
	void *status;
	while (i < threads) {
		if (debug == 1) printf("Creating thread %d\n", i);
		err = pthread_create(&(threadids[i]), &attr, &load_dict_pthread, (void *)i);
		if (err != 0)
			printf("\ncan't create thread %d:[%s]", i, strerror(err));
		else
			if (debug == 1) printf("Creating thread %d successfully\n", i);
		i++;
	}

	pthread_attr_destroy(&attr);
	if (debug == 1) printf("Start joining threads \n");
	for (i = 0; i < threads; i++) {
		err = pthread_join(threadids[i], &status);
		if (err) {
			printf("ERROR; return code from pthread_join() is %d\n", err);
			exit(-1);
		}
	}
	if (debug == 1) printf("End threads.\n");
	pthread_mutex_destroy(&read_dict_db_lock);
	pthread_mutex_destroy(&merge_nodes_lock);

}

int
start_load_data_thread(int threads){
	/*
	 * Thread configuration
	 */
	/* Initialize a mutex. Used to help provide thread ids. */
	(void)mutex_init(&read_data_db_lock, NULL);
	(void)mutex_init(&read_data_s2po_db_lock, NULL);
	(void)mutex_init(&merge_nodes_lock, NULL);
	pthread_attr_t attr;
	/* Initialize and set thread detached attribute */
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	if (debug == 1) printf("Start creating threads \n");

	pthread_t threadids[threads];
	int err, i = 0;
	void *status;
	while (i < threads) {
		if (debug == 1) printf("Creating thread %d\n", i);
		err = pthread_create(&(threadids[i]), &attr, &load_data_db_pthread, (void *)i);
		if (err != 0)
			printf("\ncan't create thread %d:[%s]", i, strerror(err));
		else
			if (debug == 1) printf("Creating thread %d successfully\n", i);
		i++;
	}

	pthread_attr_destroy(&attr);
	if (debug == 1) printf("Start joining threads \n");
	for (i = 0; i < threads; i++) {
		err = pthread_join(threadids[i], &status);
		if (err) {
			printf("ERROR; return code from pthread_join() is %d\n", err);
			exit(-1);
		}
	}
	if (debug == 1) printf("End threads.\n");
	pthread_mutex_destroy(&read_data_db_lock);
	pthread_mutex_destroy(&read_data_s2po_db_lock);
	pthread_mutex_destroy(&merge_nodes_lock);

}

/*
 * Generating dictionary db from a single file
 */


char*
extract_term(char *buf, long start, long end) {

	long cur_size = end - start + 2;
	char *term = (char *) malloc(cur_size * sizeof(char));
	int k = start;
	int j = 0;
	for (j = 0; j < cur_size - 1; j++) {
		term[j] = buf[k];
		k++;
	}
	term[cur_size - 1] = '\0';
	return term;
}


int load_dict_pthread(void* thread_id) {
	int tid = (long)thread_id;
	if (tid > num_files){
		if (debug == 1) printf("Thread %d: no more file to process. Done.\n", tid);
		return 0;
	}
	int file;
	struct timeval tvBegin, tvEnd, tvDiff;
	if (debug == 1) printf("Thread %d with ID %ld started\n", tid, (long)pthread_self());
	DBT key, value, vtmp;
	int ret, ret_t;

	DB *tmp_db;
	id_type node_id = tid * 10000000000 + 2;
	id_type node_id_odd = node_id + 1;
	if (debug == 1) printf("URI's ID starts at ");
	if (debug == 1) printf(id_type_format, node_id);
	if (debug == 1) printf("\t Literal's ID starts at ");
	if (debug == 1) printf(id_type_format, node_id_odd);
	if (debug == 1) printf("\n");

	char *db_name = malloc(sizeof(tid) + 8 * sizeof(char));


	/* Init a temporary database*/
	/* Initialize the DB handle */
	if (tmp_dbs_enabled == 1){

		char strtid[4];
		sprintf(strtid, "%d", tid);
		strcpy(db_name, "tmp_");
		strcat(db_name, strtid);
		strcat(db_name, "_db");
		if (debug == 1) printf("Thread %d: will Creating temporary db %s\n", tid, db_name);

		ret = db_create(&tmp_db, env_home_p, 0);
		if (ret != 0) {
			fprintf(stderr, "Error creating database: %s\n", db_strerror(ret));
			goto err;
		}
		tmp_db->set_h_hash = hash;
		tmp_db->set_priority(tmp_db, DB_PRIORITY_LOW);

		DBTYPE dbtype = get_dbtype(db_type);
		if (debug == 1) printf("Thread %d: Start Creating temporary db with  %s %s\n", tid, db_name, db_type);
		/* Now open the persistent database for the dictionary */
		ret = tmp_db->open(tmp_db, /* Pointer to the database */
				0, /* Txn pointer */
				0, /* prog_db_file File name */
				db_name, /* Logical db name */
				dbtype, /* Database type (using btree) */
				DB_CREATE | DB_THREAD, /* Open flags */
				0); /* File mode. Using defaults */

		if (ret != 0) {
			fprintf(stderr, "Error opening database: %s\n", db_strerror(ret));
			goto err;
		}
	} else {
		tmp_db = dict_db_p;
	}

	memset(&key, 0, sizeof(DBT));
	key.flags = DB_DBT_MALLOC;

	memset(&value, 0, sizeof(DBT));
	value.size = sizeof(id_type);

	memset(&vtmp, 0, sizeof(DBT));
	vtmp.size = sizeof(id_type);
	vtmp.flags = DB_DBT_MALLOC;

	FILE *ifp;
	size_t linesize = 10000;
	char *linebuff = NULL;
	ssize_t linelen = 0;
	char *ptr = NULL;
	char *res_p[3];

	long start_term_idx = 0, end_term_idx = 0, last_non_blank = 0, fi = 0;
	int i = 0, spo = 0, line = 0, term_started = 0, found = 0, literal = 0, bnode = 0;
	size_t cur_size;
	long id;
	char *rdf_dfile;

	for (file = tid; file < num_files; file = file + load_threads){
		gettimeofday(&tvBegin, NULL);
		rdf_dfile = files_p[file];
		/*
		 * Read the content from the rdf_file and put RDF resources into the dictdb.
		 */
		/*
		 * Read the content from the rdf_file and put RDF resources into the dictdb.
		 */
		/* Load the vendors database */
		ifp = fopen(rdf_dfile, "r");
		if (ifp == NULL) {
			fprintf(stderr, "Error opening file '%s'\n", rdf_dfile);
			return (-1);
		}

		/*
		 * Create a cursor for the dictionary database
		 */
		if (debug == 1) printf("Start loading file %s ...\n", rdf_dfile);

		/* Iterate over the input RDF file*/
		while ((linelen = getline(&linebuff, &linesize, ifp)) > 0) {
			if (linebuff[0] == '#') {
				// Skip the comment line
			} else if (linelen > 2){
//				if (debug == 1) printf("Start processing line %s\n", linebuff);
				//			if (debug == 1) printf("Start processing line %d of len %d and of size %d...\n", line, linelen, linesize);
				//				if (debug == 1) printf("Line: %d\n", line);
				for (i = 0; i < linelen; i++) {
					/*
					 * Identify the term from start_term to end_term
					 * If the cur pointer is within subject, predicate, or object
					 */
					if (isblank(linebuff[i])){
						//						if (debug == 1) printf("Thread %d: char %c at %d is blank \n", tid, linebuff[i], i);
						/*
						 * Reach blank char. Multiple possibilities:
						 * 1. If the term has been started:
						 * 		1.1 If the term is subject/predicate:
						 *			The term is ended at p[i-1]. => Found the term.
						 * 2. If the term has not been started, do nothing.
						 */
						if (term_started == 1){
							if ((spo != 2) || ((spo == 2) && (literal == 0 || literal == 2 || bnode == 1))){
								end_term_idx = i - 1;
//								if (debug == 1) printf("Thread %d: from %d to %d with spo %d literal %d\n", tid, (long)start_term_idx, (long)end_term_idx, spo, literal);

								/* Extract the found term */
								res_p[spo] = extract_term(linebuff, start_term_idx, end_term_idx);

//								if (debug == 1) printf("Thread %d: Found spo %d\t %s \n", tid, spo, res_p[spo]);

								/* Reset the variables for the next term*/
								spo++;
								start_term_idx = 0;
								end_term_idx = 0;
								literal = 0;
								term_started = 0;
								bnode = 0;
							}
						}
					} else {
						/* Start a new term if p[i] is the first non-blank char*/
						if (term_started == 0 && linebuff[i] != '.'){
							//							if (debug == 1) printf("spo %d start at %d with %c\n", spo, (int)i, linebuff[i]);
							term_started = 1;
							start_term_idx = i;
							// Identify the literal for object
						}
						if (linebuff[i] == '"' && linebuff[i-1] != '\\'){
							literal++;
						}
						if (linebuff[i] == ':' && linebuff[i-1] == '_'){
							bnode = 1;
							term_started = 1;
							start_term_idx = i - 1;
						}
					}
				} // Finish identifying the resources in the line

//				if (debug == 1) printf("Thread %d: %s\t%s\t%s\t\n", tid, res_p[0], res_p[1], res_p[2]);

				// Processing the dictionary and indexes

				for (i = 0; i < 3; i++) {
					memset(&key, 0, sizeof(DBT));
					memset(&value, 0, sizeof(DBT));
					//					memset(&vtmp, 0, sizeof(DBT));
					//					if (debug == 1) printf("one\n");

					key.data = res_p[i];
					key.size = sizeof(char) * (strlen(res_p[i]) + 1);
					key.flags = DB_DBT_MALLOC;
					//					if (debug == 1) printf("two\n");

					if (strchr(res_p[i], '"') != NULL){
						value.data = &node_id_odd;
					} else {
						value.data = &node_id;
					}

					value.size = sizeof(id_type);
					//					if (debug == 1) printf("three\n");

					vtmp.size = sizeof(id_type);
					//					if (debug == 1) printf("four\n");
					vtmp.flags = DB_DBT_MALLOC;

//					if (debug == 1) printf("Thread %d: inserting : key ", tid);
//					if (debug == 1) print_dbt((void*)key.data, CHAR_P);
//					if (debug == 1) printf("\t and value: ");
//					if (debug == 1) print_dbt((void*)value.data, ID_TYPE);
//					if (debug == 1) printf("\n");

				    if (tmp_dbs_enabled == 0){
				    	(void)mutex_lock(&thread_node_id_lock);
				    }
					ret = tmp_db->get(tmp_db, 0, &key, &vtmp, 0);

					if (ret == DB_NOTFOUND){
						ret = tmp_db->put(tmp_db, 0, &key, &value, 0);
						if (strchr(res_p[i], '"') != NULL){
							node_id_odd += 2;
						} else {
							node_id += 2;
						}
						//						if (debug == 1) printf("Thread %d: node_id %lld\n", tid, *(id_type*)value.data);
						if (ret != 0){
							printf("insert error for key:");
							print_dbt((void*)key.data, CHAR_P);
							printf("\t and value: ");
							print_dbt((void*)value.data, ID_TYPE);
							printf("\n");
							close_dbs();
							return (ret);
						}
					} else {
						free(vtmp.data);
					}
					//				if (debug == 1) printf("finished inserting %s\t%d\n", key.data, *(long *)value.data);
					if (tmp_dbs_enabled == 0){
						(void)mutex_unlock(&thread_node_id_lock);
					}

					free(res_p[i]);
					res_p[i] = NULL;
					//					free(key.data);
					//					free(value.data);
				}

				// Update the counters
				spo = 0;
				line++;
				start_term_idx = 0;
				end_term_idx = 0;
				literal = 0;
				term_started = 0;
				bnode = 0;
			}
			free(linebuff);
			linebuff = NULL;
		}

		/* Close the file */
		fclose(ifp);
		gettimeofday(&tvEnd, NULL);
		if (debug == 1) timeval_print(&tvEnd);
		timeval_subtract(&tvDiff, &tvEnd, &tvBegin);
		if (debug == 1) printf("Processed %s: %ld.%06ld sec.\n", rdf_dfile, (long int)tvDiff.tv_sec, (long int)tvDiff.tv_usec);
	}
	if (tmp_dbs_enabled == 1){
		if (debug == 1) printf("Thread %d: Printing temporary dictionary db.\n", tid);
		if (debug == 1) print_db(tmp_db, CHAR_P, ID_TYPE, 10);

		/* Putting the content of this db to the main dict db*/
		(void)mutex_lock(&merge_nodes_lock);
		if (debug == 1) printf("Thread %d: get the lock\n", tid);
		merge_dict_dbs(tmp_db, dict_db_p);
		(void)mutex_unlock(&merge_nodes_lock);
		if (debug == 1) printf("Thread %d: released the lock\n", tid);

		if (tmp_db != NULL) {
			if (debug == 1) printf("Thread %d: closing the temp db.\n", tid);
			ret_t = tmp_db->close(tmp_db, 0);
			if (ret_t != 0) {
				fprintf(stderr, "%s database close failed.\n", db_strerror(ret_t));
				ret = ret_t;
				goto err;
			}
			if (debug == 1) printf("Thread %d: removing the temp db.\n", tid);
			u_int32_t tmp;
			ret_t = env_home_p->dbremove(env_home_p, 0, 0, db_name, 0);
			if (ret_t != 0) {
				fprintf(stderr, "%s database truncate failed.\n", db_strerror(ret_t));
				ret = ret_t;
				goto err;
			}
		}
		free(db_name);

	}
	if (debug == 1) printf("Thread %d: Done processing.\n", tid);
	return 0;
	err:
	/* Close our database handle, if it was opened. */
	close_dbs();
	exit(0);
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

int gen_stat_db(DB* dbin, DB *dbout){
	/* Setting up the BerkeleyDB parameters for bulk insert */
	DBT key, data, ktmp, vtmp, k, v;
	int ret, ret_t;
	void *ptrk, *ptrd;
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	u_int32_t flag;
	flag = DB_MULTIPLE_KEY;

	key.ulen = 512 * 1024 * 1024;
	key.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	key.data = malloc(key.ulen);
	memset(key.data, 0, key.ulen);

	data.ulen = 512 * 1024 * 1024;
	data.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	data.data = malloc(data.ulen);
	memset(data.data, 0, data.ulen);

	DB_MULTIPLE_WRITE_INIT(ptrk, &key);

	DBC *cursorp;
	int num;

	cursorp = NULL;
	num = 0;

	/* Get the cursor */
	ret = dbin->cursor(dbin, 0, &cursorp, 0);
	if (ret != 0) {
		dbin->err(dbin, ret, "generate stat_s2po_db: cursor open failed.");
		goto err;
	}

	/* Get the key DBT used for the database read */
	//	if (debug == 1) printf("inserting reverse item");

	id_type cur_key, next_key;
	memset(&ktmp, 0, sizeof(DBT));
	memset(&vtmp, 0, sizeof(DBT));
	ret = cursorp->c_get(cursorp, &ktmp, &vtmp, DB_NEXT);
	cur_key = *(id_type*) ktmp.data;
	next_key = *(id_type*) ktmp.data;
	id_type count = 1;
	do {
		memset(&ktmp, 0, sizeof(DBT));
		memset(&vtmp, 0, sizeof(DBT));
		ret = cursorp->c_get(cursorp, &ktmp, &vtmp, DB_NEXT);
		next_key = *(id_type*) ktmp.data;
		switch (ret) {
		case 0:
//			if (debug == 1) printf("Cur key is %lld : next key is : %lld \n", cur_key, next_key);
			if (cur_key != next_key){

				memset(&k, 0, sizeof(DBT));
				k.flags = DB_DBT_MALLOC;
				k.size = sizeof(id_type);
				k.data = &cur_key;

				memset(&v, 0, sizeof(DBT));
				v.flags = DB_DBT_MALLOC;
				v.size = sizeof(id_type);
				v.data = &count;

				DB_MULTIPLE_KEY_WRITE_NEXT(ptrk, &key, k.data, k.size, v.data, v.size);
				assert(ptrk != NULL);
//				if (debug == 1) printf("Inserting : key ");
//				print_dbt((void*)k.data, ID_TYPE);
//				if (debug == 1) printf("\t and value: ");
//				print_dbt((void*)v.data, ID_TYPE);
//				if (debug == 1) printf("\n");

				if ((num + 1) % UPDATES_PER_BULK_PUT == 0) {
					//					if (debug == 1) printf("flush buffer to file\n");
					switch (ret = dbout->put(dbout, 0, &key, &data, flag)) {
					case 0:
						DB_MULTIPLE_WRITE_INIT(ptrk, &key);
						break;
					default:
						dbout->err(dbout, ret, "Bulk DB->put");
						goto err;
					}
				}
				num = num + 1;
				count = 1;
				cur_key = next_key;
			} else {
				count++;
			}
			break;
		case DB_NOTFOUND:
//			if (debug == 1) printf("not found\n");
			memset(&k, 0, sizeof(DBT));
			k.flags = DB_DBT_MALLOC;
			k.size = sizeof(id_type);
			k.data = &cur_key;

			memset(&v, 0, sizeof(DBT));
			v.flags = DB_DBT_MALLOC;
			v.size = sizeof(id_type);
			v.data = &count;

			DB_MULTIPLE_KEY_WRITE_NEXT(ptrk, &key, k.data, k.size, v.data, v.size);
			assert(ptrk != NULL);
			if (debug == 1) printf("Inserting last : key ");
			if (debug == 1) print_dbt((void*)k.data, ID_TYPE);
			if (debug == 1) printf("\t and value: ");
			if (debug == 1) print_dbt((void*)v.data, ID_TYPE);
			if (debug == 1) printf("\n");

			break;
		default:
			dbin->err(dbin, ret, "Count records unspecified error");
			goto err;
		}
	} while (ret == 0);

	if (num % UPDATES_PER_BULK_PUT != 0) {
		if (debug == 1) printf("Committed last buffer of data.\n");
		switch (ret = dbout->put(dbout, 0, &key, &data, flag)) {
		case 0:
//			DB_MULTIPLE_WRITE_INIT(ptrk, &key);
			break;
		default:
			printf("error\n");
			dbout->err(dbout, ret, "Bulk DB->put");
			goto err;
		}
	}
//	if (debug == 1) printf("freeing gen_stat_db\n");
	free(key.data);
	free(data.data);
//	if (debug == 1) printf("finish gen_stat_db\n");
	return (0);
	err: if (cursorp != NULL) {
		ret = cursorp->close(cursorp);
		if (ret != 0) {
			dbin->err(dbin, ret, "count_records: cursor close failed.");
		}
	}
	close_dbs();
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

int load_data_db_pthread(void* thread_id) {
	int tid = (long)thread_id;
	if (tid > num_files){
		if (debug == 1) printf("Thread %d: no more file to process. Done.\n", tid);
		return 0;
	}
	int file;
	struct timeval tvBegin, tvEnd, tvDiff;
	if (debug == 1) printf("Thread %d with ID %ld started\n", tid, (long)pthread_self());
	DBT key, value, vtmp;
	int ret, ret_t;

	DB *tmp_db;
	id_type node_id = tid * 10000000000 + 2;
	id_type node_id_odd = node_id + 1;
	if (debug == 1) printf("URI's ID starts at ");
	if (debug == 1) printf(id_type_format, node_id);
	if (debug == 1) printf("\t Literal's ID starts at ");
	if (debug == 1) printf(id_type_format, node_id_odd);
	if (debug == 1) printf("\n");

	char *db_name;

	/* Init a temporary database*/
	/* Initialize the DB handle */
	if (tmp_dbs_enabled == 1){

		char strtid[4];
		sprintf(strtid, "%d", tid);
		db_name = malloc(strlen(strtid) + 12 * sizeof(char));
		strcpy(db_name, "tmp_data");
		strcat(db_name, strtid);
		strcat(db_name, "_db");
		if (debug == 1) printf("Thread %d: Creating temporary db %s\n", tid, db_name);

		ret = db_create(&tmp_db, env_home_p, 0);
		if (ret != 0) {
			fprintf(stderr, "Error creating database: %s\n", db_strerror(ret));
			goto err;
		}
		tmp_db->set_h_hash = hash;
		tmp_db->set_priority(tmp_db, DB_PRIORITY_LOW);
		tmp_db->set_flags(tmp_db, DB_DUP);
		/* Now open the persistent database for the dictionary */
		DBTYPE dbtype = get_dbtype(db_type);
		ret = tmp_db->open(tmp_db, /* Pointer to the database */
				0, /* Txn pointer */
				0, /* prog_db_file File name */
				db_name, /* Logical db name */
				dbtype, /* Database type (using btree) */
				DB_CREATE | DB_THREAD, /* Open flags */
				0); /* File mode. Using defaults */

		if (ret != 0) {
			fprintf(stderr, "Error opening database: %s\n", db_strerror(ret));
			goto err;
		}
	} else {
		tmp_db = data_s2po_db_p;
	}

	memset(&key, 0, sizeof(DBT));
	key.flags = DB_DBT_MALLOC;

	memset(&value, 0, sizeof(DBT));
	value.size = sizeof(id_type);

	memset(&vtmp, 0, sizeof(DBT));
	vtmp.size = sizeof(id_type);
	vtmp.flags = DB_DBT_MALLOC;

	FILE *ifp;
	size_t linesize = 10000;
	char *linebuff = NULL;
	ssize_t linelen = 0;
	char *ptr = NULL;
	char *res_p[3];

	long start_term_idx = 0, end_term_idx = 0, last_non_blank = 0, fi = 0;
	int i = 0, spo = 0, line = 0, term_started = 0, found = 0, literal = 0, bnode = 0;
	size_t cur_size;
	long id;
	char *rdf_dfile;

	for (file = tid; file < num_files; file = file + load_threads){
		gettimeofday(&tvBegin, NULL);
		if (debug == 1) timeval_print(&tvBegin);
		rdf_dfile = files_p[file];
		/*
		 * Read the content from the rdf_file and put RDF resources into the dictdb.
		 */
		/*
		 * Read the content from the rdf_file and put RDF resources into the dictdb.
		 */
		/* Load the vendors database */
		ifp = fopen(rdf_dfile, "r");
		if (ifp == NULL) {
			fprintf(stderr, "Error opening file '%s'\n", rdf_dfile);
			return (-1);
		}

		/*
		 * Create a cursor for the dictionary database
		 */
		if (debug == 1) printf("Start loading file %s ...\n", rdf_dfile);

		/* Iterate over the input RDF file*/
		while ((linelen = getline(&linebuff, &linesize, ifp)) > 0) {
			if (linebuff[0] == '#') {
				// Skip the comment line
			} else if (linelen > 2){
//				if (debug == 1) printf("Start processing line %s\n", linebuff);
				//			if (debug == 1) printf("Start processing line %d of len %d and of size %d...\n", line, linelen, linesize);
				//				if (debug == 1) printf("Line: %d\n", line);
				for (i = 0; i < linelen; i++) {
					/*
					 * Identify the term from start_term to end_term
					 * If the cur pointer is within subject, predicate, or object
					 */
					if (isblank(linebuff[i])){
						//						if (debug == 1) printf("Thread %d: char %c at %d is blank \n", tid, linebuff[i], i);
						/*
						 * Reach blank char. Multiple possibilities:
						 * 1. If the term has been started:
						 * 		1.1 If the term is subject/predicate:
						 *			The term is ended at p[i-1]. => Found the term.
						 * 2. If the term has not been started, do nothing.
						 */
						if (term_started == 1){
							if ((spo != 2) || ((spo == 2) && (literal == 0 || literal == 2 || bnode == 1))){
								end_term_idx = i - 1;
//								if (debug == 1) printf("Thread %d: from %d to %d with spo %d literal %d\n", tid, (long)start_term_idx, (long)end_term_idx, spo, literal);

								/* Extract the found term */
								res_p[spo] = extract_term(linebuff, start_term_idx, end_term_idx);

//								if (debug == 1) printf("Thread %d: Found spo %d\t %s \n", tid, spo, res_p[spo]);

								/* Reset the variables for the next term*/
								spo++;
								start_term_idx = 0;
								end_term_idx = 0;
								literal = 0;
								term_started = 0;
								bnode = 0;
							}
						}
					} else {
						/* Start a new term if p[i] is the first non-blank char*/
						if (term_started == 0 && linebuff[i] != '.'){
							//							if (debug == 1) printf("spo %d start at %d with %c\n", spo, (int)i, linebuff[i]);
							term_started = 1;
							start_term_idx = i;
							// Identify the literal for object
						}
						if (linebuff[i] == '"' && linebuff[i-1] != '\\'){
							literal++;
						}
						if (linebuff[i] == ':' && linebuff[i-1] == '_'){
							bnode = 1;
							term_started = 1;
							start_term_idx = i - 1;
						}
					}
				} // Finish identifying the resources in the line

//				if (debug == 1) printf("Thread %d: %s\t%s\t%s\t\n", tid, res_p[0], res_p[1], res_p[2]);

				// Loading the data
				// Getting all three resource identifier
				id_type subject = lookup_id(dict_db_p, res_p[0]);
				id_type predicate = lookup_id(dict_db_p, res_p[1]);
				id_type object = lookup_id(dict_db_p, res_p[2]);

				POID *po = malloc(sizeof(POID));
				po->predicate = predicate;
				po->object = object;

				memset(&key, 0, sizeof(DBT));
				memset(&value, 0, sizeof(DBT));
				//					memset(&vtmp, 0, sizeof(DBT));
				//					if (debug == 1) printf("one\n");

				key.data = &subject;
				key.size = sizeof(id_type);
				value.data = po;
				value.size = sizeof(POID);

//					if (debug == 1) printf("Putting key: ");
//					if (debug == 1) print_dbt((void*)key.data, ID_TYPE);
//					if (debug == 1) printf("\t and value: ");
//					if (debug == 1) print_dbt((void*)value.data, PO_ID);
//					if (debug == 1) printf("\n");

				if (tmp_dbs_enabled == 0){
					(void)mutex_unlock(&thread_node_id_lock);
				}

				ret = tmp_db->put(tmp_db, 0, &key, &value, 0);
				if (ret != 0){
					printf("insert error for key:");
					print_dbt((void*)key.data, ID_TYPE);
					printf("\t and value: ");
					print_dbt((void*)value.data, PO_ID);
					printf("\n");

					close_dbs();
					return (ret);
				}

				if (tmp_dbs_enabled == 0){
					(void)mutex_unlock(&thread_node_id_lock);
				}

				for (i = 0; i < 3; i++){
					free(res_p[i]);
				}
				free(po);

				// Update the counters
				spo = 0;
				line++;
				start_term_idx = 0;
				end_term_idx = 0;
				literal = 0;
				term_started = 0;
				bnode = 0;
			}
			free(linebuff);
			linebuff = NULL;
		}

		/* Close the file */
		fclose(ifp);
		gettimeofday(&tvEnd, NULL);
		if (debug == 1) timeval_print(&tvEnd);
		timeval_subtract(&tvDiff, &tvEnd, &tvBegin);
		if (debug == 1) printf("Processed %s: %ld.%06ld sec.\n", rdf_dfile, (long int)tvDiff.tv_sec, (long int)tvDiff.tv_usec);
	}
	if (tmp_dbs_enabled == 1){
		if (debug == 1) printf("Thread %d: Printing temporary data db.\n", tid);
		if (debug == 1) print_db(tmp_db, ID_TYPE, PO_ID,10);

		/* Putting the content of this db to the main dict db*/
		(void)mutex_lock(&merge_nodes_lock);
		if (debug == 1) printf("Thread %d: get the lock\n", tid);
		merge_dict_dbs(tmp_db, data_s2po_db_p);
		(void)mutex_unlock(&merge_nodes_lock);
		if (debug == 1) printf("Thread %d: released the lock\n", tid);

		if (tmp_db != NULL) {
			if (debug == 1) printf("Thread %d: closing the temp db.\n", tid);
			ret_t = tmp_db->close(tmp_db, 0);
			if (ret_t != 0) {
				fprintf(stderr, "%s database close failed.\n", db_strerror(ret_t));
				ret = ret_t;
				goto err;
			}
			if (debug == 1) printf("Thread %d: removing the temp db.\n", tid);
			u_int32_t tmp;
			ret_t = env_home_p->dbremove(env_home_p, 0, 0, db_name, 0);
			if (ret_t != 0) {
				fprintf(stderr, "%s database truncate failed.\n", db_strerror(ret_t));
				ret = ret_t;
				goto err;
			}
		}
		free(db_name);

	}
	if (debug == 1) printf("Thread %d: Done processing.\n", tid);
	return 0;
	err:
	/* Close our database handle, if it was opened. */
	close_dbs();
	exit(0);
}

/*
 * Generating the reverse dictionary
 */
int gen_reverse_dict_db(DB *dbp, DB *dbrp) {
	/* Setting up the BerkeleyDB parameters for bulk insert */
	DBT key, data, ktmp, vtmp;
	int ret, ret_t;
	void *ptrk, *ptrd;
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	u_int32_t flag;
	flag = DB_MULTIPLE_KEY;

	key.ulen = 512 * 1024 * 1024;
	key.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	key.data = malloc(key.ulen);
	memset(key.data, 0, key.ulen);

	data.ulen = 512 * 1024 * 1024;
	data.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	data.data = malloc(data.ulen);
	memset(data.data, 0, data.ulen);

	DB_MULTIPLE_WRITE_INIT(ptrk, &key);
	long count = 0;

	DBC *cursorp;
	int num;

	cursorp = NULL;
	num = 0;

	/* Get the cursor */
	ret = dbp->cursor(dbp, 0, &cursorp, 0);
	if (ret != 0) {
		dbp->err(dbp, ret, "count_records: cursor open failed.");
		goto err;
	}

	/* Get the key DBT used for the database read */
	//	if (debug == 1) printf("inserting reverse item");
	char *val;
	do {
		memset(&ktmp, 0, sizeof(DBT));
		memset(&vtmp, 0, sizeof(DBT));
		ret = cursorp->get(cursorp, &ktmp, &vtmp, DB_NEXT);
		switch (ret) {
		case 0:
			ktmp.flags = DB_DBT_MALLOC;
			vtmp.flags = DB_DBT_MALLOC;

			DB_MULTIPLE_KEY_WRITE_NEXT(ptrk, &key, vtmp.data, vtmp.size, ktmp.data, ktmp.size);
			assert(ptrk != NULL);
			//				if (debug == 1) printf("Insert key: %s, \t data: %lld\n", res_p[i], cur_node_id);

			if ((num + 1) % UPDATES_PER_BULK_PUT == 0) {
				//					if (debug == 1) printf("flush buffer to file\n");
				switch (ret = dbrp->put(dbrp, 0, &key, &data, flag)) {
				case 0:
					DB_MULTIPLE_WRITE_INIT(ptrk, &key);
					break;
				default:
					dbrp->err(dbrp, ret, "Bulk DB->put");
					goto err;
				}
			}
			num = num + 1;
			break;
		case DB_NOTFOUND:
			break;
		default:
			dbp->err(dbp, ret, "Count records unspecified error");
			goto err;
		}
	} while (ret == 0);

	if ((num % UPDATES_PER_BULK_PUT) != 0) {
		if (debug == 1) printf("Committed last buffer of data.");
		switch (ret = dbrp->put(dbrp, 0, &key, &data, flag)) {
		case 0:
			break;
		default:
			dbrp->err(dbrp, ret, "Bulk DB->put");
			goto err;
		}
	}
	free(key.data);
	free(data.data);
	return (count);
	err: if (cursorp != NULL) {
		ret = cursorp->close(cursorp);
		if (ret != 0) {
			dbp->err(dbp, ret, "count_records: cursor close failed.");
		}
	}
	close_dbs();

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


int merge_dict_dbs(DB *tmp_db, DB *dict_db) {
	/* Setting up the BerkeleyDB parameters for bulk insert */
	DBT key, data, ktmp, vtmp;
	int ret, ret_t;
	void *ptrk, *ptrd;
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	u_int32_t flag;
	flag = DB_MULTIPLE_KEY;

	key.ulen = 512 * 1024 * 1024;
	key.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	key.data = malloc(key.ulen);
	memset(key.data, 0, key.ulen);

	data.ulen = 512 * 1024 * 1024;
	data.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	data.data = malloc(data.ulen);
	memset(data.data, 0, data.ulen);

	DB_MULTIPLE_WRITE_INIT(ptrk, &key);
	long count = 0;

	DBC *cursorp;
	int num;

	cursorp = NULL;
	num = 0;

	/* Get the cursor */
	ret = tmp_db->cursor(tmp_db, 0, &cursorp, 0);
	if (ret != 0) {
		tmp_db->err(tmp_db, ret, "count_records: cursor open failed.");
		goto err;
	}

	/* Get the key DBT used for the database read */
	//	if (debug == 1) printf("inserting reverse item");
	long id;
	char *val;
	do {
		memset(&ktmp, 0, sizeof(DBT));
		memset(&vtmp, 0, sizeof(DBT));
		ret = cursorp->get(cursorp, &ktmp, &vtmp, DB_NEXT);
		switch (ret) {
		case 0:
			ktmp.flags = DB_DBT_MALLOC;
			vtmp.flags = DB_DBT_MALLOC;
//			vtmp.data = &cur_node_id;

			DB_MULTIPLE_KEY_WRITE_NEXT(ptrk, &key, ktmp.data, ktmp.size, vtmp.data, vtmp.size);
			assert(ptrk != NULL);
//			cur_node_id = cur_node_id + 1;
//							if (debug == 1) printf("Insert key: %s, \t data: %lld\n", (char*)ktmp.data, *(id_type *)vtmp.data);

			if ((num + 1) % UPDATES_PER_BULK_PUT == 0) {
				//					if (debug == 1) printf("flush buffer to file\n");
				switch (ret = dict_db->put(dict_db, 0, &key, &data, flag)) {
				case 0:
					DB_MULTIPLE_WRITE_INIT(ptrk, &key);
					break;
				default:
					dict_db->err(dict_db, ret, "Bulk DB->put");
					goto err;
				}
			}
			num = num + 1;
			break;
		case DB_NOTFOUND:
			break;
		default:
			dict_db->err(dict_db, ret, "Count records unspecified error");
			goto err;
		}
	} while (ret == 0);

	if ((num % UPDATES_PER_BULK_PUT) != 0) {
		if (debug == 1) printf("Committed last buffer of data.\n");
		switch (ret = dict_db->put(dict_db, 0, &key, &data, flag)) {
		case 0:
			break;
		default:
			dict_db->err(dict_db, ret, "Bulk DB->put");
			goto err;
		}
	}
	free(key.data);
	free(data.data);
	return (count);

	err: if (cursorp != NULL) {
		ret = cursorp->close(cursorp);
		if (ret != 0) {
			dict_db->err(dict_db, ret, "count_records: cursor close failed.");
		}
	}
	close_dbs();
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

int set_config(DB *dbp, char *para_name, char *para_value){
	DBT key, value;
	int ret;
	memset(&key, 0, sizeof(DBT));
	memset(&value, 0, sizeof(DBT));
	key.data = para_name;
	key.size = sizeof(char) * (strlen(para_name) + 1);
	value.data = para_value;
	value.size = sizeof(char) * (strlen(para_value) + 1);
	ret = dbp->put(dbp, 0, &key, &value, 0);
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

int set_configuration(DB *config_db_p){
	int ret;
	ret = set_config(config_db_p, "db_home", db_home);
	ret = set_config(config_db_p, "env_home", env_home);
	ret = set_config(config_db_p, "db_file", db_file);
	ret = set_config(config_db_p, "db_type", db_type);
	ret = set_config(config_db_p, "dict_file", dict_file);
	ret = set_config(config_db_p, "stat_file", stat_file);
	ret = set_config(config_db_p, "dict_db", dict_db);
	ret = set_config(config_db_p, "rdict_db", rdict_db);
	ret = set_config(config_db_p, "data_s2po_db", data_s2po_db);
	ret = set_config(config_db_p, "data_s2pnp2o_db", data_s2pnp2o_db);
	ret = set_config(config_db_p, "stat_s2po_db", stat_s2po_db);
	ret = set_config(config_db_p, "stat_s2pnp2o_db", stat_s2pnp2o_db);

}

int get_configuration(char* db_home, char *db_file, u_int32_t flags, u_int32_t env_flags){
	int ret;

	init_config_db(db_home, db_file, flags, env_flags);

	db_home = get_config(config_db_p, "db_home");
	env_home = get_config(config_db_p, "env_home");
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

	init_dbs(flags);

}


DBTYPE get_dbtype(char *db_type){
	if (strcmp(db_type, "btree") == 0){
		return DB_BTREE;
	} else {
		return DB_HASH;
	}
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


int start_dbs(char *db_home_str, char* db_file_str, u_int32_t flags, u_int32_t env_flags){
	int ret = init_config_db(db_home_str, db_file_str, flags, env_flags);
	ret = init_dbs(flags);
	return ret;
}

int test_existing_config_db(char *env_home_str, char* db_file_str){
	config_db = malloc(strlen(db_file_str) + 8);
	strcpy(config_db, db_file_str);
	strcat(config_db, "_config");

	char *config_file = malloc(strlen(env_home_str) + strlen(db_file_str) + 9);
	strcpy(config_file, env_home_str);
	strcat(config_file, "/");
	strcat(config_file, db_file_str);
	strcat(config_file, "_config");

	if (debug == 1) printf("config file:%s:\n", config_file);

	if( exists( config_file) != 0 ) {
	    // file exists
		env_home = env_home_str;
		return 0;
	} else {
	    // file doesn't exist
		return -1;
	}

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

int init_config_db(char* env_home_str, char* db_file_str, u_int32_t flags, u_int32_t env_flags){
	int ret;

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
	ret = env_home_p->set_cachesize(env_home_p, 0, 128 * 1024 * 1024, 1);
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
		fprintf(stderr, "Error opening database: %s\n", db_strerror(ret));
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
	printf("Opening the dictionary databases ...");

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
		fprintf(stderr, "Error opening database: %s\n", db_strerror(ret));
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
		fprintf(stderr, "Error opening database: %s\n", db_strerror(ret));
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
		fprintf(stderr, "Error opening database: %s\n", db_strerror(ret));
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
		fprintf(stderr, "Error opening database: %s\n", db_strerror(ret));
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
		fprintf(stderr, "Error opening database: %s\n", db_strerror(ret));
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
		fprintf(stderr, "Error opening database: %s\n", db_strerror(ret));
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
		fprintf(stderr, "Error opening database: %s\n", db_strerror(ret));
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
