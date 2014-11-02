/*
 * triple_path.h
 *
 *  Created on: Oct 22, 2014
 *      Author: vinh
 */

#ifndef TRIPLE_PATH_H_
#define TRIPLE_PATH_H_

//#include <rdf_graph.h>
#include <common.h>
#include <pqueue.h>

#ifndef DEBUG
#define DEBUG 1
#endif

typedef struct node NODE;
struct node{
	id_type nodeid;
	int distance;
};

enum DBP{
	DICT,
	RDICT,
	DS2PO,
	SS2PO
};

/* Temp database*/
DB_ENV *tmp_env_p;
DB *tmp_db_p;
char *tmp_db_name;
char *tmp_env_home;

/* Function declarations*/
char* lookup_id_reverse(DB*, id_type id);
id_type lookup_id(DB *, char*);
id_type lookup_stat(DB *db, id_type id);

int init_tmp_db();
void remove_tmp_db(DB *tmp_db);

int put_tmp_db(DB*, id_type, id_type, int);

/*Graph algo functions*/
void db_op(char*, int, char*);
int get_neighbors_id(char *node);
int get_duplicate_values(DB*, id_type, enum TYPE, enum TYPE, int, pri_queue);
int find_shortest_path(char*, char*);
int start_finding(id_type, id_type);
int explore_neighbors_loop(DB*, DB *dbp, id_type startid, id_type endid, id_type id, int distance, pri_queue pqueue, int sink_excluded);
void show_path(DB*, id_type, id_type);

#endif /* QUERY_H_ */
