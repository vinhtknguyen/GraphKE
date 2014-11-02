/*
 * triplepath.c
 *
 *  Created on: Oct 23, 2014
 *      Author: vinh
 */

#include <pqueue.h>
#include <graphke.h>
#include "uthash.h"

typedef struct pathnode{
	id_type id;
	id_type previd;
	id_type subject;
	id_type predicate;
	id_type object;
	int distance;
	UT_hash_handle hh;
} PATHNODE;

typedef struct pair{
	id_type start;
	id_type end;
	UT_hash_handle hh;
}PAIR;

typedef struct term{
	id_type id;
	UT_hash_handle hh;
}TERM;

pri_queue pairbuffer;

int explore_neighbors_loop(PATHNODE **, DB *dbp, id_type startid, id_type endid, id_type id, int distance, pri_queue pqueue, int sink_excluded);
int path_bulk(char *filein, char* fileout, int n);
void *path_bulk_pthread(void *);
void show_node_path_inmem(PATHNODE **pathnodes, id_type startid, id_type endid);
void show_triple_path_inmem(PATHNODE **pathnodes, id_type startid, id_type endid, id_type, id_type, id_type);

void db_op(char *db, char* str, char* op){
	DB *dbp;
	int size;
	if (strcmp(db, "dict") == 0){
		dbp = dict_db_p;
		if (strcmp(op, "print_db_id") == 0){
			size = atoi(str);
			int ret = print_db(dbp, SHORTEN_URI, ID_TYPE, size);
			printf("Printed %d items.\n", ret);
		} else if (strcmp(op, "print_db_str") == 0){
			size = atoi(str);
			int ret = print_db_str(dbp, SHORTEN_URI, ID_TYPE, size);
			printf("Printed %d items.\n", ret);
		} else if (strcmp(op, "print_key_value_id") == 0){
			int ret = print_db_value_id(dbp, SHORTEN_URI, ID_TYPE, str);
			printf("Got %d values for key %s.\n",ret,  str);
		} else if (strcmp(op, "print_key_value_str") == 0){
			int ret = print_db_value_str(dbp, SHORTEN_URI, ID_TYPE, str);
			printf("Got %d values for key %s.\n",ret,  str);
		} else {
			printf("Size: %ld\n", get_db_size(dbp));
		}
	} else if (strcmp(db, "rdict") == 0){
		dbp = rdict_db_p;
		if (strcmp(op, "print_db_id") == 0){
			size = atoi(str);
			int ret = print_db(dbp, ID_TYPE, SHORTEN_URI, size);
			printf("Printed %d items.\n", ret);
		} else if (strcmp(op, "print_db_str") == 0){
			size = atoi(str);
			int ret = print_db_str(dbp, ID_TYPE, SHORTEN_URI, size);
			printf("Printed %d items.\n", ret);
		} else if (strcmp(op, "print_key_value_id") == 0){
			int ret = print_db_value_id(dbp, ID_TYPE, SHORTEN_URI, str);
			printf("Got %d values for key %s.\n",ret,  str);
		} else if (strcmp(op, "print_key_value_str") == 0){
			int ret = print_db_value_str(dbp, ID_TYPE, SHORTEN_URI, str);
			printf("Got %d values for key %s.\n",ret,  str);
		} else {
			printf("Size: %ld\n", get_db_size(dbp));
		}
	} else if (strcmp(db, "data_s2po") == 0){
		dbp = data_s2po_db_p;
		if (strcmp(op, "print_db_id") == 0){
			size = atoi(str);
			int ret = print_db(dbp, ID_TYPE, PO_ID, size);
			printf("Printed %d items.\n", ret);
		} else if (strcmp(op, "print_db_str") == 0){
			size = atoi(str);
			int ret = print_db_str(dbp, ID_TYPE, PO_ID, size);
			printf("Printed %d items.\n", ret);
		} else if (strcmp(op, "print_key_value_id") == 0){
			int ret = print_db_value_id(dbp, ID_TYPE, PO_ID, str);
			printf("Got %d values for key %s.\n",ret,  str);
		} else if (strcmp(op, "print_key_value_str") == 0){
			int ret = print_db_value_str(dbp, ID_TYPE, PO_ID, str);
			printf("Got %d values for key %s.\n",ret,  str);
		} else {
			printf("Size: %ld\n", get_db_size(dbp));
		}
	} else if (strcmp(db, "stat_s2po") == 0){
		dbp = stat_s2po_db_p;
		if (strcmp(op, "print_db_id") == 0){
			size = atoi(str);
			int ret = print_db(dbp, ID_TYPE, LONG, size);
			printf("Printed %d items.\n", ret);
		} else if (strcmp(op, "print_db_str") == 0){
			size = atoi(str);
			int ret = print_db_str(dbp, ID_TYPE, LONG, size);
			printf("Printed %d items.\n", ret);
		} else if (strcmp(op, "print_key_value_id") == 0){
			int ret = print_db_value_id(dbp, ID_TYPE, LONG, str);
			printf("Got %d values for key %s.\n",ret,  str);
		} else if (strcmp(op, "print_key_value_str") == 0){
			int ret = print_db_value_str(dbp, ID_TYPE, LONG, str);
			printf("Got %d values for key %s.\n",ret,  str);
		} else {
			printf("Size: %ld\n", get_db_size(dbp));
		}
	}  else if (strcmp(db, "dict_prefixes") == 0){
		dbp = dict_prefixes_db_p;
		if (strcmp(op, "print_db_id") == 0){
			size = atoi(str);
			int ret = print_db(dbp, CHAR_P, CHAR_P, size);
			printf("Printed %d items.\n", ret);
		} else if (strcmp(op, "print_db_str") == 0){
			size = atoi(str);
			int ret = print_db_str(dbp, CHAR_P, CHAR_P, size);
			printf("Printed %d items.\n", ret);
		} else if (strcmp(op, "print_key_value_id") == 0){
			int ret = print_db_value_id(dbp, CHAR_P, CHAR_P, str);
			printf("Got %d values for key %s.\n",ret,  str);
		} else if (strcmp(op, "print_key_value_str") == 0){
			int ret = print_db_value_str(dbp, CHAR_P, CHAR_P, str);
			printf("Got %d values for key %s.\n",ret,  str);
		} else {
			printf("Size: %ld\n", get_db_size(dict_prefixes_db_p));
		}
	} else {
		dbp = data_s2po_db_p;
		if (strcmp(op, "print_db_id") == 0){
			size = atoi(str);
			int ret = print_db(dbp, ID_TYPE, PO_ID, size);
			printf("Printed %d items.\n", ret);
		} else if (strcmp(op, "print_db_str") == 0){
			size = atoi(str);
			int ret = print_db_str(dbp, ID_TYPE, PO_ID, size);
			printf("Printed %d items.\n", ret);
		} else if (strcmp(op, "print_key_value_id") == 0){
			int ret = print_db_value_id(dbp, ID_TYPE, PO_ID, str);
			printf("Got %d values for key %s.\n",ret,  str);
		} else if (strcmp(op, "print_key_value_str") == 0){
			int ret = print_db_value_str(dbp, ID_TYPE, PO_ID, str);
			printf("Got %d values for key %s.\n",ret,  str);
		} else {
			printf("Size: %ld\n", get_db_size(dbp));
		}
	}
}


int get_neighbors_id(char *node){
	int ret;
	// Look up for id
	id_type nodeid = lookup_id(dict_db_p, node);
	if (DEBUG == 1) {
		printf("Getting neighbors for: ");
		printf(ID_TYPE_FORMAT, nodeid);
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
		id_type c;
		pri_type p;
		while ((c = (id_type)priq_pop(pqueue, &p))){
			printf("Popped item: ");
			printf(ID_TYPE_FORMAT, c);
			printf(" with pri: ");
			printf("%d", (int)p);
			printf("\n");
		}
	}

	return ret;
}


int get_neighbors_str(char *node){
	int ret;
	// Look up for id
	id_type nodeid = lookup_id(dict_db_p, node);
	if (DEBUG == 1) {
		printf("Getting neighbors for: ");
		printf(ID_TYPE_FORMAT, nodeid);
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
		id_type c;
		pri_type p;
		while ((c = (id_type)priq_pop(pqueue, &p))){
			printf("Popped item: ");
			printf("%", lookup_id_reverse(rdict_db_p, c));
			printf(" with pri: ");
			printf("%d", (int)p);
			printf("\n");
		}
	}

	return ret;
}


char *makeuri(char *in){
	char *out = malloc(strlen(in) + 3);
	strcpy(out, "<");
	strcat(out, in);
	strcat(out, ">");
	return out;
}

int generate_term_pairs(char *filein, char *fileout){

	FILE *fi = fopen(filein, "r");
	if (fi == NULL){
	    printf("Error opening file %s!\n", filein);
	    exit(-1);
	}

	FILE *fo = fopen(fileout, "w+");
	if (fo == NULL){
	    printf("Error opening file %s to write!\n", fileout);
	    exit(-1);
	}

	size_t linesize = 2000;
	char *linebuff = NULL;
	ssize_t linelen = 0;
	TERM *nodehead = NULL;


	char *pos = NULL, *prevpos = NULL, *start, *end;
	while ((linelen = getline(&linebuff, &linesize, fi)) > 0) {
		// Make a URI
		int size = strlen(linebuff);
		if (strlen(linebuff) > 0 && (linebuff[size-1] == '\n')) {
			linebuff[size-1] = '\0';
		}
		if (size > 1){
			pos = strtok(linebuff, "\t");
			start = strtok(NULL, "\t");
			end = strtok(NULL, "\t");
			if (DEBUG == 1) {
				printf("Before: %d %s %s %s %s", size, prevpos, pos, start, end);
				printf("\n");
			}
			if (prevpos == NULL) {
				prevpos = malloc(strlen(pos) + 1);
				strcpy(prevpos, pos);
			}

			if (DEBUG == 1) {
				printf("After: %d %s %s %s %s ", size, prevpos, pos, start, end);
				printf("\n");
			}
			if (strcmp(prevpos, pos) != 0){
				// Generating pairs
				generate_pairs(nodehead, fo);
				TERM *t, *tt;
				HASH_ITER(hh, nodehead, t, tt) {
					HASH_DEL(nodehead, t);
					free(t);
				}
				nodehead = NULL;
				free(prevpos);
				prevpos = malloc(strlen(pos) + 1);
				strcpy(prevpos, pos);
			}
			if (start != NULL && end != NULL){
				char *newstart = makeuri(start);
				char *newend = makeuri(end);
	//			if (DEBUG == 1) printf("Found %s:\n", term);
				if (DEBUG == 1) {
					printf("Got %s %s %s", pos, newstart, newend);
					printf("\n");
				}
				TERM *tmp = NULL;
				id_type id1 = lookup_id(dict_db_p, newstart);
				if (id1 > 0){
					HASH_FIND_PTR(nodehead, &id1, tmp);
					if (tmp == NULL) {
						TERM *term1 = malloc(sizeof(TERM));
						term1->id = id1;
						HASH_ADD_PTR(nodehead, id, term1);
					}
				}

				tmp = NULL;
				id_type id2 = lookup_id(dict_db_p, newend);
				if (id2 > 0){
					HASH_FIND_PTR(nodehead, &id2, tmp);
					if (tmp == NULL) {
						TERM *term2 = malloc(sizeof(TERM));
						term2->id = id2;
						HASH_ADD_PTR(nodehead, id, term2);
					}
				}
				if (DEBUG == 1) {
					printf("Got ");
					printf(ID_TYPE_FORMAT, id1);
					printf("\t");
					printf("Got ");
					printf(ID_TYPE_FORMAT, id2);
					printf("\n");
				}
				free(newstart);
				free(newend);

			}
		}
	}
	if (nodehead != NULL) generate_pairs(nodehead, fo);
	free(prevpos);
	free(linebuff);

	TERM *t, *tt;
	HASH_ITER(hh, nodehead, t, tt) {
		HASH_DEL(nodehead, t);
		free(t);
	}
	nodehead = NULL;

	fclose(fi);
	fclose(fo);
	return 0;
}

int path_bulk(char *filein, char* fileout, int n){

	/*
	 * Read the filein and put all the terms into a queue
	 * Start n threads to compute the path
	 * For now, one thread
	 */

	FILE *fi = fopen(filein, "r+");
	if (fi == NULL){
	    printf("Error opening file %s!\n", filein);
	    exit(-1);
	}

//	char *fo_pairs_str = malloc(strlen(fileout) + 7);
//	strcpy(fo_pairs_str, fileout);
//	strcat(fo_pairs_str, "_pairs");

	/* Generating pairs */
//	generate_term_pairs(filein, fo_pairs_str);

	size_t linesize = 2000;
	char *linebuff = NULL;
	ssize_t linelen = 0;
	TERM *nodehead = NULL;
	pairbuffer = priq_new(0);

	FILE *fo = fopen(fileout, "w+");
	if (fo == NULL){
	    printf("Error opening file %s to write!\n", fileout);
	    exit(-1);
	}

	char *pos = NULL, *prevpos = NULL, *start, *end;
	while ((linelen = getline(&linebuff, &linesize, fi)) > 0) {
		// Make a URI
		int size = strlen(linebuff);
		if (strlen(linebuff) > 0 && (linebuff[size-1] == '\n')) {
			linebuff[size-1] = '\0';
		}
		if (linebuff != NULL){
			start = strtok(linebuff, "\t");
			end = strtok(NULL, "\t");
			if (start != NULL && end != NULL){
				PAIR *newpair1 = (PAIR*) malloc(sizeof(PAIR));
				newpair1->start = lookup_id(dict_db_p, start);
				newpair1->end = lookup_id(dict_db_p, end);
				priq_push(pairbuffer, newpair1, 0);
			}
		}
	}
	free(linebuff);
	fclose(fo);
	fclose(fi);

	(void)mutex_init(&write_data_s2po_db_lock, NULL);
	(void)mutex_init(&write_dict_db_lock, NULL);

	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	if (DEBUG == 1) printf("Start creating threads \n");

	pthread_t threadids[n];
	int err, i = 0;
	void *status;
	for (i = 0; i < n; i++){
		err = pthread_create(&(threadids[i]), &attr, &path_bulk_pthread, (void *)fileout);
		if (err != 0) printf("\ncan't create thread %d:[%s]", i, strerror(err));
	}
	// /Experiments/cprojects/GraphKE/developer_logs/test_yagoinput.txt

	pthread_attr_destroy(&attr);

	if (DEBUG == 1) printf("Start joining threads \n");
	for (i = 0; i < n; i++) {
		err = pthread_join(threadids[i], &status);
		if (err) {
			printf("ERROR; return code from pthread_join() is %d\n", err);
			exit(-1);
		}
	}

	pthread_mutex_destroy(&write_data_s2po_db_lock);
	pthread_mutex_destroy(&write_dict_db_lock);


	return 0;
}

int generate_pairs(TERM *nodehead, FILE *fo){
	int size = 0;
	// Generating pairs
	TERM *d1, *d2;
	char *term1, *term2;
	id_type id1, id2;
	if (DEBUG == 1) printf("Generating pairs\n");
	for (d1 = nodehead; d1 != NULL; d1=d1->hh.next){
		for (d2 = nodehead; d2 != NULL; d2=d2->hh.next){
			if (d2->id != d1->id){
				size++;
//				if (DEBUG == 1) {
//					printf("From ");
//					printf(ID_TYPE_FORMAT, d1->id);
//					printf(" to ");
//					printf(ID_TYPE_FORMAT, d2->id);
//					printf("\n");
//				}
				term1 = print_dbt_str(&(d1->id), ID_TYPE);
				term2 = print_dbt_str(&(d2->id), ID_TYPE);
				fprintf(fo, "%s\t%s\t\n", term1, term2);
				free(term1);
				free(term2);
			}
		}
	}
	return size;
}

void *path_bulk_pthread(void *fileout_str){

	PAIR *pair;
	id_type p;
	id_type start, end;
	int ret;
	int i = 0;

	char *fo_str = (char*) fileout_str;
	FILE *fo = fopen(fo_str, "a+");

	if (fo == NULL){
	    printf("Error opening file %s to write!\n", fo_str);
	    exit(-1);
	}

	char *startstr, *endstr;
	while (1){
		pair = (PAIR*)priq_pop(pairbuffer, &p);
		if (pair != NULL){

			struct timeval tvStart, tvEnd, tvDiff;
			gettimeofday(&tvStart, NULL);
			printf("%d\n", i++);

			if (DEBUG == 1) {
//				printf("From ");
//				printf(ID_TYPE_FORMAT, start);
//				printf(" to ");
//				printf(ID_TYPE_FORMAT, end);
//				printf("\n");
			}

			ret = start_finding(pair->start, pair->end);
			startstr = print_dbt_str(&(pair->start), ID_TYPE);
			endstr = print_dbt_str(&(pair->end), ID_TYPE);

			gettimeofday(&tvEnd, NULL);
			timeval_subtract(&tvDiff, &tvEnd, &tvStart);

			fprintf(fo, "%d\t", ret);
			fprintf(fo, "%ld.%06ld sec\t", (long int)tvDiff.tv_sec,(long int)tvDiff.tv_usec);
			fprintf(fo, "%s\t", startstr);
			fprintf(fo, "%s\n", endstr);

			free(startstr);
			free(endstr);
			free(pair);

		} else {
			break;
		}
	}
	if (DEBUG == 1) printf("Thread is done.\n");
	fclose(fo);
	return 0;
}

int start_finding(id_type startid, id_type endid){
	int ret;

	int sink_excluded = 1;

	pri_queue pqueue = priq_new(0);
	id_type id;
	pri_type dis = 1;
	PATHNODE *pathnodes = NULL, *pathnode, *nodetmp;

	id_type nodeid_stat = lookup_stat(stat_s2po_db_p, startid);
	if (nodeid_stat < 0){
		printf("No path found.\n");
		return -1;
	} else {
		//		if (DEBUG == 1) printf("Putting startid into the queue and the map\n");
		priq_push(pqueue, (void*)startid, dis);
		//		ret = put_tmp_db(tmp_db_p, startid, startid, (int)dis);
		ret = put_node(&pathnodes, startid, startid, (int)dis, -1, -1, -1);
	}

	if (DEBUG == 1) printf("Start finding\n");
	int result = 0;
	while (1){
		//		if (DEBUG == 1) printf("Next round\n");
		id = (id_type)priq_pop(pqueue, &dis);
		// Not yet visited
		if (DEBUG == 1){
			printf("Exploring node ");
			printf(ID_TYPE_FORMAT, id);
			printf(" with pri: %d", dis);
			printf("\n");
		}
		if (id == 0) {
			ret = 0;
			break;
		}
		ret = explore_neighbors_loop(&pathnodes, data_s2po_db_p, startid, endid, id, (int)dis, pqueue, sink_excluded);
		if (ret > 0){
			//			printf("Found\n");
			break;
		}
		//		if (DEBUG == 1){
		//			printf("Popped item: ");
		//			printf(ID_TYPE_FORMAT, id);
		//			printf(" with pri: %d", dis);
		//			printf("\n");
		//		}
		// Get the set of (predicate, object) pairs
	}


	PATHNODE *d, *pt;
	//	if (DEBUG == 1) printf("\n\nPrinting priqueue\n");
	//		for(d = pathnodes; d != NULL; d=d->hh.next) {
	//			printf("Node %s has previous %s and distance ", lookup_id_reverse(rdict_db_p, d->id), lookup_id_reverse(rdict_db_p, d->previd));
	//			printf(ID_TYPE_FORMAT, d->distance);
	//			printf("\n");
	//		}
	//		if (DEBUG == 1) printf("\n\nEnd Printing priqueue\n");

	// Print the path
	PATHNODE *de, *n, *nt;
	if (ret > 0){
		printf("Node path:\n");
		show_node_path_inmem(&pathnodes, startid, endid);
		printf("Triple path:\n");
		show_triple_path_inmem(&pathnodes, startid, endid, -1, -1, -1);
	}
	//	if (ret == 1){
	//		printf("Path:\n");
	//		show_path_inmem(startid, endid);
	//		printf("\n");
	//	}

//	for(d = pathnodes; d != NULL; d=d->hh.next) {
//		SPOID* spoid = d->spoid;
//		free(spoid);
//	}

	// Freeing the variables
	/* free the mymapping contents */
	HASH_ITER(hh, pathnodes, n, nt) {
		HASH_DEL(pathnodes, n);
		free(n);
	}
	free(pqueue->buf);
	free(pqueue);
	//	remove_tmp_db(tmp_db_p, "tmp", 0);
	return ret;
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

	if (startid == endid && startid > 0){
		printf("Two nodes are same.\n", end);
		return 0;
	}
	if (ret >= 0){
		// Searching for shortest path
		if (DEBUG == 1) {
			printf("Finding the shortest path from %s (", start);
			printf(ID_TYPE_FORMAT, startid);
			printf(") to %s (", end);
			printf(ID_TYPE_FORMAT, endid);
			printf(")...\n");
		}
		ret = start_finding(startid, endid);
	} else {
		if (DEBUG == 1) printf("Invalid input\n");
	}
	return ret;
}

void show_node_path_inmem(PATHNODE **pathnodes, id_type startid, id_type endid){
	PATHNODE *node = NULL, *tmp;
	HASH_FIND_PTR(*pathnodes, &endid, node);
	char *str;
	if (node != NULL){
		id_type id = node->id;
		if (node->id == startid){
			str = print_dbt_str(&id, ID_TYPE);
			printf("%s\n", str);
			free(str);
			return;
		} else {
			show_node_path_inmem(pathnodes, startid, node->previd);
		}
		char *str = print_dbt_str(&id, ID_TYPE);
		printf("%s\n", str);
		free(str);
	}
}

int compare_spoid(SPOID *spoid1, SPOID* spoid2){
	if (spoid1 == NULL || spoid2 == NULL) return -1;
	return ((spoid1->subject - spoid2->subject + spoid1->predicate - spoid2->predicate + spoid1->object - spoid2->object));
}

void show_triple_path_inmem(PATHNODE **pathnodes, id_type startid, id_type endid, id_type prevsub, id_type prevpred, id_type prevobj){
	PATHNODE *node = NULL, *tmp;
	HASH_FIND_PTR(*pathnodes, &endid, node);
	char *str;
	if (node != NULL){


		if (node->id != startid){
			show_triple_path_inmem(pathnodes, startid, node->previd, node->subject, node->predicate, node->object);
		} else {
			return;
		}
		if ((node->subject - prevsub != 0) || (node->predicate - prevpred != 0) || (node->object - prevobj != 0)){
//			printf("At node ");
//			print_dbt_str(&endid, ID_TYPE);
//			printf("\n");
			str = print_dbt_str(&(node->subject), ID_TYPE);
			printf("%s\t", str);
			free(str);
			str = print_dbt_str(&(node->predicate), ID_TYPE);
			printf("%s\t", str);
			free(str);
			str = print_dbt_str(&(node->object), ID_TYPE);
			printf("%s\n", str);
			free(str);
		}
	}
	//	free(endstr);
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
	//	if (DEBUG == 1) printf("Path: \n");
	int ret = tmp_db->get(tmp_db, 0, &key, &value, 0);
	if (ret == 0){
		NODE *prevnode = (NODE*)value.data;
		if (prevnode->nodeid != startid){
			show_path(tmp_db, startid, prevnode->nodeid);
		} else {
			// print this id first
			char *str = print_dbt_str(&startid, ID_TYPE);
			printf("%s\n", str);
			free(str);
		}
	}
	// print this id in the last
	char *str = print_dbt_str(&endid, ID_TYPE);
	printf("%s\n", str);
	free(str);
	free(value.data);
}

int put_node(PATHNODE **pathnodes, id_type nodeid, id_type prev, int dis, id_type sub, id_type pred, id_type obj){
	PATHNODE *d, *n, *nt;
//	char *tmp = lookup_id_reverse(rdict_db_p, nodeid);
	//	printf("Adding key: %s ", tmp);
	//	printf(" previd: %s ", lookup_id_reverse(rdict_db_p, prev));
	//	printf(" dis: %d\n", dis);

	int ret = 0;
	PATHNODE *existingnode = NULL, *newnode;
	HASH_FIND_PTR(*pathnodes, &nodeid, existingnode);
	if (existingnode == NULL){
		ret = 1;
		//		if (DEBUG == 1) printf("this is a new key\n");
	} else if (dis < existingnode->distance){
		//		if (DEBUG == 1) {
		//			printf("delete existing key\n");
		//			printf(ID_TYPE_FORMAT, existingnode->id);
		//			printf("\t previd: ");
		//			printf(ID_TYPE_FORMAT, existingnode->previd);
		//			printf("\t dis: %d\n", existingnode->distance);
		//		}
		HASH_DEL(*pathnodes, existingnode);
		ret = 1;
	} else ret = 0;
	if (ret == 1){
		newnode = (PATHNODE*) malloc(sizeof(PATHNODE));
		newnode->id = nodeid;
		newnode->previd = prev;
		newnode->distance = dis;
		if (sub > 0 && pred > 0 && obj > 0){
			newnode->subject = sub;
			newnode->predicate = pred;
			newnode->object = obj;
		}
		HASH_ADD_PTR(*pathnodes, id, newnode);
		//		if (DEBUG == 1) {
		//			tmp = lookup_id_reverse(rdict_db_p, newnode->id);
		//			printf("Added key: %s ", tmp);
		//			printf(" previd: %s ", lookup_id_reverse(rdict_db_p, newnode->previd));
		//			printf(" dis: %d\n", newnode->distance);
		//		}
	}
	return ret;
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
	//	if (DEBUG == 1) {
	//		printf("Getting key: ");
	//		printf(ID_TYPE_FORMAT, id);
	//		printf("\t previd: ");
	//		printf(ID_TYPE_FORMAT, prev);
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
		if (DEBUG == 1) {
			printf("Putting key: ");
			printf(ID_TYPE_FORMAT, id);
			printf("\t previd: ");
			printf(ID_TYPE_FORMAT, prev);
			printf("\t dis: %d\n", dis);
		}
	}
	return update;
}


int explore_neighbors_loop(PATHNODE **pathnodes, DB *dbp, id_type startid, id_type endid, id_type id, int distance, pri_queue pqueue, int sink_excluded){
	DBT key, value;
	DBC *cursorp;
	int ret, ret1;
	id_type predicate, object, predicate_stat, object_stat;

	cursorp = NULL;
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

	ret = cursorp->c_get(cursorp, &key, &value, DB_SET);
	if (!ret){
		do {
			/* Get the key DBT used for the database read */
			switch (ret) {
			case 0:
//				if (DEBUG == 1){
//					printf("value:");
//					print_dbt((void*)key.data, ID_TYPE);
//					printf("\t");
//					print_dbt((void*)value.data, PO_ID);
//					printf("\n");
//				}
				printf("");
				POID *poid = (POID*)value.data;
				predicate = poid->predicate;
				object = poid->object;
				//			ret1 = put_tmp_db(tmp_db, predicate, id, distance + 1);
				//			if (DEBUG == 1){
				//				printf("From %s ==============>>>>>>>", lookup_id_reverse(rdict_db_p, predicate));
				//				printf("to %s \n", lookup_id_reverse(rdict_db_p, id));
				//			}
				ret1 = put_node(pathnodes, predicate, id, distance + 1, id, predicate, object);

				if (predicate == endid && predicate > 0){
					char *predstr = print_dbt_str(&predicate, ID_TYPE);
					printf("%s\n", predstr);
					printf("Found shortest path for %s %d\n", predstr, distance+1);
					free(predstr);
					//				show_path(tmp_db_p, startid, id);
					//				printf(ID_TYPE_FORMAT, id);
					//				printf("\t");
					//				printf(ID_TYPE_FORMAT, predicate);
					return distance+1;
				}
				if (predicate % 2 == 0 && sink_excluded == 1){
					if (ret1 == 1){
						// Shorter distance found
						priq_push(pqueue, (void*)predicate, distance + 1);
					}
				}

				//			ret1 = put_tmp_db(tmp_db, object, predicate, distance + 2);
				//			if (DEBUG == 1){
				//				printf("From %s  ==============>>>>>>>", lookup_id_reverse(rdict_db_p, object));
				//				printf("to %s \n", lookup_id_reverse(rdict_db_p, predicate));
				//			}
				ret1 = put_node(pathnodes, object, predicate, distance + 2, id, predicate, object);
				if (object == endid  && object > 0){
					char *objstr = print_dbt_str(&object, ID_TYPE);
					printf("%s\n", objstr);
					printf("Found shortest path for %s %d\n", objstr, distance + 2);
					free(objstr);
					//				show_path(tmp_db_p, startid, id);
					//				printf(ID_TYPE_FORMAT, id);
					//				printf("\t");
					return distance+2;
				}
				if (object % 2 == 0 && sink_excluded == 1){
					if (ret1 == 1){
						// Shorter distance found
						priq_push(pqueue, (void*)object, distance + 2);
					}
				}

				break;
			case DB_NOTFOUND:
				break;
			default:
				if (DEBUG == 1) printf("error setting db_set\n");
				goto cursor_err;
			}
			//		memset(&value, 0, sizeof(DBT));

		} while ((ret = cursorp->get(cursorp, &key, &value, DB_NEXT_DUP)) == 0);

	}
	cursorp->close(cursorp);

	return -1;
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
		//		if (DEBUG == 1){
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
					if (DEBUG == 1){
						printf("Value:");
						printf(ID_TYPE_FORMAT, predicate);
						printf("\t with pri:");
						printf(ID_TYPE_FORMAT, predicate_stat);
						printf("\t");
					}
				} else if (sink_excluded == 0){
					if (DEBUG == 1){
						printf("Value:");
						printf(ID_TYPE_FORMAT, predicate);
						printf("\t");
					}
				}
			} else if (sink_excluded == 0){
				if (DEBUG == 1){
					printf("Value:");
					printf(ID_TYPE_FORMAT, predicate);
					printf("\t");
				}
			}

			object = poid->object;
			if (object % 2 == 0){
				object_stat = lookup_stat(stat_s2po_db_p, object);
				if (sink_excluded == 1 && object_stat > 0){
					priq_push(pq, (void*)object, object_stat);
					if (DEBUG == 1){
						printf("Value:");
						printf(ID_TYPE_FORMAT, object);
						printf("\t with pri:");
						printf(ID_TYPE_FORMAT, object_stat);
						printf("\n");
					}
				} else if (sink_excluded == 0){
					if (DEBUG == 1){
						printf("Value:");
						printf(ID_TYPE_FORMAT, object);
						printf("\n");
					}
				}
			} else  if (sink_excluded == 0){
				if (DEBUG == 1){
					printf("Value:");
					printf(ID_TYPE_FORMAT, object);
					printf("\n");
				}
			}


			break;
		case DB_NOTFOUND:
			break;
		default:
			if (DEBUG == 1) printf("error setting db_set\n");
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




