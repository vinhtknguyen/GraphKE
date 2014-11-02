/*
 * rdf_graph.c
 *
 *  Created on: Oct 22, 2014
 *      Author: vinh
 */

#include <db.h>
#include <graphke.h>
#include <common.h>
#include <uthash.h>
/**
 * For loading data into memory
 */

typedef struct myprefixes{
	char *prefix;
	char *uri;
	UT_hash_handle hh;
} MYPREFIXES;

typedef struct mydictionary{
	char *term;
	id_type id;
	id_type target;
	UT_hash_handle hh;
}MYDICTIONARY;

typedef struct mymapping{
	id_type source;
	id_type target;
	UT_hash_handle hh;
} MYMAPPING;

typedef struct mytriples{
	id_type subject;
	id_type predicate;
	id_type object;
	id_type tripleid;
//	SPOID *triple;
	UT_hash_handle hh;
} MYTRIPLES;

MYDICTIONARY *inmemdict;
MYPREFIXES *inmem_u2p;

int numns;

/* Functions*/

MYMAPPING *merging_ids_inmem(MYDICTIONARY*, int);
MYTRIPLES *resolve_ids_inmem(MYMAPPING*, MYTRIPLES*, int );
void* serialize_dict(void*);
void* serialize_rdict(void*);
void* serialize_stat(void*);
void serialize_all();
long serialize_data(MYTRIPLES *, DB*, int);
char* get_full_form_term(MYPREFIXES *, char *, int);
char* get_full_form_term_from_db(char *, int);
char *get_short_form_term(char *in);
void* serialize_prefixes(void* threadid);
char* lookup_prefix(char *str);
char *lookup_id_reverse_full_form(DB *db, id_type id);
int path_bulk(char *, char *, int);
/*
 * Printing the first n elements of the db
 */



char* print_dbt(void* data, enum TYPE type){
	char *ret, *tmp;
	int i;
	long l;
	id_type it;
	char tmpnum[20];
	switch (type){
	case CHAR_P:
		tmp = (char*)data;
		ret = malloc(strlen(tmp) + 1);
		strcpy(ret, tmp);
		break;
	case SHORTEN_URI:
		tmp = (char*) data;
		ret = malloc(strlen(tmp) + 1);
		strcpy(ret, tmp);
		break;
	case INT:
		i = *(int*) data;
		sprintf(tmpnum, "%d", i);
		ret = malloc(strlen(tmpnum) + 1);
		strcpy(ret, tmpnum);
		break;
	case LONG:
		l = *(long*)data;
		sprintf(tmpnum, "%ld", l);
		ret = malloc(strlen(tmpnum) + 1);
		strcpy(ret, tmpnum);
		break;
	case ID_TYPE:
		it = *(id_type*)data;
		sprintf(tmpnum, ID_TYPE_FORMAT, it);
		ret = malloc(strlen(tmpnum) + 1);
		strcpy(ret, tmpnum);
		break;
	case PO_ID:
		ret = poid_to_string((POID*)data);
//		printf("%s", ret);
//		free(ret);
		break;
	case PO_STR:
		ret = postr_to_string((POSTR*)data);
//		printf("%s", ret);
//		free(ret);
		break;
	case SPO_ID:
		ret = spoid_to_string((SPOID*)data);
//		printf("%s", ret);
//		free(ret);
		break;
	case SPO_STR:
		ret = spostr_to_string((SPOSTR*)data);
//		printf("%s", ret);
//		free(ret);
		break;
	default:
		tmp = (char*) data;
		ret = malloc(strlen(tmp) + 1);
		strcpy(ret, tmp);
		free(tmp);
		break;
	}
	return ret;
}

char* print_dbt_str(void* data, enum TYPE type){
	char *ret, *tmp, *sub, *pred, *obj;
	char tmpnum[20];
	int i;
	long l;
	switch (type){
	case CHAR_P:
		tmp = (char*)data;
		ret = malloc(strlen(tmp) + 1);
		strcpy(ret, tmp);
		break;
	case SHORTEN_URI:
		printf("");
		tmp = get_full_form_term_from_db((char*)data, 0);
		ret = malloc(strlen(tmp) + 1);
		strcpy(ret, tmp);
		free(tmp);
//		printf("%s", ret);
//		free(ret);
		break;
	case INT:
		i = *(int*) data;
		sprintf(tmpnum, "%d", i);
		ret = malloc(strlen(tmpnum) + 1);
		strcpy(ret, tmpnum);
		break;
	case LONG:
		l = *(int*) data;
		sprintf(tmpnum, "%d", l);
		ret = malloc(strlen(tmpnum) + 1);
		strcpy(ret, tmpnum);
		break;
	case ID_TYPE:
		printf("");
		tmp = lookup_id_reverse_full_form(rdict_db_p, *(id_type*)data);
		ret = malloc(strlen(tmp) + 1);
		strcpy(ret, tmp);
		free(tmp);
//		printf("%s", ret);
//		free(ret);
		break;
	case PO_ID:
		printf("");
		POID *poid = (POID*)data;
		pred = lookup_id_reverse_full_form(rdict_db_p, poid->predicate);
		obj = lookup_id_reverse_full_form(rdict_db_p, poid->object);
		ret = malloc(strlen(pred) + strlen(obj) + 2);
		strcpy(ret, pred);
		strcat(ret, "\t");
		strcat(ret, obj);
		free(pred);
		free(obj);
		break;
	case PO_STR:
		ret = postr_to_string((POSTR*)data);
//		printf("%s", ret);
		break;
	case SPO_ID:
		printf("");
		SPOID *spoid = (SPOID*)data;
		sub = lookup_id_reverse_full_form(rdict_db_p, spoid->subject);
		pred = lookup_id_reverse_full_form(rdict_db_p, spoid->predicate);
		obj = lookup_id_reverse_full_form(rdict_db_p, spoid->object);
		ret = malloc(strlen(sub) + strlen(pred) + strlen(obj) + 3);
		strcpy(ret, sub);
		strcat(ret, "\t");
		strcat(ret, pred);
		strcat(ret, "\t");
		strcat(ret, obj);
		free(sub);
		free(pred);
		free(obj);
		break;
	case SPO_STR:
		ret = spostr_to_string((SPOSTR*)data);
//		printf("%s", ret);
		break;
	default:
		tmp = (char*) data;
		ret = malloc(strlen(tmp) + 1);
		strcpy(ret, tmp);
		break;
	}
	return ret;
}

int print_db(DB *dbp, enum TYPE ktype, enum TYPE vtype, int n) {
	DBT key, value, value_dup;
	DBC *cursorp;
	int count, ret1, ret2;

	cursorp = NULL;
	count = 0;

	char *keystr, *valuestr;
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
		ret1 = cursorp->c_get(cursorp, &key, &value, DB_NEXT);
		switch (ret1) {
		case 0:
			count++;
			printf("Key:");
			keystr = print_dbt((void*)key.data, ktype);
			printf("%s", keystr);
			printf("\tValue:");
			valuestr = print_dbt((void*)value.data, vtype);
			printf("%s", valuestr);
			printf("\n");
			free(keystr);
			free(valuestr);
			break;
		case DB_NOTFOUND:
			break;
		default:
			dbp->err(dbp, ret1, "print_db_str unspecified error");
			goto cursor_err;
		}
	} while (ret1 == 0 & count < n);

	ret1 = cursorp->close(cursorp);
	if (ret1 != 0) {
		dbp->err(dbp, ret1, "print_db_str: cursor close failed.");
	}

	return count;
	cursor_err: if (cursorp != NULL) {
		ret1 = cursorp->close(cursorp);
		if (ret1 != 0) {
			dbp->err(dbp, ret1, "print_db_str: cursor close failed.");
		}
	}
}

int print_db_str(DB *dbp, enum TYPE ktype, enum TYPE vtype, int n) {
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

	char *keystr, *valuestr;
	do {
		/* Get the key DBT used for the database read */
		memset(&key, 0, sizeof(DBT));
		memset(&value, 0, sizeof(DBT));
		ret1 = cursorp->c_get(cursorp, &key, &value, DB_NEXT);
		switch (ret1) {
		case 0:
			count++;
			printf("Key: ");
			keystr = print_dbt_str((void*)key.data, ktype);
			printf("%s", keystr);
			printf("\tValue:");
			valuestr = print_dbt_str((void*)value.data, vtype);
			printf("%s", valuestr);
			free(keystr);
			free(valuestr);
			printf("\n");
			break;
		case DB_NOTFOUND:
			break;
		default:
			dbp->err(dbp, ret1, "print_db_str unspecified error");
			goto cursor_err;
		}
	} while (ret1 == 0 & count < n);

	ret1 = cursorp->close(cursorp);
	if (ret1 != 0) {
		dbp->err(dbp, ret1, "print_db_str: cursor close failed.");
	}

	return count;
	cursor_err: if (cursorp != NULL) {
		ret1 = cursorp->close(cursorp);
		if (ret1 != 0) {
			dbp->err(dbp, ret1, "print_db_str: cursor close failed.");
		}
	}
}

int print_db_value_id(DB *dbp, enum TYPE ktype, enum TYPE vtype, char* str) {
	DBT key, value, value_dup;
	DBC *cursorp;
	int count, ret1, ret2;

	cursorp = NULL;
	count = 0;

	/* Get the cursor */
	ret1 = dbp->cursor(dbp, 0, &cursorp, 0);
	if (ret1 != 0) {
		dbp->err(dbp, ret1, "print_db_value: cursor open failed.");
		goto cursor_err;
	}

	memset(&key, 0, sizeof(DBT));
	memset(&value, 0, sizeof(DBT));

	switch(ktype){
	case SHORTEN_URI:
		key.data = str;
		key.size = strlen(str) + 1;
		break;
	case ID_TYPE:
		printf("Getting id for %s\n", str);
		char *e;
		id_type id = strtoll(str, &e, 0);
		key.data = &id;
		key.size = sizeof(id_type);
		break;
	case CHAR_P:
		key.data = str;
		key.size = strlen(str) + 1;
		break;
	}
	ret1 = cursorp->c_get(cursorp, &key, &value, DB_SET);
	char *keystr, *valuestr;
	if (!ret1){
		do {
			/* Get the key DBT used for the database read */

				switch (ret1) {
				case 0:
					count++;
					keystr = print_dbt((void*)key.data, ktype);
					valuestr = print_dbt((void*)value.data, vtype);
					printf("Key: %s \t Value: %s \n", keystr, valuestr);
					free(keystr);
					free(valuestr);
					break;
				case DB_NOTFOUND:
					break;
				default:
					dbp->err(dbp, ret1, "print_db_str unspecified error");
					goto cursor_err;
				}

		} while ((ret1 = cursorp->c_get(cursorp, &key, &value, DB_NEXT_DUP)) == 0);
	}

	ret1 = cursorp->close(cursorp);
	if (ret1 != 0) {
		dbp->err(dbp, ret1, "print_db_str: cursor close failed.");
	}

	return count;
	cursor_err: if (cursorp != NULL) {
		ret1 = cursorp->close(cursorp);
		if (ret1 != 0) {
			dbp->err(dbp, ret1, "print_db_str: cursor close failed.");
		}
	}
}

int print_db_value_str(DB *dbp, enum TYPE ktype, enum TYPE vtype, char* str) {
	DBT key, value, value_dup;
	DBC *cursorp;
	int count, ret1, ret2;

	cursorp = NULL;
	count = 0;

	/* Get the cursor */
	ret1 = dbp->cursor(dbp, 0, &cursorp, 0);
	if (ret1 != 0) {
		dbp->err(dbp, ret1, "print_db_value: cursor open failed.");
		goto cursor_err;
	}

	memset(&key, 0, sizeof(DBT));
	memset(&value, 0, sizeof(DBT));

	switch(ktype){
	case SHORTEN_URI:
		key.data = str;
		key.size = strlen(str) + 1;
		break;
	case ID_TYPE:
		printf("");
		id_type id = lookup_id(dict_db_p, str);
		key.data = &id;
		key.size = sizeof(id_type);
		break;
	case CHAR_P:
		key.data = str;
		key.size = strlen(str) + 1;
		break;
	}
	ret1 = cursorp->c_get(cursorp, &key, &value, DB_SET);
	char *keystr, *valuestr;
	if (!ret1){
		do {
			/* Get the key DBT used for the database read */

				switch (ret1) {
				case 0:
					count++;
					keystr = print_dbt_str((void*)key.data, ktype);
					valuestr = print_dbt_str((void*)value.data, vtype);
					printf("Key: %s \t Value: %s \n", keystr, valuestr);
					free(keystr);
					free(valuestr);
					break;
				case DB_NOTFOUND:
					break;
				default:
					dbp->err(dbp, ret1, "print_db_str unspecified error");
					goto cursor_err;
				}

		} while ((ret1 = cursorp->c_get(cursorp, &key, &value, DB_NEXT_DUP)) == 0);
	}

	ret1 = cursorp->close(cursorp);
	if (ret1 != 0) {
		dbp->err(dbp, ret1, "print_db_str: cursor close failed.");
	}

	return count;
	cursor_err: if (cursorp != NULL) {
		ret1 = cursorp->close(cursorp);
		if (ret1 != 0) {
			dbp->err(dbp, ret1, "print_db_str: cursor close failed.");
		}
	}
}
char* poid_to_string(POID *poid){
	char p[256], o[256];
	sprintf(p, ID_TYPE_FORMAT, poid->predicate);
	sprintf(o, ID_TYPE_FORMAT, poid->object);
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
	sprintf(s, ID_TYPE_FORMAT, spoid->subject);
	sprintf(p, ID_TYPE_FORMAT, spoid->predicate);
	sprintf(o, ID_TYPE_FORMAT, spoid->object);
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
	ret = cursorp->get(cursorp, &key, &value, DB_NEXT);
	do {
		//		memset(&key, 0, sizeof(DBT));
		//		memset(&value, 0, sizeof(DBT));
		switch (ret) {
		case 0:
			count++;
			ret = cursorp->get(cursorp, &key, &value, DB_NEXT);
			//			free(key.data);
			//			free(value.data);
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


int merge_dbs_with_mapping(DB *in_p, DB *out_p, DB* mapping_p) {


	/* Setting up the BerkeleyDB parameters for bulk insert */
	DBT dictkey, dictdata, mappingkey, mappingdata;
	DBT inkey, invalue, outkey, outvalue;
	int ret, ret_t;
	void *ptrdict, *ptrmapping;
	u_int32_t flag;

	memset(&dictkey, 0, sizeof(DBT));
	memset(&dictdata, 0, sizeof(DBT));
	memset(&mappingkey, 0, sizeof(DBT));
	memset(&mappingdata, 0, sizeof(DBT));
	flag = DB_MULTIPLE_KEY;

	dictkey.ulen = bulk_size * 1024 * 1024;
	dictkey.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	dictkey.data = malloc(dictkey.ulen);
	memset(dictkey.data, 0, dictkey.ulen);

	dictdata.ulen = 1 * 1024 * 1024;
	dictdata.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	dictdata.data = malloc(dictdata.ulen);
	memset(dictdata.data, 0, dictdata.ulen);

	mappingkey.ulen = bulk_size * 1024 * 1024;
	mappingkey.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	mappingkey.data = malloc(mappingkey.ulen);
	memset(mappingkey.data, 0, mappingkey.ulen);

	mappingdata.ulen = 1 * 1024 * 1024;
	mappingdata.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	mappingdata.data = malloc(mappingdata.ulen);
	memset(mappingdata.data, 0, mappingdata.ulen);

	DB_MULTIPLE_WRITE_INIT(ptrdict, &dictkey);
	DB_MULTIPLE_WRITE_INIT(ptrmapping, &mappingkey);

	DBC *cursorp;
	int mapping_inserted = 0;
	int numdict = 0;
	int nummapping = 0;

	/* Get the cursor */
	ret = in_p->cursor(in_p, 0, &cursorp, 0);
	if (ret != 0) {
		in_p->err(in_p, ret, "count_records: cursor open failed.");
		goto err;
	}
	id_type dictid, tmpid;
	do {
		memset(&inkey, 0, sizeof(DBT));
		memset(&invalue, 0, sizeof(DBT));
		memset(&outvalue, 0, sizeof(DBT));

		inkey.flags = DB_DBT_MALLOC;
		invalue.flags = DB_DBT_MALLOC;
		outkey.flags = DB_DBT_MALLOC;
		outvalue.flags = DB_DBT_MALLOC;

		// Read dictionary item from temporary db
		ret = cursorp->get(cursorp, &inkey, &invalue, DB_NEXT);
		switch (ret) {
		case 0:

			tmpid = *(id_type*) invalue.data;
			// Check if it exists in the main dictionary out_p
			ret_t = out_p->get(out_p, 0, &inkey, &outvalue, 0);

			if (ret_t == 0){
				dictid = *(id_type*) outvalue.data;
				//								if (DEBUG == 1) {
				//									printf("Temp %s id: ", (char*)inkey.data);
				//									print_dbt(invalue.data, ID_TYPE);
				//									printf("\t dict id: ");
				//									print_dbt(outvalue.data, ID_TYPE);
				//									printf("\n");
				//								}
				// This id does exist in the dictionary
				// Check if the two ids are same
				if (tmpid != dictid){
					//										if (DEBUG == 1) {
					//											printf("Inserted mapping from: ");
					//											print_dbt(invalue.data, ID_TYPE);
					//											printf("\t value: ");
					//											print_dbt(outvalue.data, ID_TYPE);
					//											printf("\n");
					//										}
					// Create a mapping from temporary id to official id
					//					ret_t = mapping_p->put(mapping_p, 0, &invalue, &outvalue, 0);
					DB_MULTIPLE_KEY_WRITE_NEXT(ptrmapping, &mappingkey, invalue.data, invalue.size, outvalue.data, outvalue.size);
					assert(ptrmapping != NULL);
					nummapping++;
					mapping_inserted = 1;


				}
			} else {
				//								if (DEBUG == 1) {
				//									printf("Inserting non-existent temp id: ");
				//									print_dbt(invalue.data, ID_TYPE);
				//									printf("\n");
				//								}
				//				if (DEBUG == 1) printf("Insert key: %s, \t data: %lld\n", (char*)inkey.data, *(id_type *)invalue.data);
				// Put it into the main dictionary
				DB_MULTIPLE_KEY_WRITE_NEXT(ptrdict, &dictkey, inkey.data, inkey.size, invalue.data, invalue.size);
				assert(ptrdict != NULL);
				numdict++;
				//			ret_t = out_p->put(out_p, 0, &inkey, &invalue, 0);
			}
			//			cur_node_id = cur_node_id + 1;

			if (numdict % UPDATES_PER_BULK_PUT == 0) {
				//					if (DEBUG == 1) printf("flush buffer to file\n");
				switch (ret = out_p->put(out_p, 0, &dictkey, &dictdata, flag)) {
				case 0:
					DB_MULTIPLE_WRITE_INIT(ptrdict, &dictkey);
					break;
				default:
					out_p->err(out_p, ret, "Bulk DB->put");
					goto err;
				}
			}
			if (nummapping % UPDATES_PER_BULK_PUT == 0) {
				//					if (DEBUG == 1) printf("flush buffer to file\n");
				switch (ret = mapping_p->put(mapping_p, 0, &mappingkey, &mappingdata, flag)) {
				case 0:
					DB_MULTIPLE_WRITE_INIT(ptrmapping, &mappingkey);
					break;
				default:
					mapping_p->err(mapping_p, ret, "Bulk DB->put");
					goto err;
				}
			}

			break;
		case DB_NOTFOUND:
			break;
		default:
			in_p->err(in_p, ret, "Merging db unspecified error");
			goto err;
		}
	} while (ret == 0);

	if (numdict % UPDATES_PER_BULK_PUT != 0) {
		//					if (DEBUG == 1) printf("flush buffer to file\n");
		switch (ret = out_p->put(out_p, 0, &dictkey, &dictdata, flag)) {
		case 0:
			DB_MULTIPLE_WRITE_INIT(ptrdict, &dictkey);
			break;
		default:
			out_p->err(out_p, ret, "Bulk DB->put");
			goto err;
		}
	}
	if (nummapping % UPDATES_PER_BULK_PUT != 0) {
		//					if (DEBUG == 1) printf("flush buffer to file\n");
		switch (ret = mapping_p->put(mapping_p, 0, &mappingkey, &mappingdata, flag)) {
		case 0:
			DB_MULTIPLE_WRITE_INIT(ptrmapping, &mappingkey);
			break;
		default:
			mapping_p->err(mapping_p, ret, "Bulk DB->put");
			goto err;
		}
	}
	return (mapping_inserted);

	err: if (cursorp != NULL) {
		ret = cursorp->close(cursorp);
		if (ret != 0) {
			in_p->err(in_p, ret, "count_records: cursor close failed.");
		}
	}
	close_dbs();
}


int merge_dbs(DB *tmp_db, DB *dict_db) {
	/* Setting up the BerkeleyDB parameters for bulk insert */
	DBT key, data, ktmp, vtmp;
	int ret, ret_t;
	void *ptrk, *ptrd;
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	u_int32_t flag;
	flag = DB_MULTIPLE_KEY;

	key.ulen = bulk_size * 1024 * 1024;
	key.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	key.data = malloc(key.ulen);
	memset(key.data, 0, key.ulen);

	data.ulen = 1 * 1024 * 1024;
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
	//	if (DEBUG == 1) printf("inserting reverse item");
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
			//							if (DEBUG == 1) printf("Insert key: %s, \t data: %lld\n", (char*)ktmp.data, *(id_type *)vtmp.data);

			if ((num + 1) % UPDATES_PER_BULK_PUT == 0) {
				//					if (DEBUG == 1) printf("flush buffer to file\n");
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
		//		if (DEBUG == 1) printf("Committed last buffer of data.\n");
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

int close_db(DB* dbp){
	int ret;
	if (dbp != NULL) {
		ret = dbp->close(dbp, 0);
		if (ret != 0) {
			fprintf(stderr, "%s database close failed.\n", db_strerror(ret));
		}
	}
}

int close_dbs() {
	int ret, ret_t;
	if (DEBUG == 1) printf("Closing databases and environment ...");
	/* Close our database handle, if it was opened. */

	if (dict_db_p != NULL) {
		ret_t = dict_db_p->close(dict_db_p, 0);
		if (ret_t != 0) {
			fprintf(stderr, "%s database dict_db_p close failed.\n", db_strerror(ret_t));
			ret = ret_t;
		}
	}

	if (dict_prefixes_db_p != NULL) {
		ret_t = dict_prefixes_db_p->close(dict_prefixes_db_p, 0);
		if (ret_t != 0) {
			fprintf(stderr, "%s database dict_prefixes_db_p close failed.\n", db_strerror(ret_t));
			ret = ret_t;
		}
	}

	if (rdict_db_p != NULL) {
		ret_t = rdict_db_p->close(rdict_db_p, 0);
		if (ret_t != 0) {
			fprintf(stderr, "%s database rdict_db_p close failed.\n", db_strerror(ret_t));
			ret = ret_t;
		}
	}

	if (data_db_p != NULL) {
		ret_t = data_db_p->close(data_db_p, 0);
		if (ret_t != 0) {
			fprintf(stderr, "%s database data_db_p close failed.\n", db_strerror(ret_t));
			ret = ret_t;
		}
	}

	if (data_s2po_db_p != NULL) {
		ret_t = data_s2po_db_p->close(data_s2po_db_p, 0);
		if (ret_t != 0) {
			fprintf(stderr, "%s database data_s2po_db_p close failed.\n", db_strerror(ret_t));
			ret = ret_t;
		}
	}
	if (data_s2pnp2o_db_p != NULL) {
		ret_t = data_s2pnp2o_db_p->close(data_s2pnp2o_db_p, 0);
		if (ret_t != 0) {
			fprintf(stderr, "%s database data_s2pnp2o_db_p close failed.\n", db_strerror(ret_t));
			ret = ret_t;
		}
	}

	if (stat_s2po_db_p != NULL) {
		ret_t = stat_s2po_db_p->close(stat_s2po_db_p, 0);
		if (ret_t != 0) {
			fprintf(stderr, "%s database data_s2pnp2o_db_p close failed.\n", db_strerror(ret_t));
			ret = ret_t;
		}
	}
	if (stat_s2pnp2o_db_p != NULL) {
		ret_t = stat_s2pnp2o_db_p->close(stat_s2pnp2o_db_p, 0);
		if (ret_t != 0) {
			fprintf(stderr, "%s database data_s2pnp2o_db_p close failed.\n", db_strerror(ret_t));
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
	//	free(db_home);
	free(env_home);
	free(db_file);
	free(dict_file);
	free(stat_file);
	free(dict_db);
	free(dict_prefixes_db);
	free(rdict_db);
	free(data_s2po_db);
	free(data_s2pnp2o_db);
	free(stat_s2po_db);
	free(stat_s2pnp2o_db);
	if (DEBUG == 1) printf("Done.\n");
	return ret;
}

char*
extract_term(char *buf, long start, long end) {
	//	if (DEBUG == 1) printf("Buff %s %ld: to :%ld:", buf, start, end);
	long cur_size = end - start + 2;
	char *term = (char *) malloc((size_t)cur_size * sizeof(char));
	int k = start;
	int j = 0;
	for (j = 0; j < cur_size - 1; j++) {
		term[j] = buf[k];
		k++;
	}
	term[cur_size - 1] = '\0';
	//	if (DEBUG == 1) printf("*%s*%d\n", term, strlen(term));
	return term;
}

void init_variables(char *db_name_str, char *env_home_str, DBTYPE db_type_in, int DEBUG_in) {
	/* Global variables */
	int ret;

	db_name = db_name_str;
	env_home = env_home_str;
	db_type = db_type_in;
	db_home = env_home;

	db_file = malloc(strlen(db_home) + strlen(db_name_str) + 5);
	strcpy(db_file, db_home);
	strcat(db_file, "/");
	strcat(db_file, db_name_str);
	strcat(db_file, "_db");
	if (DEBUG == 1) printf("data file: %s\n", db_file);

	dict_file = malloc(strlen(db_home) + strlen(db_name_str) + 7);
	strcpy(dict_file, db_home);
	strcat(dict_file, "/");
	strcat(dict_file, db_name_str);
	strcat(dict_file, "_dict");
	if (DEBUG == 1) printf("dict file: %s\n", dict_file);

	stat_file = malloc(strlen(db_home) + strlen(db_name_str) + 7);
	strcpy(stat_file, db_home);
	strcat(stat_file, "/");
	strcat(stat_file, db_name_str);
	strcat(stat_file, "_stat");
	if (DEBUG == 1) printf("stat file: %s\n", stat_file);

	dict_db = malloc(strlen(db_name_str) + 6);
	strcpy(dict_db, db_name_str);
	strcat(dict_db, "_dict");

	dict_prefixes_db = malloc(strlen(db_name_str) + 6);
	strcpy(dict_prefixes_db, db_name_str);
	strcat(dict_prefixes_db, "_pref");

	rdict_db = malloc(strlen(db_name_str) + 7);
	strcpy(rdict_db, db_name_str);
	strcat(rdict_db, "_rdict");

	data_s2po_db = malloc(strlen(db_name_str) + 7);
	strcpy(data_s2po_db, db_name_str);
	strcat(data_s2po_db, "_s2po");

	data_s2pnp2o_db = malloc(strlen(db_name_str) + 9);
	strcpy(data_s2pnp2o_db, db_name_str);
	strcat(data_s2pnp2o_db, "_s2pnp2o");

	stat_s2po_db = malloc(strlen(db_name_str) + 7);
	strcpy(stat_s2po_db, db_name_str);
	strcat(stat_s2po_db, "_s2po");

	stat_s2pnp2o_db = malloc(strlen(db_name_str) + 9);
	strcpy(stat_s2pnp2o_db, db_name_str);
	strcat(stat_s2pnp2o_db, "_s2pnp2o");

}

DBTYPE get_dbtype(char *type){
	if (type != NULL){
		if (strcmp(type, "btree") == 0)
			return DB_BTREE;
		else {
			return DB_HASH;
		}
	}
	return DB_HASH;
}

int init_dbs(char *db_name, u_int32_t flags) {
	/* Global variables */

	int ret, ret_t;
	u_int32_t env_flags;

	/* Create the environment */
	ret = db_env_create(&env_home_p, 0);
	if (ret != 0) {
		fprintf(stderr, "Error creating environment handle: %s\n",
				db_strerror(ret));
		goto err;
	}

	env_flags = DB_CREATE | /* Create the environment if it does not exist */
			DB_INIT_LOCK | /* Initialize the locking subsystem */
			DB_INIT_MPOOL | /* Initialize the memory pool (in-memory cache) */
			//			DB_PRIVATE | /* Region files are backed by heap memory.  */
			DB_THREAD; /* Cause the environment to be free-threaded */

	/*
	 * Specify the size of the in-memory cache.
	 */
	ret = env_home_p->set_cachesize(env_home_p, 0, 128 * 1024 * 1024, 1);
	if (ret != 0) {
		fprintf(stderr, "Error increasing the cache size: %s\n",
				db_strerror(ret));
		goto err;
	}

	if (DEBUG == 1) printf("Opening environment in %s\n", env_home);
	/* Now actually open the environment */
	ret = env_home_p->open(env_home_p, env_home, env_flags, 0);
	if (ret != 0) {
		fprintf(stderr, "Error opening environment: %s\n", db_strerror(ret));
		goto err;
	}


	if (DEBUG == 1) printf("Done opening environment\n");
	/* Building dictionary */
	if (DEBUG == 1) printf("Opening the dictionary databases ...\n");

	/* Initialize the DB handle */
	ret = db_create(&dict_db_p, env_home_p, 0);
	if (ret != 0) {
		fprintf(stderr, "Error creating database: %s\n", db_strerror(ret));
		goto err;
	}

	/* Setting up the hash function for the dictionary */
	dict_db_p->set_h_hash = hash;
	dict_db_p->set_priority(dict_db_p, DB_PRIORITY_VERY_HIGH);

	/* Now open the persistent database for the dictionary */
	ret = dict_db_p->open(dict_db_p, /* Pointer to the database */
			0, /* Txn pointer */
			dict_file, /* prog_db_file File name */
			dict_db, /* Logical db name */
			db_type, /* Database type (using btree) */
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
	rdict_db_p->set_priority(rdict_db_p, DB_PRIORITY_VERY_HIGH);
	rdict_db_p->set_h_hash = hash;
	/* Now open the persistent database for the dictionary */
	ret = rdict_db_p->open(rdict_db_p, /* Pointer to the database */
			0, /* Txn pointer */
			dict_file, /* prog_db_file File name */
			rdict_db, /* Logical db name */
			db_type, /* Database type (using btree) */
			flags, /* Open flags */
			0); /* File mode. Using defaults */

	if (ret != 0) {
		fprintf(stderr, "Error opening database: %s\n", db_strerror(ret));
		goto err;
	}

	/* Initialize the DB handle */
	ret = db_create(&dict_prefixes_db_p, env_home_p, 0);
	if (ret != 0) {
		fprintf(stderr, "Error creating database: %s\n", db_strerror(ret));
		goto err;
	}

	/* Setting up the hash function for the dictionary */
	dict_prefixes_db_p->set_h_hash = hash;
	dict_prefixes_db_p->set_priority(dict_prefixes_db_p, DB_PRIORITY_VERY_HIGH);

	/* Now open the persistent database for the dictionary */
	ret = dict_prefixes_db_p->open(dict_prefixes_db_p, /* Pointer to the database */
			0, /* Txn pointer */
			dict_file, /* prog_db_file File name */
			dict_prefixes_db, /* Logical db name */
			db_type, /* Database type (using btree) */
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
			db_type, /* Database type (using btree) */
			flags, /* Open flags */
			0); /* File mode. Using defaults */

	if (ret != 0) {
		fprintf(stderr, "Error opening database: %s\n", db_strerror(ret));
		goto err;
	}
	if (DEBUG == 1) printf("Done.\n");

	/* Initialize the DB handle */
	ret = db_create(&data_s2po_db_p, env_home_p, 0);
	if (ret != 0) {
		fprintf(stderr, "Error creating database data_s2po_db_p handle : %s\n", db_strerror(ret));
		goto err;
	}

	data_s2po_db_p->set_priority(data_s2po_db_p, DB_PRIORITY_VERY_HIGH);
	data_s2po_db_p->set_flags(data_s2po_db_p, DB_DUP);
//	u_int32_t data_s2po_hff_size = (int)(data_s2po_db_p->get_pagesize - 32)/(int)(sizeof(id_type) + sizeof(POID) + 8);
//	data_s2po_db_p->set_h_ffactor(data_s2po_db_p, data_s2po_hff_size);
	/* Now open the persistent database for the dictionary */
	ret = data_s2po_db_p->open(data_s2po_db_p, /* Pointer to the database */
			0, /* Txn pointer */
			db_file, /* prog_db_file File name */
			data_s2po_db, /* Logical db name */
			db_type, /* Database type (using btree) */
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
			db_type, /* Database type (using btree) */
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
//	u_int32_t stat_s2po_hff_size = (int)(stat_s2po_db_p->get_pagesize - 32)/(int)(sizeof(id_type) + sizeof(id_type) + 8);
//	stat_s2po_db_p->set_h_ffactor(stat_s2po_db_p, stat_s2po_hff_size);

	/* Now open the persistent database for the dictionary */
	ret = stat_s2po_db_p->open(stat_s2po_db_p, /* Pointer to the database */
			0, /* Txn pointer */
			stat_file, /* prog_db_file File name */
			stat_s2po_db, /* Logical db name */
			db_type, /* Database type (using btree) */
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
			db_type, /* Database type (using btree) */
			flags, /* Open flags */
			0); /* File mode. Using defaults */

	if (ret != 0) {
		fprintf(stderr, "Error opening database: %s\n", db_strerror(ret));
		goto err;
	}
	if (DEBUG == 1) printf("Done.\n");

	return (ret);

	err:
	/* Close our database handle, if it was opened. */
	close_dbs();
	exit(0);
}


/*
 * Read multiple files from the given directory, one thread per file
 */

int load_dir_two_rounds(char *rdf_datadir, char *parser_type, int threads, int nfiles) {
	if (DEBUG == 1) printf("Start load_dir_nooverwrite.\n");
	struct timeval tvBegin, tvEnd, tvDiff;

	/* Set up global variables*/
	load_threads = threads;
	num_files = nfiles;
	if (threads > 1){
		tmp_dbs_enabled = 1;
	} else {
		tmp_dbs_enabled = 0;
	}

	DIR *d;
	struct dirent *entry;
	int i = 0, err, count = 0;
	void *status;
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
	init_load_dict_thread(threads);
	printf("Done generating the dictionary.\n");

	//	double size = get_db_size(dict_db_p);
	//	printf("Dictionary size: %d\n", (int) size);
	//	if (DEBUG == 1) print_db(dict_db_p, CHAR_P, ID_TYPE, 100);

	printf("Starting loading data ...\n");
	init_load_data_thread(threads);
	printf("Done loading data.\n");

	//	size = get_db_size(data_s2po_db_p);
	//	printf("Data db size: %d\n", (int) size);
	//	if (DEBUG == 1) print_db(data_s2po_db_p, ID_TYPE, PO_ID, 100);

	printf("Starting generating statistics ...\n");
	gen_stat_db(data_s2po_db_p, stat_s2po_db_p);
	printf("Done generating stat.\n");

	//	size = get_db_size(stat_s2po_db_p);
	//	printf("Stat_s2np_db size: %d\n", (int) size);
	//	if (DEBUG == 1) print_db(stat_s2po_db_p, ID_TYPE, ID_TYPE, 100);
	//
	printf("Starting generating the reverse dictionary ...\n ");
	gen_reverse_dict_db(dict_db_p, rdict_db_p);
	printf("Done generating the reverse dictionary.\n");


	//	size = get_db_size(rdict_db_p);
	//	printf("Reverse dictionary size: %d\n", (int) size);

	if (DEBUG == 1) print_db(rdict_db_p, ID_TYPE, CHAR_P, 100);
	/* Free the file array*/
	for (i = 0; i < num_files; i++){
		free(files[i]);
	}

	if (DEBUG == 1) printf("Done.\n");
	close_dbs();
	return (0);

}

int load_dir_one_round(char *rdf_datadir, char *parser_type, int threads, int nfiles) {
	int ret;

	/* Set up global variables*/
	load_threads = threads;
	num_files = nfiles;
	if (threads > 1){
		tmp_dbs_enabled = 1;
	} else {
		tmp_dbs_enabled = 0;
	}

	DIR *d;
	struct dirent *entry;
	int i = 0, err, count = 0;
	void *status;
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


	/**
	 * Initialize global variables for id_mappings_p and tmp_data_s2po_p
	 */

	long size;
	int print_size = 5;
	printf("Start loading...\n");

	ret = init_load_one_round_thread(threads);

	if (DEBUG == 1) {
		size = get_db_size(dict_db_p);
		printf("dict_db size: %ld\n", size);
		print_db(dict_db_p, CHAR_P, ID_TYPE, print_size);
	}

	if (DEBUG == 1){
		size = get_db_size(data_s2po_db_p);
		printf("data_s2po_db size: %ld\n", size);
		print_db(data_s2po_db_p, ID_TYPE, PO_ID, print_size);
	}

	if (DEBUG == 1)printf("Starting generating statistics ...\n");
	gen_stat_db(data_s2po_db_p, stat_s2po_db_p);
	if (DEBUG == 1)printf("Done generating stat.\n");

	if (DEBUG == 1){
		size = get_db_size(stat_s2po_db_p);
		printf("Stat_s2po_db size: %d\n", (int) size);
		if (DEBUG == 1) print_db(stat_s2po_db_p, ID_TYPE, ID_TYPE, print_size);
	}

	if (DEBUG == 1) printf("Starting generating the reverse dictionary ... \n");
	gen_reverse_dict_db(dict_db_p, rdict_db_p);
	if (DEBUG == 1)printf("Done generating the reverse dictionary.\n");


	if (DEBUG == 1){
		size = get_db_size(rdict_db_p);
		printf("Reverse dictionary size: %d\n", (int) size);
	}
	/* Free the file array*/
	for (i = 0; i < num_files; i++){
		free(files[i]);
	}

	printf("Finished loading. \nClosing databases and environments...\n");
	close_dbs();
	printf("Done.\n");

	return 1;
}

int load_dir_one_round_inmem(char *rdf_datadir, char *parser_type, int threads, int nfiles) {
	int ret;

	/* Set up global variables*/
	load_threads = threads;
	num_files = nfiles;
	if (threads > 1){
		tmp_dbs_enabled = 1;
	} else {
		tmp_dbs_enabled = 0;
	}

	filequeue = priq_new(0);
	DIR *d;
	struct dirent *entry;
	int i = 0, err, count = 0;
	void *status;
	char *files[num_files];
	d = opendir(rdf_datadir);
	i = 0;

	struct stat st;

	off_t filesize;
	    if (d) {
		while ((entry = readdir(d)) != NULL) {
			if (entry->d_type == DT_REG){
				char* file = malloc(strlen(entry->d_name) + strlen(rdf_datadir) + 2);
				strcpy(file, rdf_datadir);
				strcat(file, "/");
				strcat(file, entry->d_name);
				if (stat(file, &st) == 0)
					filesize = -1*st.st_size;

				priq_push(filequeue, (void*)file, filesize);
				//				strcpy(files[i], '\0');
				i++;
			}
		}
		closedir(d);
	}

	/**
	 * Initialize global variables for id_mappings_p and tmp_data_s2po_p
	 */

	long size, dictsize;
	int print_size = 5;
	printf("Start loading...\n");
	inmemdict = NULL;
	inmem_u2p = NULL;
	numns = 0;

	ret = init_load_one_round_inmem_thread(threads);
	serialize_prefixes((void*)(intptr_t)3);

	serialize_all();


	MYDICTIONARY *it, *itt;
	/* free the inmemdict contents */
	HASH_ITER(hh, inmemdict, it, itt) {
		free(it->term);
		HASH_DEL(inmemdict, it);
		free(it);
	}

//	if (DEBUG == 1){
//		size = get_db_size(data_s2po_db_p);
//		printf("data_s2po_db size: %ld\n", size);
//		print_db(data_s2po_db_p, ID_TYPE, PO_ID, print_size);
//	}

//	if (DEBUG == 1)printf("Starting generating statistics ...\n");
//	gen_stat_db(data_s2po_db_p, stat_s2po_db_p);
//	if (DEBUG == 1)printf("Done generating stat.\n");

//	if (DEBUG == 1){
//		size = get_db_size(stat_s2po_db_p);
//		printf("Stat_s2po_db size: %d\n", (int) size);
//		if (DEBUG == 1) print_db(stat_s2po_db_p, ID_TYPE, ID_TYPE, print_size);
//	}

//	if (DEBUG == 1) printf("Starting generating the reverse dictionary ... \n");
//	gen_reverse_dict_db(dict_db_p, rdict_db_p);
//	if (DEBUG == 1)printf("Done generating the reverse dictionary.\n");
//

//	if (DEBUG == 1){
//		size = get_db_size(rdict_db_p);
//		printf("Reverse dictionary size: %d\n", (int) size);
//	}

	printf("Finished loading. \nClosing databases and environments...\n");
	close_dbs();
	printf("Done.\n");

	return 1;
}

void serialize_all(){
	pthread_attr_t attr;
	/* Initialize and set thread detached attribute */
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	if (DEBUG == 1) printf("Start creating threads \n");

	pthread_t threadids[3];
	int err, i = 0;
	void *status;
	err = pthread_create(&(threadids[0]), &attr, &serialize_stat, (void *)(intptr_t)i);
	if (err != 0) printf("\ncan't create thread %d:[%s]", i, strerror(err));
	i++;
		err = pthread_create(&(threadids[1]), &attr, &serialize_dict, (void *)(intptr_t)i);
		if (err != 0) printf("\ncan't create thread %d:[%s]", i, strerror(err));
		i++;
		err = pthread_create(&(threadids[2]), &attr, &serialize_rdict, (void *)(intptr_t)i);
		if (err != 0) printf("\ncan't create thread %d:[%s]", i, strerror(err));
		i++;

//		err = pthread_create(&(threadids[3]), &attr, &serialize_prefixes, (void *)(intptr_t)i);
//		if (err != 0) printf("\ncan't create thread %d:[%s]", i, strerror(err));

	pthread_attr_destroy(&attr);

	if (DEBUG == 1) printf("Start joining threads \n");
	for (i = 0; i < 3; i++) {
		err = pthread_join(threadids[i], &status);
		if (err) {
			printf("ERROR; return code from pthread_join() is %d\n", err);
			exit(-1);
		}
	}

}

int resolve_ids(DB *in_p, DB *out_p, DB* mapping_p) {

	//	if (DEBUG == 1) printf("Start resolving ids\n");

	/* Setting up the BerkeleyDB parameters */
	DBT key, data, inkey, invalue, outkey, outvalue;
	int ret, ret_t;
	void *ptrk, *ptrd;
	u_int32_t flag;

	memset(&key, 0, sizeof(DBT));
	memset(&key, 0, sizeof(DBT));
	flag = DB_MULTIPLE_KEY;

	key.ulen = bulk_size * 1024 * 1024;
	key.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	key.data = malloc(key.ulen);
	memset(key.data, 0, key.ulen);

	data.ulen = 1 * 1024 * 1024;
	data.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	data.data = malloc(data.ulen);
	memset(data.data, 0, data.ulen);
	DB_MULTIPLE_WRITE_INIT(ptrk, &key);

	DBC *cursorp;

	/* Get the cursor */
	ret = in_p->cursor(in_p, 0, &cursorp, 0);
	if (ret != 0) {
		in_p->err(in_p, ret, "count_records: cursor open failed.");
		goto err;
	}

	POID* tmppo;
	long num = 0;

	memset(&inkey, 0, sizeof(DBT));
	memset(&invalue, 0, sizeof(DBT));

	int i = 0;
	// Read data item from temporary data_s2po_db
	while ((ret = cursorp->c_get(cursorp, &inkey, &invalue, DB_NEXT)) == 0){
		//		if (DEBUG == 1) printf("Got it: %d\n", i++);
		switch (ret) {
		case 0:

			// Resolve ids for all 3 s, p, o

			tmppo = (POID*) invalue.data;
			id_type tmps = *(id_type*) inkey.data;

			//					if (DEBUG == 1) {
			//						printf("Current data item: ");
			//						print_dbt((void*)inkey.data, ID_TYPE);
			//						printf("\t value: ");
			//						print_dbt((void*)invalue.data, PO_ID);
			//						printf("\n");
			//					}

			id_type s = lookup_stat(mapping_p, tmps);
			id_type p = lookup_stat(mapping_p, tmppo->predicate);
			id_type o = lookup_stat(mapping_p, tmppo->object);
			//			if (DEBUG == 1) {
			//				printf("id mapping lookup: ");
			//				printf(ID_TYPE_FORMAT, tmps);
			//				printf("\t is: ");
			//				printf(ID_TYPE_FORMAT, s);
			//				printf("\n");
			//			}
			//
			//			if (DEBUG == 1) {
			//				printf("id mapping lookup: ");
			//				printf(ID_TYPE_FORMAT, tmppo->predicate);
			//				printf("\t is: ");
			//				printf(ID_TYPE_FORMAT, p);
			//				printf("\n");
			//			}
			//			if (DEBUG == 1) {
			//				printf("id mapping lookup: ");
			//				printf(ID_TYPE_FORMAT, tmppo->object);
			//				printf("\t is: ");
			//				printf(ID_TYPE_FORMAT, o);
			//				printf("\n");
			//			}
			id_type news = (s > 0)?s:tmps;
			id_type newp = (p > 0)?p:tmppo->predicate;
			id_type newo = (o > 0)?o:tmppo->object;

			// Reconstruct the data item in the data_s2po_db
			tmppo->object = newo;
			tmppo->predicate = newp;

			//			if (DEBUG == 1) {
			//				printf("Construct data item: ");
			//				printf(ID_TYPE_FORMAT, news);
			//				printf("\t value: ");
			//				print_dbt((void*)tmppo, PO_ID);
			//				printf("\n");
			//			}

			DB_MULTIPLE_KEY_WRITE_NEXT(ptrk, &key, &news, sizeof(id_type), tmppo, sizeof(POID));
			assert(ptrk != NULL);
			num++;

			//			ret_t = out_p->get(out_p, 0, &outkey, &outvalue, DB_GET_BOTH);

			//			ret_t = out_p->put(out_p, 0, &outkey, &outvalue, 0);
			//			if (ret_t == DB_NOTFOUND){
			//			}

			if (num % UPDATES_PER_BULK_PUT == 0) {
				//					if (DEBUG == 1) printf("flush buffer to file\n");
				switch (ret = out_p->put(out_p, 0, &key, &data, flag)) {
				case 0:
					DB_MULTIPLE_WRITE_INIT(ptrk, &key);
					break;
				default:
					out_p->err(out_p, ret, "Bulk DB->put");
					goto err;
				}
			}

			break;
		case DB_NOTFOUND:
			break;
		default:
			out_p->err(out_p, ret, "Resolving ids in data_s2po_db_p unspecified error");
			goto err;
		}
	}

	if (num % UPDATES_PER_BULK_PUT != 0) {
		//					if (DEBUG == 1) printf("flush buffer to file\n");
		switch (ret = out_p->put(out_p, 0, &key, &data, flag)) {
		case 0:
			DB_MULTIPLE_WRITE_INIT(ptrk, &key);
			break;
		default:
			out_p->err(out_p, ret, "Bulk DB->put");
			goto err;
		}
	}
	return (ret);

	err: if (cursorp != NULL) {
		ret = cursorp->close(cursorp);
		if (ret != 0) {
			in_p->err(in_p, ret, "count_records: cursor close failed.");
		}
	}
	close_dbs();
}



DB* init_tmp_db(char *tmp_db_name, char* tmp_db_file, int dup){
	int ret;

	DB* tmp_db_p;
	ret = db_create(&tmp_db_p, env_home_p, 0);
	if (ret != 0) {
		fprintf(stderr, "Error creating database: %s\n", db_strerror(ret));
		goto err;
	}
	tmp_db_p->set_priority(tmp_db_p, DB_PRIORITY_VERY_HIGH);
	if (dup == 1){
		tmp_db_p->set_flags(tmp_db_p, DB_DUP);
	}


	//	if (DEBUG == 1) printf("Opening tmp database %s\n", tmp_db_name);
	/* Now open the persistent database for the dictionary */
	ret = tmp_db_p->open(tmp_db_p, /* Pointer to the database */
			0, /* Txn pointer */
			tmp_db_file, /* prog_db_file File name */
			tmp_db_name, /* Logical db name */
			DB_HASH, /* Database type (using btree) */
			DB_CREATE | DB_THREAD, /* Open flags */
			0); /* File mode. Using defaults */

	if (ret != 0) {
		fprintf(stderr, "Error opening database: %s\n", db_strerror(ret));
		goto err;
	}
	//	if (DEBUG == 1) printf("Done opening tmp database\n");

	return tmp_db_p;
	err:
	ret = tmp_db_p->close(tmp_db_p, 0);
	if (ret != 0) {
		fprintf(stderr, "%s database %s close failed.\n", db_strerror(ret), tmp_db_name);
		goto err;
	}
	u_int32_t tmp;
	ret = env_home_p->dbremove(env_home_p, 0, 0, tmp_db_name, 0);
	if (ret != 0) {
		fprintf(stderr, "%s database truncate failed.\n", db_strerror(ret));
		goto err;
	}
}

int remove_tmp_db(DB* tmp_db_p, char*tmp_db_name, char*tmp_db_file){
	int ret;
	if (tmp_db_p != NULL) {
		if (DEBUG == 1) printf("closing the temp db.\n", tmp_db_name);
		ret = tmp_db_p->close(tmp_db_p, 0);
		if (ret != 0) {
			fprintf(stderr, "%s database %s close failed.\n", db_strerror(ret), tmp_db_name);
		}
		if (DEBUG == 1) printf("Thread %s: removing the temp db.\n", tmp_db_name);
		u_int32_t tmp;
		ret = env_home_p->dbremove(env_home_p, 0, tmp_db_file, tmp_db_name, 0);
		if (ret != 0) {
			fprintf(stderr, "%s database %struncate failed.\n", db_strerror(ret), tmp_db_name);
		}
	}
	return ret;

}

//struct my_struct {
//    int id;                    /* key */
//    char name[10];
//    UT_hash_handle hh;         /* makes this structure hashable */
//};

int extract_triples(char *linebuff, MYPREFIXES *myprefixes, char** res_p, int tid){
	long start_term_idx = 0, end_term_idx = 0, last_non_blank = 0, fi = 0;
	int i = 0, spo = 0, line = 0, term_started = 0, found = 0, literal = 0, bnode = 0, prefix_line = 0;
	for (i = 0; i < strlen(linebuff); i++) {
		/*
		 * Identify the term from start_term to end_term
		 * If the cur pointer is within subject, predicate, or object
		 */
		if (isblank(linebuff[i])){
			//						if (DEBUG == 1) printf("Thread %d: char %c at %d is blank \n", tid, linebuff[i], i);
			/*
			 * Reach blank char. Multiple possibilities:
			 * 1. If the term has been started:
			 * 		1.1 If the term is subject/predicate:
			 *			The term is ended at p[i-1]. => Found the term.
			 * 2. If the term has not been started, do nothing.
			 */
			if (term_started == 1){
//				if (DEBUG == 1) printf("Thread %d: at spo %d \t %d \n", tid, spo, literal);

				if ((spo != 2) || ((spo == 2) && (literal == 0 || literal == 2 || bnode == 1))){
					end_term_idx = i - 1;
					//								if (DEBUG == 1) printf("Thread %d: from %d to %d with spo %d literal %d\n", tid, (long)start_term_idx, (long)end_term_idx, spo, literal);

					/* Extract the found term */
					/* Extract the found term */
					char *term = extract_term(linebuff, start_term_idx, end_term_idx);
					if (strcmp(term, "@base") == 0 || strcmp(term, "@prefix") == 0){
						prefix_line = 1;
					}
					res_p[spo] = get_full_form_term(myprefixes, term, prefix_line);
//					if (DEBUG == 1) printf("Thread %d: Found spo %d \t %s \n", tid, spo, term);
//					if (DEBUG == 1) printf("Thread %d: Found full spo %d \t %s \n", tid, spo, res_p[spo]);
					free(term);

					//								if (DEBUG == 1) printf("Thread %d: Found spo %d\t %s \n", tid, spo, res_p[spo]);

					/* Reset the variables for the next term*/
					spo++;
					start_term_idx = 0;
					end_term_idx = 0;
					literal = 0;
					term_started = 0;
					bnode = 0;
				}
				if (literal > 2){
//					if (DEBUG == 1) printf("Thread %d: at spo %d \t %d \n", tid, spo, literal);
					return -1;
				}

			}
		} else {
			/* Start a new term if p[i] is the first non-blank char*/
			if (term_started == 0 && linebuff[i] != '.'){
				//							if (DEBUG == 1) printf("spo %d start at %d with %c\n", spo, (int)i, linebuff[i]);
				term_started = 1;
				start_term_idx = i;
				// Identify the literal for object
			}
			if (linebuff[i] == '"' && (isblank(linebuff[i-1]) || isblank(linebuff[i+1]) || ((linebuff[i+1] == '^') && (linebuff[i+2] == '^')) || (linebuff[i+1] == '@') )){
				literal++;
//				if (DEBUG == 1) printf("Thread %d: double quote at pos %d of spo %d \t %d \n", tid, i, spo, literal);
			}
			if (linebuff[i] == ':' && linebuff[i-1] == '_'){
				bnode = 1;
				term_started = 1;
				start_term_idx = i - 1;
			}
		}
	} // Finish identifying the resources in the line
	// Update the counters

//	for (i = 0; i < 3; i++){
//		if (res_p[i] != NULL){
//			if (DEBUG == 1) printf("res_p[%d]=%s\n", i, res_p[i]);
//		}
//	}
//	spo = 0;
//	line++;
//	start_term_idx = 0;
//	end_term_idx = 0;
//	literal = 0;
//	term_started = 0;
//	bnode = 0;
//	prefix_line = 0;

	return prefix_line;
}

void* load_one_round_inmem_pthread(void* thread_id){
	// Load data file into line buffer
	// Create two inmemory temporary maps of dictionary and data
	// Resolve the id in idmap and generate the
	int tid = (intptr_t)thread_id;
	int file;
	struct timeval tvBegin, tvEnd, tvDiff;
	if (DEBUG == 1) printf("Thread %d with ID %ld started\n", tid, (long)pthread_self());

	DB *tmp_db_p, *tmp_data_p;
	char tid_str[5];
	sprintf(tid_str, "%d", tid);

	tmp_data_p = init_tmp_db(tid_str, 0, 1);
	id_type node_id = tid * 10000000000 + 2;
	id_type node_id_odd = node_id + 1;
	id_type triple_id = (tid == 0)?1:tid*10000000000;

	if (DEBUG == 1) printf("Triple's ID starts at ");
	if (DEBUG == 1) printf(ID_TYPE_FORMAT, triple_id);
	if (DEBUG == 1) printf("URI's ID starts at ");
	if (DEBUG == 1) printf(ID_TYPE_FORMAT, node_id);
	if (DEBUG == 1) printf("\t Literal's ID starts at ");
	if (DEBUG == 1) printf(ID_TYPE_FORMAT, node_id_odd);
	if (DEBUG == 1) printf("\n");

	FILE *ifp;
	size_t cur_size;
	long id;
	char *rdf_dfile;

	struct timeval start, end, diff;


//	for (file = tid; file < num_files; file = file + load_threads);
	int stop = 0;
	id_type pri;
	while (stop == 0){
		if (DEBUG == 1) printf("Thread %d: Fetching file ...\n", tid);
		int fetch_file = 0;
		while (fetch_file == 0){
			(void)mutex_lock(&fetch_file_lock);
			rdf_dfile = (char*)priq_pop(filequeue, &pri);
			fetch_file = 1;
			(void)mutex_unlock(&fetch_file_lock);
		}

		if (rdf_dfile == NULL) {
			stop = 1;
			if (DEBUG == 1) printf("Thread %d: no more file to process. Done.\n", tid);
			break;
		}
		gettimeofday(&tvBegin, NULL);

		gettimeofday(&start, NULL);
		/*
		 * Read the content from the rdf_file and put RDF resources into the dictdb.
		 */
		ifp = fopen(rdf_dfile, "r");
		if (ifp == NULL) {
			fprintf(stderr, "Error opening file '%s'\n", rdf_dfile);
			exit (-1);
		}

		/*
		 * Create a cursor for the dictionary database
		 */
		if (DEBUG == 1) printf("Start loading file %s ...\n", rdf_dfile);


		size_t linesize = 10000;
		char *linebuff = NULL;
		ssize_t linelen = 0;
		char *ptr = NULL;
		int line = 0;


		MYPREFIXES *myprefixes = NULL;
		MYDICTIONARY *mydicttmp = NULL;
		MYTRIPLES *mytriples = NULL;

		/* Iterate over the input RDF file*/
		while ((linelen = getline(&linebuff, &linesize, ifp)) > 0) {
			line++;
			if (linebuff[0] == '#') {
				// Skip the comment line
			} else if (linelen > 2){
				char **res_p = malloc(3*sizeof(char*));

//				if (DEBUG == 1) printf("Start processing line %s\n", linebuff);
				//			if (DEBUG == 1) printf("Start processing line %d of len %d and of size %d...\n", line, linelen, linesize);
//				if (DEBUG == 1) printf("%d:Line: %d %s\n", tid, line, linebuff);
				int prefix_line = extract_triples(linebuff, myprefixes, res_p, tid);

				if (prefix_line == 1 && (strcmp(res_p[0], "@base") == 0)){
					//					if (DEBUG == 1) printf("Thread %d: %s:\t%s\t%s\t\n", tid, res_p[0], res_p[1]);
					prefix_line = 1;
					// Resolve the URI
					if (DEBUG == 1) printf("Getting the base %s\n", res_p[1]);
					MYPREFIXES *p2u_p = (MYPREFIXES*)malloc(sizeof(MYPREFIXES));
					p2u_p->prefix = res_p[0];
					p2u_p->uri = res_p[1];
					HASH_ADD_STR(myprefixes, prefix, p2u_p );
				} else if (prefix_line == 1 && (strcmp(res_p[0], "@prefix") == 0)){
					//					if (DEBUG == 1) printf("Thread %d: %s:\t%s\t%s\t\n", tid, res_p[0], res_p[1], res_p[2]);
					prefix_line = 1;
					if (DEBUG == 1) printf("Getting the prefix %s as %s\n", res_p[1], res_p[2]);
					MYPREFIXES *existing = NULL;
					HASH_FIND_STR(inmem_u2p, res_p[2], existing);

					MYPREFIXES *p2u_p = (MYPREFIXES*)malloc(sizeof(MYPREFIXES));
					p2u_p->prefix = res_p[1];
					p2u_p->uri = res_p[2];
					HASH_ADD_STR(myprefixes, prefix, p2u_p );
					free(res_p[0]);
//					(void)mutex_unlock(&update_prefix_ns_lock);
				} else if (prefix_line == 0){
					// Processing the dictionary and indexes
//					if (DEBUG == 1) printf("Getting normal triple\n");
					// Processing the dictionary and indexes
//					if (DEBUG == 1) printf("Thread %d: %s:\n%s\n%s\n", tid, res_p[0], res_p[1], res_p[2]);

					id_type res_id[3];
					int i = 0;
					for (i = 0; i < 3; i++) {
						if (res_p[i] != NULL){
//							char *shortform = get_short_form_term(res_p[i]);

							MYDICTIONARY *dicttmp = NULL;
							HASH_FIND_STR(mydicttmp, res_p[i], dicttmp);
							if(dicttmp == NULL){
								if (strstr(res_p[i], "\"") != NULL){
									res_id[i] = node_id_odd;
								} else {
									res_id[i] = node_id;
								}
								MYDICTIONARY *dictitem = (MYDICTIONARY*) malloc(sizeof(MYDICTIONARY));
								// Add URI into the tmpdict;

								dictitem->term = malloc(strlen(res_p[i]) + 1);
								strcpy(dictitem->term, res_p[i]);
								dictitem->id = res_id[i];
//								if (DEBUG == 1) printf("going to put this term to temp dict %s\n", dictitem->term);
								HASH_ADD_STR(mydicttmp, term, dictitem );
//								if (DEBUG == 1) printf("putting this item to temp dict %s\n", dictitem->term);
								if (strstr(res_p[i], "\"") != NULL){
									node_id_odd = node_id_odd + 2;
								} else {
									node_id = node_id + 2;
								}
							} else {
								res_id[i] = dicttmp->id;
							}
							free(res_p[i]);
						}
					}

					MYTRIPLES *triple = (MYTRIPLES*)malloc(sizeof(MYTRIPLES));
					triple->subject = res_id[0];
					triple->predicate = res_id[1];
					triple->object = res_id[2];
					triple->tripleid = tripleid;
					tripleid++;
//					SPOID *idtriple = malloc(sizeof(SPOID));
//					idtriple->subject = res_id[0];
//					idtriple->predicate = res_id[1];
//					idtriple->object = res_id[2];
//					triple->triple = idtriple;
					// Add triple into the triple list;
					//					if (DEBUG == 1) printf("going to add %s\n", res_p[i]);
					HASH_ADD_PTR(mytriples, tripleid, triple);
					//					if (DEBUG == 1) printf("added %s\n", res_p[i]);
					// Getting the id from the main dict
//					printf("Thread %d insert data triple:", tid);
//					print_dbt((void*)triple->triple, SPO_ID);
//					printf("\n");
				}
				free(res_p);
			}

		}

		free(linebuff);
		linebuff = NULL;
		/* Close the file */
		fclose(ifp);
		int print_size = 100;
		gettimeofday(&end, NULL);
		timeval_subtract(&diff, &end, &start);
		if (DEBUG == 1) printf("Finish parsing file %s: %ld.%06ld sec.\n", rdf_dfile, (long int)diff.tv_sec, (long int)diff.tv_usec);


		/* Start merging temporary dictionary with final dict
		 * and generate mapping from tmp id to final id for dict item */
		/* Resolving id for temporary data */

		MYDICTIONARY *d;

//		for(d = mydicttmp; d != NULL; d=d->hh.next) {
//			printf("%d Dict temp item %s with id ", tid, d->term);
//			printf(ID_TYPE_FORMAT, d->id);
//			printf("\n");
//		}
		MYPREFIXES *p, *pt;
		HASH_ITER(hh, myprefixes, p, pt) {
			HASH_DEL(myprefixes, p);
			free(p->prefix);
			free(p->uri);
			free(p);
		}

		int done_merge_dict = 0, done_merge_data = 0, done_resolve_ids = 0;
		MYMAPPING *mappings;
		MYTRIPLES *newdata;
		long datasize;
		while (done_merge_dict == 0){

			if (done_merge_dict == 0){
				(void)mutex_lock(&write_dict_db_lock);
				if (DEBUG == 1) printf("Thread %d got the merge dict lock\n", tid);
				gettimeofday(&start, NULL);
				if (DEBUG == 1) timeval_print(&start);

				mappings = merging_ids_inmem(mydicttmp, tid);
				done_merge_dict = 1;

				gettimeofday(&end, NULL);
				timeval_subtract(&diff, &end, &start);
				if (DEBUG == 1) timeval_print(&end);
				if (DEBUG == 1) printf("Thread %d finished merging dict in %s: %ld.%06ld sec.\n", tid, rdf_dfile, (long int)diff.tv_sec, (long int)diff.tv_usec);
				if (DEBUG == 1) printf("Thread %d released the merge dict lock\n", tid);
				(void)mutex_unlock(&write_dict_db_lock);
			}
		}
		if (DEBUG == 1) printf("Thread %d starts resolving ids\n", tid);
		gettimeofday(&start, NULL);
		if (DEBUG == 1) timeval_print(&start);

		mytriples = resolve_ids_inmem(mappings, mytriples, tid);
		done_resolve_ids = 1;

		gettimeofday(&end, NULL);
		timeval_subtract(&diff, &end, &start);
		if (DEBUG == 1) timeval_print(&end);
		if (DEBUG == 1) printf("Thread %d finished resolving ids in %s: %ld.%06ld sec.\n", tid, rdf_dfile, (long int)diff.tv_sec, (long int)diff.tv_usec);

		gettimeofday(&start, NULL);
		if (DEBUG == 1) timeval_print(&start);

		done_merge_data = 0;
		while (done_merge_data == 0){

			if (done_merge_data == 0){
				(void)mutex_lock(&write_data_s2po_db_lock);
				if (DEBUG == 1) printf("Thread %d got the merge data lock\n", tid);
				gettimeofday(&start, NULL);
				if (DEBUG == 1) timeval_print(&start);

				datasize = serialize_data(mytriples, data_s2po_db_p, tid);
				done_merge_data = 1;

				gettimeofday(&end, NULL);
				timeval_subtract(&diff, &end, &start);
				if (DEBUG == 1) timeval_print(&end);
				if (DEBUG == 1) printf("Thread %d finished merging data in %s: %ld.%06ld sec.\n", tid, rdf_dfile, (long int)diff.tv_sec, (long int)diff.tv_usec);
				if (DEBUG == 1) printf("Thread %d released the merge data lock\n", tid);
				(void)mutex_unlock(&write_data_s2po_db_lock);
			}
		}
//		datasize = serialize_data(newdata, tmp_data_p, tid);
//		done_merge_data = 1;

		gettimeofday(&end, NULL);
		timeval_subtract(&diff, &end, &start);
		if (DEBUG == 1) timeval_print(&end);
		if (DEBUG == 1) printf("Thread %d finished merging data in %s: %ld.%06ld sec.\n", tid, rdf_dfile, (long int)diff.tv_sec, (long int)diff.tv_usec);

		gettimeofday(&end, NULL);
		timeval_subtract(&diff, &end, &tvBegin);
		if (DEBUG == 1) printf("This thread finished loading %ld from file %s: %ld.%06ld sec.\n",datasize, rdf_dfile, (long int)diff.tv_sec, (long int)diff.tv_usec);
//		printf("Thread %d freeing myprefixes\n", tid);
		/* free the myprefixes contents */
		free(rdf_dfile);
	}
//	int done_merge_data = 0;
//	while (done_merge_data == 0){
//
//		if (done_merge_data == 0){
//			(void)mutex_lock(&write_data_s2po_db_lock);
//			if (DEBUG == 1) printf("Thread %d got the merge data lock\n", tid);
//			gettimeofday(&start, NULL);
//			if (DEBUG == 1) timeval_print(&start);
//
//			merge_dbs(tmp_data_p, data_s2po_db_p);
//			done_merge_data = 1;
//
//			gettimeofday(&end, NULL);
//			timeval_subtract(&diff, &end, &start);
//			if (DEBUG == 1) timeval_print(&end);
//			if (DEBUG == 1) printf("Thread %d finished merging data in %s: %ld.%06ld sec.\n", tid, rdf_dfile, (long int)diff.tv_sec, (long int)diff.tv_usec);
//			if (DEBUG == 1) printf("Thread %d released the merge data lock\n", tid);
//			(void)mutex_unlock(&write_data_s2po_db_lock);
//		}
//	}
	remove_tmp_db(tmp_data_p, tid_str, 0);
	return 0;
	err:
	printf("error");
}

MYTRIPLES* resolve_ids_inmem(MYMAPPING *mymapping, MYTRIPLES *mytmpdata, int tid){
	MYTRIPLES *d, *newdata = NULL;
	MYMAPPING *m, *mtmp;

	for(d = mytmpdata; d != NULL; d=d->hh.next) {
		//		if (DEBUG == 1) {
		//			printf("Reading item %s with id ", d->term);
		//			printf(ID_TYPE_FORMAT, d->id);
		//			printf("\n ");
		//		}
		// Getting the id from the main dict

//		SPOID *idtriple = d->triple;
//
//		id_type subject = idtriple->subject;
//		id_type predicate = idtriple->predicate;
//		id_type object = idtriple->object;


		MYMAPPING *ms = NULL, *mp = NULL, *mo = NULL;
		HASH_FIND_PTR(mymapping, &(d->subject), ms);
		HASH_FIND_PTR(mymapping, &(d->predicate), mp);
		HASH_FIND_PTR(mymapping, &(d->object), mo);

		// Add this triple to db.


		MYTRIPLES *tmp, *newitem = (MYTRIPLES*) malloc(sizeof(MYTRIPLES));
		newitem->subject = (ms == NULL)?d->subject:ms->target;
		newitem->predicate = (mp == NULL)?d->predicate:mp->target;
		newitem->object = (mo == NULL)?d->object:mo->target;
		newitem->tripleid = d->tripleid;
		HASH_ADD_PTR(newdata, tripleid, newitem);
	}

//	printf("Thread %d freeing mydicttmp\n", tid);
	MYMAPPING *it, *itt;
	/* free the mymapping contents */
	HASH_ITER(hh, mymapping, it, itt) {
		HASH_DEL(mymapping, it);
		free(it);
	}
	MYTRIPLES *t, *tt;
	/* free the mymapping contents */
	HASH_ITER(hh, mytmpdata, t, tt) {
		HASH_DEL(mytmpdata, t);
		free(t);
	}
	return newdata;
}

MYMAPPING *merging_ids_inmem(MYDICTIONARY *mydicttmp, int tid){
	MYDICTIONARY *d;
	MYMAPPING *mymapping = NULL, *m, *mtmp;

	for(d = mydicttmp; d != NULL; d=d->hh.next) {
//				if (DEBUG == 1) {
//					printf("Reading item %s with id ", d->term);
//					printf(ID_TYPE_FORMAT, d->id);
//					printf("\n ");
//				}
		// Getting the id from the main dict
		MYDICTIONARY *dictitem = NULL;
		HASH_FIND_STR(inmemdict, d->term, dictitem);
		if (dictitem != NULL){
			d->target = dictitem->id;
			m = (MYMAPPING*) malloc(sizeof(MYMAPPING));
			m->source = d->id;
			m->target = dictitem->id;
//			if (DEBUG == 1) {
//				printf("thread %d insert mapping from ", tid);
//				printf(ID_TYPE_FORMAT, m->source);
//				printf(" to ");
//				printf(ID_TYPE_FORMAT, m->target);
//				printf("\n ");
//			}
			HASH_ADD_PTR(mymapping, source, m );
		} else {
			dictitem = (MYDICTIONARY*) malloc(sizeof(MYDICTIONARY));

			dictitem->term = malloc(strlen(d->term) + 1);
			strcpy(dictitem->term, d->term);
			dictitem->id = d->id;
			HASH_ADD_STR(inmemdict, term, dictitem);
//			if (DEBUG == 1) {
//				printf("thread %d insert item %s with id \n", tid, dictitem->term);
//				printf(ID_TYPE_FORMAT, dictitem->id);
//				printf("\n ");
//			}
		}
	}
//	printf("Thread %d freeing mydicttmp\n", tid);
	MYDICTIONARY *it, *itt;
	/* free the mymapping contents */
	HASH_ITER(hh, mydicttmp, it, itt) {
		free(it->term);
		HASH_DEL(mydicttmp, it);
		free(it);
	}
	return mymapping;
}

void* serialize_dict(void* threadid){

	int tid = (intptr_t) threadid;
	if (DEBUG == 1) printf("Thread %d started serializing dictionary to file\n", tid);

	struct timeval start, end, diff;
	gettimeofday(&start, NULL);
	/* Setting up the BerkeleyDB parameters */
	DBT key, value, rkey, rvalue;
	int ret, ret_t;
	void *ptrk, *ptrrk;
	u_int32_t flag;

	flag = DB_MULTIPLE_KEY;

	key.ulen = bulk_size * 1024 * 1024;
	key.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	key.data = malloc(key.ulen);
	memset(key.data, 0, key.ulen);

	value.ulen = 1024 * 1024;
	value.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	value.data = malloc(value.ulen);
	memset(value.data, 0, value.ulen);
	DB_MULTIPLE_WRITE_INIT(ptrk, &key);

//	rkey.ulen = bulk_size * 1024 * 1024;
//	rkey.flags = DB_DBT_USERMEM | DB_DBT_BULK;
//	rkey.data = malloc(rkey.ulen);
//	memset(rkey.data, 0, rkey.ulen);
//
//	rvalue.ulen = 1024 * 1024;
//	rvalue.flags = DB_DBT_USERMEM | DB_DBT_BULK;
//	rvalue.data = malloc(rvalue.ulen);
//	memset(rvalue.data, 0, rvalue.ulen);
//	DB_MULTIPLE_WRITE_INIT(ptrrk, &rkey);
	/* Put temporary data into the final data_s2po db */

	MYDICTIONARY *dtmp, *d, *mydicttmp = inmemdict;
	long num = 0;

	for(d = mydicttmp; d != NULL; d=d->hh.next) {
		// Getting the id from the main dict
//
//						printf("thread %d insert data item for key: %s and value ", tid, d->term);
//						printf(ID_TYPE_FORMAT, d->id);
//						printf("\n");

		//				ret = put_data_item(tmp_data_p, res_id[0], res_id[1], res_id[2]);
		if (d->id % 2 == 0){
			char *term = get_short_form_term(d->term);
//			char *term = d->term;
			if (term != NULL){

				id_type id = d->id;
				DB_MULTIPLE_KEY_WRITE_NEXT(ptrk, &key, term, strlen(term) + 1, &id, sizeof(id_type));
	//			DB_MULTIPLE_KEY_WRITE_NEXT(ptrrk, &rkey, &id, sizeof(id_type), term, strlen(term) + 1);
				free(term);
				assert(ptrk != NULL);
				num++;
		//		if (DEBUG == 1) printf("thread %d asserted ptr\n", tid);
				if (num % UPDATES_PER_BULK_PUT == 0) {
		//		if (DEBUG == 1) printf("thread %d flush buffer to file\n", tid);
					switch (ret = dict_db_p->put(dict_db_p, 0, &key, &value, flag)) {
					case 0:
						DB_MULTIPLE_WRITE_INIT(ptrk, &key);
						break;
					default:
						dict_db_p->err(dict_db_p, ret, "Bulk DB->put");
						goto err;
					}
				}
	//			if (num % UPDATES_PER_BULK_PUT == 0) {
	//	//								if (DEBUG == 1) printf("thread %d flush buffer to file\n", tid);
	//				switch (ret = rdict_db_p->put(rdict_db_p, 0, &rkey, &rvalue, flag)) {
	//				case 0:
	//					DB_MULTIPLE_WRITE_INIT(ptrrk, &rkey);
	//					break;
	//				default:
	//					rdict_db_p->err(rdict_db_p, ret, "Bulk DB->put");
	//					goto err;
	//				}
	//			}
			}

		}
	}

	if (num % UPDATES_PER_BULK_PUT != 0) {
		//					if (DEBUG == 1) printf("flush buffer to file\n");
		switch (ret = dict_db_p->put(dict_db_p, 0, &key, &value, flag)) {
		case 0:
			DB_MULTIPLE_WRITE_INIT(ptrk, &key);
			break;
		default:
			dict_db_p->err(dict_db_p, ret, "Bulk DB->put");
			goto err;
		}
	}
//	if (num % UPDATES_PER_BULK_PUT != 0) {
////								if (DEBUG == 1) printf("thread %d flush buffer to file\n", tid);
//		switch (ret = rdict_db_p->put(rdict_db_p, 0, &rkey, &rvalue, flag)) {
//		case 0:
//			DB_MULTIPLE_WRITE_INIT(ptrrk, &rkey);
//			break;
//		default:
//			rdict_db_p->err(rdict_db_p, ret, "Bulk DB->put");
//			goto err;
//		}
//	}


//	printf("Dictionary size: %ld", num);
	free(key.data);
	free(value.data);
	gettimeofday(&end, NULL);
	timeval_subtract(&diff, &end, &start);
	if (DEBUG == 1) printf("Thread %d finished serializing dict in: %ld.%06ld sec.\n", tid, (long int)diff.tv_sec, (long int)diff.tv_usec);

	return num;
	err:
	printf("error in serializing data to db");
	exit(0);
}

void* serialize_prefixes(void* threadid){

	int tid = (intptr_t) threadid;
	if (DEBUG == 1) printf("Thread %d started serializing prefixes to file\n", tid);

	struct timeval start, end, diff;
	gettimeofday(&start, NULL);
	/* Setting up the BerkeleyDB parameters */
	DBT key, value, rkey, rvalue;
	int ret, ret_t;
	void *ptrk;
	u_int32_t flag;

	flag = DB_MULTIPLE_KEY;

	key.ulen = bulk_size * 1024 * 1024;
	key.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	key.data = malloc(key.ulen);
	memset(key.data, 0, key.ulen);

	value.ulen = 1024 * 1024;
	value.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	value.data = malloc(value.ulen);
	memset(value.data, 0, value.ulen);
	DB_MULTIPLE_WRITE_INIT(ptrk, &key);

	/* Generate the prefixes from the dictionary*/
	MYDICTIONARY *item, *newitem, *newshortendict = NULL, *tmp;

	for(item = inmemdict; item != NULL; item=item->hh.next) {
		char* shorten = get_short_form_term(item->term);
		newitem = (MYDICTIONARY*) malloc(sizeof(MYDICTIONARY));
		newitem->id = item->id;
		newitem->term = shorten;
		HASH_ADD_STR(newshortendict, term, newitem);
		free(item->term);
	}

	MYDICTIONARY *it, *itt;
	/* free the mymapping contents */
	HASH_ITER(hh, inmemdict, it, itt) {
		HASH_DEL(inmemdict, it);
		free(it);
	}

	/* Reset the main dict pointer */
	inmemdict = newshortendict;


	/* Put temporary data into the final data_s2po db */

	MYPREFIXES *dtmp, *d;
	long num = 0;

	for(d = inmem_u2p; d != NULL; d=d->hh.next) {
		// Getting the id from the main dict

//		printf("thread %d insert data item for prefix: %s and uri %s", tid, d->prefix, d->uri);

		//				ret = put_data_item(tmp_data_p, res_id[0], res_id[1], res_id[2]);

		DB_MULTIPLE_KEY_WRITE_NEXT(ptrk, &key, d->uri, strlen(d->uri) + 1, d->prefix, strlen(d->prefix) + 1);
		DB_MULTIPLE_KEY_WRITE_NEXT(ptrk, &key, d->prefix, strlen(d->prefix) + 1, d->uri, strlen(d->uri) + 1);
//		if (DEBUG == 1) printf("thread %d asserted ptr\n", tid);
		assert(ptrk != NULL);
		num++;
		num++;
		if (num % UPDATES_PER_BULK_PUT == 0) {
//		if (DEBUG == 1) printf("thread %d flush buffer to file\n", tid);
			switch (ret = dict_prefixes_db_p->put(dict_prefixes_db_p, 0, &key, &value, flag)) {
			case 0:
				DB_MULTIPLE_WRITE_INIT(ptrk, &key);
				break;
			default:
				dict_prefixes_db_p->err(dict_prefixes_db_p, ret, "Bulk DB->put");
				goto err;
			}
		}
	}

	if (num % UPDATES_PER_BULK_PUT != 0) {
//							if (DEBUG == 1) printf("flush buffer to file\n");
		switch (ret = dict_prefixes_db_p->put(dict_prefixes_db_p, 0, &key, &value, flag)) {
		case 0:
			DB_MULTIPLE_WRITE_INIT(ptrk, &key);
			break;
		default:
			dict_prefixes_db_p->err(dict_prefixes_db_p, ret, "Bulk DB->put");
			goto err;
		}
	}

//	printf("Dictionary size: %ld", num);
	free(key.data);
	free(value.data);
	gettimeofday(&end, NULL);
	timeval_subtract(&diff, &end, &start);
	if (DEBUG == 1) printf("Thread %d finished serializing prefixes in: %ld.%06ld sec.\n", tid, (long int)diff.tv_sec, (long int)diff.tv_usec);

	return num;
	err:
	printf("error in serializing data to db");
	exit(0);
}

void* serialize_rdict(void *threadid){
	int tid = (intptr_t) threadid;
	if (DEBUG == 1) printf("Thread %d started serializing reverse dictionary to file\n", tid);
	struct timeval start, end, diff;
	gettimeofday(&start, NULL);
	/* Setting up the BerkeleyDB parameters */
	DBT key, value, rkey, rvalue;
	int ret, ret_t;
	void *ptrrk;
	u_int32_t flag;

	flag = DB_MULTIPLE_KEY;

	rkey.ulen = bulk_size * 1024 * 1024;
	rkey.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	rkey.data = malloc(rkey.ulen);
	memset(rkey.data, 0, rkey.ulen);

	rvalue.ulen = 1024 * 1024;
	rvalue.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	rvalue.data = malloc(rvalue.ulen);
	memset(rvalue.data, 0, rvalue.ulen);
	DB_MULTIPLE_WRITE_INIT(ptrrk, &rkey);

	/* Put temporary data into the final data_s2po db */

	MYDICTIONARY *dtmp, *d, *mydicttmp = inmemdict;
	long num = 0;

	for(d = mydicttmp; d != NULL; d=d->hh.next) {
		// Getting the id from the main dict

//						printf("thread %d insert data key: ", tid);
//						printf(ID_TYPE_FORMAT,  d->id);
//						printf("\t for value : %s \n", d->term);

		//				ret = put_data_item(tmp_data_p, res_id[0], res_id[1], res_id[2]);
//		char *term = d->term;
		char *term = get_short_form_term(d->term);
		if (term != NULL){

			id_type id = d->id;
			DB_MULTIPLE_KEY_WRITE_NEXT(ptrrk, &rkey, &id, sizeof(id_type), term, strlen(term) + 1);
			free(term);
			assert(ptrrk != NULL);
			num++;
			if (num % UPDATES_PER_BULK_PUT == 0) {
	//								if (DEBUG == 1) printf("thread %d flush buffer to file\n", tid);
				switch (ret = rdict_db_p->put(rdict_db_p, 0, &rkey, &rvalue, flag)) {
				case 0:
					DB_MULTIPLE_WRITE_INIT(ptrrk, &rkey);
					break;
				default:
					rdict_db_p->err(rdict_db_p, ret, "Bulk DB->put");
					goto err;
				}
			}
		}
	}

	if (num % UPDATES_PER_BULK_PUT != 0) {
		//					if (DEBUG == 1) printf("flush buffer to file\n");
		switch (ret = rdict_db_p->put(rdict_db_p, 0, &rkey, &rvalue, flag)) {
		case 0:
			DB_MULTIPLE_WRITE_INIT(ptrrk, &rkey);
			break;
		default:
			rdict_db_p->err(rdict_db_p, ret, "Bulk DB->put");
			goto err;
		}
	}

//	printf("Dictionary size: %ld", num);
	free(rkey.data);
	free(rvalue.data);
	gettimeofday(&end, NULL);
	timeval_subtract(&diff, &end, &start);
	if (DEBUG == 1) printf("Thread %d finished serializing rdict in: %ld.%06ld sec.\n", tid, (long int)diff.tv_sec, (long int)diff.tv_usec);
	return num;
	err:
	printf("error in serializing data to db");
	exit(0);
}

void* serialize_stat(void* threadid){
	int tid = (intptr_t) threadid;
	if (DEBUG == 1) printf("Thread %d started serializing stat db to file\n", tid);
	struct timeval start, end, diff;
	gettimeofday(&start, NULL);

	gen_stat_db(data_s2po_db_p, stat_s2po_db_p);

	gettimeofday(&end, NULL);
	timeval_subtract(&diff, &end, &start);
	if (DEBUG == 1) printf("Thread %d finished serializing stat db in: %ld.%06ld sec.\n", tid, (long int)diff.tv_sec, (long int)diff.tv_usec);
}

long serialize_data(MYTRIPLES *mytriples, DB* dbp, int tid){
	/* Setting up the BerkeleyDB parameters */
	DBT key, value;
	int ret = 0, ret_t;
	void *ptrk, *ptrd;
	u_int32_t flag;

	flag = DB_MULTIPLE;

	memset(&key, 0, sizeof(DBT));
	memset(&value, 0, sizeof(DBT));
	key.ulen = bulk_size * 1024 * 1024;
	key.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	key.data = malloc(key.ulen);
	memset(key.data, 0, key.ulen);

	value.ulen = bulk_size * 1024 * 1024;
	value.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	value.data = malloc(value.ulen);
	memset(value.data, 0, value.ulen);
	DB_MULTIPLE_WRITE_INIT(ptrk, &key);
	DB_MULTIPLE_WRITE_INIT(ptrd, &value);

	/* Put temporary data into the final data_s2po db */

	MYTRIPLES *triple;
	id_type subject;
	long nums2po = 0;
	SPOID *idtriple;
	for(triple = mytriples; triple != NULL; triple=triple->hh.next) {

//		idtriple = triple->triple;
		// Add this triple to db.
		POID *tmppo = malloc(sizeof(POID));
		tmppo->predicate = triple->predicate;
		tmppo->object = triple->object;
		subject = triple->subject;

		//				ret = put_data_item(tmp_data_p, res_id[0], res_id[1], res_id[2]);
//		if (DEBUG == 1){
//			char *str = print_dbt((void*)tmppo, PO_ID);
//			printf("Thread %d going to insert data item for key:", tid);
//			printf(ID_TYPE_FORMAT,  subject);
//			printf("\t and value: %s", str);
//			printf("\n");
//			free(str);
//		}

		DB_MULTIPLE_WRITE_NEXT(ptrk, &key, &subject, sizeof(id_type));
		DB_MULTIPLE_WRITE_NEXT(ptrd, &value, tmppo, sizeof(POID));
//		if (DEBUG == 1){
//			printf("Thread %d inserted data item for key:", tid);
//			printf(ID_TYPE_FORMAT,  subject);
//			printf("\t and value: ");
//			print_dbt((void*)tmppo, PO_ID);
//			printf("\n");
//		}
		assert(ptrk != NULL);
		assert(ptrd != NULL);
		nums2po++;
		free(tmppo);
		if (nums2po % UPDATES_PER_BULK_PUT == 0) {
			//					if (DEBUG == 1) printf("flush buffer to file\n");
			switch (ret = dbp->put(dbp, 0, &key, &value, flag)) {
			case 0:
				DB_MULTIPLE_WRITE_INIT(ptrk, &key);
				DB_MULTIPLE_WRITE_INIT(ptrd, &value);
				break;
			default:
				dbp->err(dbp, ret, "Bulk DB->put");
				goto err;
			}
		}
	}

	if (nums2po % UPDATES_PER_BULK_PUT != 0) {
//				if (DEBUG == 1) printf("flush buffer to file\n");
		switch (ret = dbp->put(dbp, 0, &key, &value, flag)) {
		case 0:
			DB_MULTIPLE_WRITE_INIT(ptrk, &key);
			DB_MULTIPLE_WRITE_INIT(ptrd, &value);
			break;
		default:
			dbp->err(dbp, ret, "Bulk DB->put");
			goto err;
		}
	}

//	printf("Thread %d freeing mytriples\n", tid);
	/* free the mytriples contents */
	MYTRIPLES *t, *tt;
	HASH_ITER(hh, mytriples, t, tt) {
		HASH_DEL(mytriples, t);
		free(t);
	}

	if (key.data != NULL) free(key.data);
	if (value.data != NULL) free(value.data);
//	printf("Thread %d done putting data into files\n", tid);

	return nums2po;

	err:
	printf("error in serializing data to db\n");
	exit(0);
}

int compare_strtriple(SPOSTR *s1, SPOSTR *s2){
	return (strcmp(s1->subject, s2->subject) && strcmp(s1->predicate, s2->predicate) && strcmp(s1->object, s2->object));
}

int compare_idtriple(SPOID *s1, SPOID *s2){
	return (s1->subject == s2->subject && s1->predicate == s2->predicate && s1->object == s2->object);
}

char *get_short_form_term(char *in){
	char *ret = NULL;
	char *instr = malloc(strlen(in) + 1);
	strcpy(instr, in);
	if (in[0] == '"' || (in[0] == '_' && in[1] == ':')){
		return instr;
	}
	if (in[0] == '<'){

		if (strstr(in, "<http://") != NULL){
			int i;
			int last_slash_pos = -1;
			for (i = strlen(in); i >= 0; i--){
				if (in[i] == '/' || in[i] == '#'){
					last_slash_pos = i;
					break;
				}
			}
	//		if (DEBUG == 1) printf("last pos of /# is %d in %s\n", last_slash_pos, in);

			if (last_slash_pos != -1){

				char* ns = malloc(last_slash_pos + 3);
				strcpy(ns, "<");
				strncat(ns, in + 1, last_slash_pos);
				strcat(ns, ">");
				ns[last_slash_pos + 2] = '\0';

				size_t size = strlen(in) - 2 - last_slash_pos + 1;
				char *resource = malloc(size);
				int j = 0;
				for (i = last_slash_pos + 1; i < strlen(in); i++){
					resource[j] = in[i];
					j++;
				}
				resource[size-1] = '\0';
		//		if (DEBUG == 1) printf(" %s is splitted into %s and %s\n", in, ns, resource);

				MYPREFIXES *prefixtmp = NULL, *d;
		//		if (DEBUG == 1){
		//			printf("Printing inmem_u2p before adding\n");
		//			for(d = inmem_u2p; d != NULL; d=d->hh.next) {
		//				printf("Dict temp item prefix %s with uri %s\n", d->prefix, d->uri);
		//			}
		//		}
				HASH_FIND_STR(inmem_u2p, ns, prefixtmp);
				char nsshort[50];

				if (prefixtmp == NULL){
					int done_adding_prefix = 0;
					while (done_adding_prefix == 0){
						(void)mutex_lock(&update_prefix_ns_lock);
						numns++;
						sprintf(nsshort, "ns%d", numns);
		//				if (DEBUG == 1) printf("short prefix of %s is %s\n", in, nsshort);
						MYPREFIXES *u2p_p = (MYPREFIXES*)malloc(sizeof(MYPREFIXES));
						u2p_p->prefix = malloc(strlen(nsshort) + 1);
						strcpy(u2p_p->prefix, nsshort);
						u2p_p->uri = ns;
//						if (DEBUG == 1) printf("insert new prefix, short form of %s is %s\n", ns, nsshort);
						HASH_ADD_STR(inmem_u2p, uri, u2p_p );
						done_adding_prefix = 1;
						(void)mutex_unlock(&update_prefix_ns_lock);
					}
		//			if (DEBUG == 1){
		//				printf("Printing inmem_u2p after adding\n");
		//				for(d = inmem_u2p; d != NULL; d=d->hh.next) {
		//					printf("Dict temp item prefix %s with uri %s\n", d->prefix, d->uri);
		//				}
		//			}
					char *short_form = malloc(strlen(nsshort) + strlen(resource) + 2);
					strcpy(short_form, nsshort);
					strcat(short_form, ":");
					strcat(short_form,resource);
//					if (DEBUG == 1) printf("short form of %s is %s\n", in, short_form);
					free(resource);
					free(instr);
					return short_form;
				} else {
		//			if (DEBUG == 1) printf("found prefix for short form of %s is %s and %s\n", ns, prefixtmp->prefix, prefixtmp->uri);
					if (strcmp(prefixtmp->prefix, "@base") == 0){
						char *short_form = malloc(strlen(resource) + 3);
						strcpy(short_form, "<");
						strcat(short_form, resource);
						strcat(short_form, ">");
//						if (DEBUG == 1) printf("base case short form of %s is %s\n", ns, short_form);

						free(ns);
						free(resource);
						free(instr);
						return short_form;
					} else {
						char *short_form = malloc(strlen(resource) + strlen(prefixtmp->prefix) + 2);
						strcpy(short_form, prefixtmp->prefix);
						strcat(short_form, ":");
						strcat(short_form, resource);
//						if (DEBUG == 1) printf("existing prefix case short form of %s is %s\n", ns, short_form);
						free(ns);
						free(resource);
						free(instr);
					return short_form;
					}
				}
			}
		}
	}
	return instr;
}

char *get_short_form_term_from_db(char *in){
	char *ret = malloc(strlen(in) + 1);
	strcpy(ret, in);
	ret[strlen(in)] = '\0';

//			if (DEBUG == 1) printf("input is %s\n", in);
	char *resource = NULL, *ns = NULL;

	if (in[0] == '"' || (in[0] == '_' && in[1] == ':')){
		return ret;
	} else if (in[0] == '<'){
		int i;
		int last_slash_pos = -1;
		for (i = strlen(in); i >= 0; i--){
			if (in[i] == '/' || in[i] == '#'){
				last_slash_pos = i;
				break;
			}
		}
//		if (DEBUG == 1) printf("last pos of /# is %d in %s\n", last_slash_pos, in);

		if (last_slash_pos != -1){
			ns = malloc(sizeof(char)*(last_slash_pos + 3));
//			strncy(ns, in, last_slash_pos + 1);
//			strcat(ns, ">");
//			ns[last_slash_pos + 2] = '\0';

//			for (i = 0; i <= last_slash_pos; i++){
//				ns[i] = in[i];
//			}
			memcpy(ns, in, last_slash_pos + 1);
			ns[last_slash_pos + 1] = '>';
			ns[last_slash_pos + 2] = '\0';

//			if (DEBUG == 1) printf(" %s is splitted into ns %s of len %d\n", in, ns, strlen(ns));

			size_t size = strlen(in) - 2 - last_slash_pos + 1;
			if (size > 0){
				resource = malloc(sizeof(char) * (strlen(in) - 2 - last_slash_pos + 1));
//				int j = 0;
//				for (i = last_slash_pos + 1; i < strlen(in) - 1; i++){
//					resource[j] = in[i];
//					j++;
//				}
				memcpy(resource, in + last_slash_pos + 1, size-1);
				resource[size-1] = '\0';
//				if (DEBUG == 1) printf(" j %d vs. size-1 %d\n", j, size-1);
//				if (DEBUG == 1) printf(" %s is splitted into %s of len %d\n", in, resource, strlen(resource));

				char *prefix, *d;
				prefix = lookup_prefix(ns);
//				if (DEBUG == 1) printf("prefix is %s of len %d\n", prefix, strlen(prefix));
				if (prefix != NULL){
					char *short_form = malloc(strlen(prefix) + strlen(resource) + 2);
					strcpy(short_form, prefix);
					strcat(short_form, ":");
					strcat(short_form,resource);
//						if (DEBUG == 1) printf("short form %s of %s of len is %d\n", in, short_form, strlen(short_form));
					free(ns);
					free(resource);
					free(prefix);
					free(ret);
					return short_form;
				}
			}
		}
	}
	return ret;
}

char *get_full_form_term(MYPREFIXES *pr, char *in, int prefix_line){
	char *rets;
	char *instr = malloc(strlen(in) + 1);
	strcpy(instr, in);
	//		if (DEBUG == 1) printf("input is %s\n", in);
	MYPREFIXES *pref = NULL;

	if (instr[0] == '<'){
		if (strstr(instr, "<http://") != NULL){
			// This is a full URI, no need to process
			//		if (DEBUG == 1) printf("normal uri %s\n", in);
			return instr;
		}
		// This is with a base uri
		HASH_FIND_STR(pr, "@base", pref);
//				if (DEBUG == 1) printf("base uri %s of size %d\n", pref->uri, strlen(pref->uri));
//				if (DEBUG == 1) printf("in uri %s of size %d\n", in, strlen(in));

		if (pref != NULL){
			size_t size = strlen(pref->uri) + strlen(instr) - 1;
			rets = malloc(size);
			//		printf("size:%d", size);
			int i = 0, j = 1;
			for (i = 0; i < strlen(pref->uri) - 1; i++){
				rets[i] = pref->uri[i];
			}
			for (; i < size; i++){
				rets[i] = in[j];
				j++;
			}
			rets[size - 1] = '\0';

			//		if (DEBUG == 1) printf("output full uri %s with size %d\n", ret, strlen(ret));
			free(instr);
			return rets;
		}
	}

	if (isalpha(instr[0])){

		// Could be prefix or a shorted term
		//		if (DEBUG == 1) printf("alpha is %s \n", in);
		static char *saved1;
		char *ptr = instr;
		char *pre = strtok_r(ptr, ":", &saved1);
		if (pre != NULL){
			ptr = saved1;
			//			if (DEBUG == 1) printf("pre is %s\n", pre);
			char *res = strtok_r(ptr, ":", &saved1);
			if (res != NULL){
				//				if (DEBUG == 1) printf("res is %s\n", res);
				HASH_FIND_STR(pr, pre, pref);
				//				if (DEBUG == 1) printf("retrieved pre is %s\n", pref->uri);
				if (pref != NULL){

					size_t size = strlen(pref->uri) - 1 + strlen(res) + 2;
					rets = malloc(size);
					//				printf("size:%d", size);
					int i = 0, j = 0;
					for (i = 0; i < strlen(pref->uri) - 1; i++){
						rets[i] = pref->uri[i];
					}
					for (i = strlen(pref->uri) - 1; i < size - 2; i++){
						rets[i] = res[j];
						j++;
					}
					rets[size-2] = '>';
					rets[size-1] = '\0';
					free(instr);
					return rets;
				}
			} else {

				// This is a refix
				if (prefix_line == 1){
					rets = malloc(strlen(pre) + 1);
					strcpy(rets, pre);
					free(instr);
					return rets;
				}
			}
		} else {
			if (DEBUG == 1) printf("pre is null %s\n", instr);
		}
		return instr;
	}

	if (instr[0] == '@' || (instr[0] == '_' && instr[1] == ':')) {
		return instr;
	}

	if (instr[0] == '"'){
		// Literal
		static char *saved1;
		char *ptr = instr;
		char *pre = strtok_r(ptr, "^^", &saved1);
		if (pre != NULL){
			ptr = saved1;
			//			if (DEBUG == 1) printf("pre is %s\n", pre);
			char *res = strtok_r(ptr, "^^", &saved1);
			if (res != NULL){
				char *type = get_full_form_term(pr, res, 0);
				rets = malloc(strlen(pre) + strlen(type) + 3);
				strcpy(rets, pre);
				strcat(rets, "^^");
				strcat(rets, type);
				free(type);
				free(instr);
				return rets;
			} else {
				if (prefix_line == 1){
					rets = malloc(strlen(pre) + 1);
					strcpy(rets, pre);
					free(instr);
					return rets;
				}
			}
		} else {
			//			if (DEBUG == 1) printf("pre is null %s\n", in);
		}
	}

	return instr;
}

char *get_full_form_term_from_db(char *in, int prefix_line){
	char *rets, *prefixuri;
	char *instr = malloc(strlen(in) + 1);
	strcpy(instr, in);
//			if (DEBUG == 1) printf("input is %s\n", in);
	MYPREFIXES *pref = NULL;

	if (instr[0] == '@' || (instr[0] == '_' && instr[1] == ':')) {
		return instr;
	} else if (instr[0] == '"'){
		// Literal
		static char *saved1;
		char *ptr = in;
		char *pre = strtok_r(ptr, "^^", &saved1);
		if (pre != NULL){
			ptr = saved1;
			//			if (DEBUG == 1) printf("pre is %s\n", pre);
			char *res = strtok_r(ptr, "^^", &saved1);
			if (res != NULL){
				char *type = get_full_form_term_from_db(res, 0);
				rets = malloc(strlen(pre) + strlen(type) + 3);
				strcpy(rets, pre);
				strcat(rets, "^^");
				strcat(rets, type);
				free(type);
				free(instr);
				return rets;
			} else {
				if (prefix_line == 1){
					rets = malloc(strlen(pre) + 1);
					strcpy(rets, pre);
					free(instr);
					return rets;
				}
			}
		} else {
			//			if (DEBUG == 1) printf("pre is null %s\n", in);
		}
	} else if (instr[0] == '<'){
		if (strstr(instr, "<http://") != NULL){
			// This is a full URI, no need to process
			//		if (DEBUG == 1) printf("normal uri %s\n", in);
			return instr;
		}
		// This is with a base uri
		prefixuri = lookup_prefix("@base");
//				if (DEBUG == 1) printf("base uri %s of size %d\n", pref->uri, strlen(pref->uri));
//				if (DEBUG == 1) printf("in uri %s of size %d\n", in, strlen(in));

		if (prefixuri != NULL){
			size_t size = strlen(prefixuri) + strlen(instr) - 1;
			rets = malloc(size);
			//		printf("size:%d", size);
			int i = 0, j = 1;
			for (i = 0; i < strlen(prefixuri) - 1; i++){
				rets[i] = prefixuri[i];
			}
			for (; i < size; i++){
				rets[i] = in[j];
				j++;
			}
			rets[size - 1] = '\0';

			//		if (DEBUG == 1) printf("output full uri %s with size %d\n", ret, strlen(ret));
			free(instr);
			return rets;
		}
	} else if (isalpha(instr[0])){

		// Could be prefix or a shorted term
		//		if (DEBUG == 1) printf("alpha is %s \n", in);
		static char *saved1;
		char *ptr = in;
		char *pre = strtok_r(ptr, ":", &saved1);
		if (pre != NULL){
			prefixuri = lookup_prefix(pre);
			ptr = saved1;
//						if (DEBUG == 1) printf("pre is %s\n", pre);
//						if (DEBUG == 1) printf("retrieved pre is %s\n", prefixuri);
			char *res = strtok_r(ptr, ":", &saved1);
			if (res != NULL){
//				if (DEBUG == 1) printf("res is %s\n", res);
				if (prefixuri != NULL){

					size_t size = strlen(prefixuri) - 1 + strlen(res) + 2;
					rets = malloc(size);
					memset(rets, 0, size);
					strncpy(rets, prefixuri, strlen(prefixuri) - 1);
					strcat(rets, res);
					rets[size-2] = '>';
					rets[size-1] = '\0';
//					if (DEBUG == 1) printf("full form is %s\n", rets);
					free(instr);
					free(prefixuri);
					return rets;
				}
			} else {

				// This is a refix
				if (prefix_line == 1){
					rets = malloc(strlen(prefixuri) + 1);
					memset(rets, 0, strlen(prefixuri) + 1);
					strcpy(rets, prefixuri);
					free(instr);
					free(prefixuri);
					return rets;
				}
			}
		} else {
//			if (DEBUG == 1) printf("This is a prefix %s\n", instr);
			char *prefix = lookup_prefix(instr);
			char *ret = malloc(strlen(prefix) + 1);
			strcpy(ret, prefix);
			free(prefix);
			return ret;
		}
		return instr;
	}

	return instr;
}

void* load_one_round_pthread(void* thread_id) {
	int tid = (intptr_t)thread_id;
	if (tid > num_files){
		if (DEBUG == 1) printf("Thread %d: no more file to process. Done.\n", tid);
		return 0;
	}
	int file;
	struct timeval tvBegin, tvEnd, tvDiff;
	if (DEBUG == 1) printf("Thread %d with ID %ld started\n", tid, (long)pthread_self());

	DB *tmp_db_p, *tmp_data_p;
	id_type node_id = tid * 10000000000 + 2;
	id_type node_id_odd = node_id + 1;
	id_type triple_id = (tid == 0)?1:tid*10000000000;

	if (DEBUG == 1) printf("Triple's ID starts at ");
	if (DEBUG == 1) printf(ID_TYPE_FORMAT, triple_id);
	if (DEBUG == 1) printf("URI's ID starts at ");
	if (DEBUG == 1) printf(ID_TYPE_FORMAT, node_id);
	if (DEBUG == 1) printf("\t Literal's ID starts at ");
	if (DEBUG == 1) printf(ID_TYPE_FORMAT, node_id_odd);
	if (DEBUG == 1) printf("\n");

	char *tmp_db_name = malloc(sizeof(tid) + 10 * sizeof(char));
	char *tmp_db_file;
	char *tmp_data_db_name =  malloc(sizeof(tid) + 10 * sizeof(char));
	char *tmp_data_db_file;

	char strtid[4];
	sprintf(strtid, "%d", tid);
	strcpy(tmp_db_name, "tmp_");
	strcat(tmp_db_name, strtid);
	strcat(tmp_db_name, "_dict");

	tmp_db_file = malloc(strlen(env_home) + strlen(tmp_db_name) + 2);
	strcpy(tmp_db_file, env_home);
	strcat(tmp_db_file, "/");
	strcat(tmp_db_file, tmp_db_name);

	strcpy(tmp_data_db_name, "tmp_");
	strcat(tmp_data_db_name, strtid);
	strcat(tmp_data_db_name, "_data");

	tmp_data_db_file = malloc(strlen(env_home) + strlen(tmp_data_db_name) + 2);
	strcpy(tmp_data_db_file, env_home);
	strcat(tmp_data_db_file, "/");
	strcat(tmp_data_db_file, tmp_data_db_name);

	FILE *ifp;
	size_t linesize = 10000;
	char *linebuff = NULL;
	ssize_t linelen = 0;
	size_t cur_size;
	long id;
	char *rdf_dfile;

	struct timeval start, end, diff;


	/* Setting up the BerkeleyDB parameters */
	DBT key, value, vtmp, s2pokey, s2podata;
	int ret, ret_t;
	void *ptrdict, *ptrs2po;
	u_int32_t flag;

	flag = DB_MULTIPLE_KEY;

	s2pokey.ulen = bulk_size * 1024 * 1024;
	s2pokey.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	s2pokey.data = malloc(s2pokey.ulen);
	memset(s2pokey.data, 0, s2pokey.ulen);

	s2podata.ulen = 1 * 1024 * 1024;
	s2podata.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	s2podata.data = malloc(s2podata.ulen);
	memset(s2podata.data, 0, s2podata.ulen);

	long numdict;
	long nums2po;

	MYPREFIXES *myprefixes = NULL, *prefix, *tmp;

	for (file = tid; file < num_files; file = file + load_threads){
		rdf_dfile = files_p[file];
		gettimeofday(&tvBegin, NULL);

		/* Init a temporary database*/
		/* Initialize the DB handle */
		if (tmp_dbs_enabled == 1){

			tmp_db_p = init_tmp_db(tmp_db_name, 0, 0);
			tmp_data_p = init_tmp_db(tmp_data_db_name, 0, 1);
		} else {
			tmp_db_p = dict_db_p;
			tmp_data_p = data_s2po_db_p;
		}

		gettimeofday(&start, NULL);
		/*
		 * Read the content from the rdf_file and put RDF resources into the dictdb.
		 */
		ifp = fopen(rdf_dfile, "r");
		if (ifp == NULL) {
			fprintf(stderr, "Error opening file '%s'\n", rdf_dfile);
			exit (-1);
		}

		/*
		 * Create a cursor for the dictionary database
		 */
		if (DEBUG == 1) printf("Start loading file %s ...\n", rdf_dfile);


		/**
		 *  Initizize the variables for bulk load
		 */
		DB_MULTIPLE_WRITE_INIT(ptrs2po, &s2pokey);

		char *ptr = NULL;
		char *res_p[3];

		long start_term_idx = 0, end_term_idx = 0, last_non_blank = 0, fi = 0;
		int i = 0, spo = 0, line = 0, term_started = 0, found = 0, literal = 0, bnode = 0, prefix_line = 0;
		/* Iterate over the input RDF file*/
		while ((linelen = getline(&linebuff, &linesize, ifp)) > 0) {
			if (linebuff[0] == '#') {
				// Skip the comment line
			} else if (linelen > 2){
				//								if (DEBUG == 1) printf("Start processing line %s\n", linebuff);
				//			if (DEBUG == 1) printf("Start processing line %d of len %d and of size %d...\n", line, linelen, linesize);
				//												if (DEBUG == 1) printf("%d:Line: %d\n", tid,line);
				for (i = 0; i < linelen; i++) {
					/*
					 * Identify the term from start_term to end_term
					 * If the cur pointer is within subject, predicate, or object
					 */
					if (isblank(linebuff[i])){
						//						if (DEBUG == 1) printf("Thread %d: char %c at %d is blank \n", tid, linebuff[i], i);
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
								//								if (DEBUG == 1) printf("Thread %d: from %d to %d with spo %d literal %d\n", tid, (long)start_term_idx, (long)end_term_idx, spo, literal);

								/* Extract the found term */
								res_p[spo] = extract_term(linebuff, start_term_idx, end_term_idx);
								if (strcmp(res_p[spo], "@base") == 0 || strcmp(res_p[spo], "@prefix") == 0){
									prefix_line = 1;
								}
								//								if (DEBUG == 1) printf("Thread %d: Found spo %d \t %s \n", tid, spo, tmp);
								res_p[spo] = get_full_form_term(myprefixes, res_p[spo], prefix_line);
								//								if (DEBUG == 1) printf("Thread %d: Found full spo %d \t %s \n", tid, spo, res_p[spo]);

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
						if (term_started == 0 && linebuff[i] != '.' && linebuff[i] != ':'){
							//							if (DEBUG == 1) printf("spo %d start at %d with %c\n", spo, (int)i, linebuff[i]);
							term_started = 1;
							start_term_idx = i;
							// Identify the literal for object
						}
						if (linebuff[i] == '"' && (( linebuff[i-1] != '\\' ) || (linebuff[i-1] == '\\' && linebuff[i-2] == '\\'))){
							literal++;
						}
						if (linebuff[i] == ':' && linebuff[i-1] == '_'){
							bnode = 1;
							term_started = 1;
							start_term_idx = i - 1;
						}
					}
				} // Finish identifying the resources in the line


				if (prefix_line == 1 && (strcmp(res_p[0], "@base") == 0)){
					//					if (DEBUG == 1) printf("Thread %d: %s:\t%s\t%s\t\n", tid, res_p[0], res_p[1]);
					prefix_line = 1;
					// Resolve the URI
					if (DEBUG == 1) printf("Getting the base %s\n", res_p[1]);
					prefix = (MYPREFIXES*)malloc(sizeof(MYPREFIXES));
					prefix->prefix = res_p[0];
					prefix->uri = res_p[1];
					HASH_ADD_KEYPTR( hh, myprefixes, prefix->prefix, strlen(prefix->prefix), prefix );
				} else if (prefix_line == 1 && (strcmp(res_p[0], "@prefix") == 0)){
					//					if (DEBUG == 1) printf("Thread %d: %s:\t%s\t%s\t\n", tid, res_p[0], res_p[1], res_p[2]);
					prefix_line = 1;
					if (DEBUG == 1) printf("Getting the prefix %s as %s\n", res_p[1], res_p[2]);
					prefix = (MYPREFIXES*)malloc(sizeof(MYPREFIXES));
					prefix->prefix = res_p[1];
					prefix->uri = res_p[2];
					HASH_ADD_KEYPTR( hh, myprefixes, prefix->prefix, strlen(prefix->prefix), prefix );
					free(res_p[0]);
				} else if (prefix_line == 0){
					// Processing the dictionary and indexes
					//					if (DEBUG == 1) printf("Getting normal triple\n");
					//					if (DEBUG == 1) printf("Thread %d: %s:\t%s\t%s\t\n", tid, res_p[0], res_p[1], res_p[2]);

					id_type res_id[3];
					for (i = 0; i < 3; i++) {

						// Check if this is a valid term
						if (res_p[i] != NULL){

							res_id[i] = lookup_id(tmp_db_p, res_p[i]);
							if (res_id[i] < 0){
								if (strstr(res_p[i], "\"") != NULL){
									res_id[i] = node_id_odd;
								} else {
									res_id[i] = node_id;
								}
								//								printf("%d insert dict item for key:", tid);
								//								printf("%s", res_p[i]);
								//								printf("\t and value: ");
								//								printf(ID_TYPE_FORMAT, res_id[i]);
								//								printf("\n");
								put_dict_item(tmp_db_p, res_p[i], res_id[i]);
								if (strstr(res_p[i], "\"") != NULL){
									node_id_odd = node_id_odd + 2;
								} else {
									node_id = node_id + 2;
								}
							}

							free(res_p[i]);
							res_p[i] = NULL;
							//					free(key.data);
							//					free(value.data);
						} else {
							printf("This triple %s is invalid\n", linebuff);
						}
					}

					// Store the triple ids into tmp_data_s2po_db

					POID *tmppo = malloc(sizeof(POID));
					tmppo->predicate = res_id[1];
					tmppo->object = res_id[2];
					//				printf("insert data item for key:");
					//				printf(ID_TYPE_FORMAT,  res_id[0]);
					//				printf("\t and value: ");
					//				print_dbt((void*)tmppo, PO_ID);
					//				printf("\n");

					id_type subject = res_id[0];

					//				ret = put_data_item(tmp_data_p, res_id[0], res_id[1], res_id[2]);
					DB_MULTIPLE_KEY_WRITE_NEXT(ptrs2po, &s2pokey, &subject, sizeof(id_type), tmppo, sizeof(POID));
					assert(ptrs2po != NULL);
					nums2po++;
					if (nums2po % UPDATES_PER_BULK_PUT == 0) {
						//					if (DEBUG == 1) printf("flush buffer to file\n");
						switch (ret = tmp_data_p->put(tmp_data_p, 0, &s2pokey, &s2podata, flag)) {
						case 0:
							DB_MULTIPLE_WRITE_INIT(ptrs2po, &s2pokey);
							break;
						default:
							tmp_data_p->err(tmp_data_p, ret, "Bulk DB->put");
							goto err;
						}
					}
					free(tmppo);

				}


				// Update the counters
				spo = 0;
				line++;
				start_term_idx = 0;
				end_term_idx = 0;
				literal = 0;
				term_started = 0;
				bnode = 0;
				prefix_line = 0;

			}
			free(linebuff);
			linebuff = NULL;
		}

		if (nums2po % UPDATES_PER_BULK_PUT != 0) {
			//					if (DEBUG == 1) printf("flush buffer to file\n");
			switch (ret = tmp_data_p->put(tmp_data_p, 0, &s2pokey, &s2podata, flag)) {
			case 0:
				DB_MULTIPLE_WRITE_INIT(ptrs2po, &s2pokey);
				break;
			default:
				tmp_data_p->err(tmp_data_p, ret, "Bulk DB->put");
				goto err;
			}
		}
		/* Close the file */
		fclose(ifp);
		int print_size = 100;
		gettimeofday(&end, NULL);
		timeval_subtract(&diff, &end, &start);
		if (DEBUG == 1) printf("Finish parsing file %s: %ld.%06ld sec.\n", rdf_dfile, (long int)diff.tv_sec, (long int)diff.tv_usec);

		if (tmp_dbs_enabled == 1){

			int done_merge_dict = 0;
			int done_merge_data = 0;
			int done_resolved_id = 0;

			char* tmp_id_mappings = malloc(strlen(tmp_data_db_name) + 2);
			strcpy(tmp_id_mappings, tmp_data_db_name);
			strcat(tmp_id_mappings, "i");
			DB* tmp_id_mappings_p = init_tmp_db(tmp_id_mappings, 0, 0);

			char* tmp_resolved_db = malloc(strlen(tmp_data_db_name) + 2);
			strcpy(tmp_resolved_db, tmp_data_db_name);
			strcat(tmp_resolved_db, "r");
			DB* tmp_data_resolved_p = init_tmp_db(tmp_resolved_db, 0, 1);
			long size;

			int with_id_mapping = 0;
			while (done_merge_dict == 0 || done_merge_data == 0){

				/* Putting the content of this db to the main dict db*/
				if (done_merge_dict == 0){

					(void)mutex_lock(&write_dict_db_lock);

					if (DEBUG == 1) printf("Thread %d: get the merge dict lock\n", tid);
					gettimeofday(&start, NULL);
					if (DEBUG == 1) timeval_print(&start);

					with_id_mapping = merge_dbs_with_mapping(tmp_db_p, dict_db_p, tmp_id_mappings_p);
					done_merge_dict = 1;

					gettimeofday(&end, NULL);
					timeval_subtract(&diff, &end, &start);
					if (DEBUG == 1) printf("Finish merge_dbs_with_mapping %s: %ld.%06ld sec.\n", rdf_dfile, (long int)diff.tv_sec, (long int)diff.tv_usec);
					if (DEBUG == 1) timeval_print(&end);
					if (DEBUG == 1) printf("Thread %d: release the merge dict lock\n", tid);

					(void)mutex_unlock(&write_dict_db_lock);
				}

				if (done_merge_dict == 1 && done_merge_data == 0 && done_resolved_id == 0){
					if (with_id_mapping == 1){

						gettimeofday(&start, NULL);
						if (DEBUG == 1) timeval_print(&start);

						resolve_ids(tmp_data_p, tmp_data_resolved_p, tmp_id_mappings_p);
						done_resolved_id = 1;

						gettimeofday(&end, NULL);
						timeval_subtract(&diff, &end, &start);
						if (DEBUG == 1) timeval_print(&end);
						if (DEBUG == 1) printf("Finished resolving id in %s: %ld.%06ld sec.\n", rdf_dfile, (long int)diff.tv_sec, (long int)diff.tv_usec);
					}
				}

				/* Putting the content of this db to the temp data db*/
				if (done_merge_data == 0){
					(void)mutex_lock(&write_data_s2po_db_lock);

					gettimeofday(&start, NULL);
					if (DEBUG == 1) timeval_print(&start);
					if (DEBUG == 1) printf("Thread %d: get the merge data lock\n", tid);

					if (with_id_mapping == 1 && done_resolved_id == 1){
						merge_dbs(tmp_data_resolved_p, data_s2po_db_p);
						done_merge_data = 1;
					} else if (with_id_mapping == 0){
						merge_dbs(tmp_data_p, data_s2po_db_p);
						done_merge_data = 1;
					}
					if (DEBUG == 1) printf("Thread %d: release the merge data lock\n", tid);
					gettimeofday(&end, NULL);
					if (DEBUG == 1) timeval_print(&end);
					timeval_subtract(&diff, &end, &start);
					if (DEBUG == 1) printf("Finished merge_dbs %s: %ld.%06ld sec.\n", rdf_dfile, (long int)diff.tv_sec, (long int)diff.tv_usec);

					(void)mutex_unlock(&write_data_s2po_db_lock);
				}
			}
			if (DEBUG == 1) {
				printf("Thread %d: Printing temporary dictionary db.\n", tid);
				print_db(tmp_db_p, CHAR_P, ID_TYPE, print_size);
			}

			if (DEBUG == 1) {
				printf("Thread %d: Printing temporary data db.\n", tid);
				print_db(tmp_data_p, ID_TYPE, PO_ID, print_size);
			}

			if (DEBUG == 1) {
				printf("Thread %d: Printing id mapping db.\n", tid);
				print_db(tmp_id_mappings_p, ID_TYPE, ID_TYPE, print_size);
			}
			if (DEBUG == 1) {
				printf("Thread %d: Printing resolved data db.\n", tid);
				print_db(tmp_data_resolved_p, ID_TYPE, PO_ID, print_size);
			}
			if (DEBUG == 1) {
				printf("Thread %d: Printing dict db.\n", tid);
				print_db(dict_db_p, CHAR_P, ID_TYPE, print_size);
			}
			remove_tmp_db(tmp_db_p, tmp_db_name, 0);
			remove_tmp_db(tmp_data_p, tmp_data_db_name, 0);
			remove_tmp_db(tmp_data_resolved_p, tmp_resolved_db, 0);
			remove_tmp_db(tmp_id_mappings_p, tmp_id_mappings, 0);

			free(tmp_resolved_db);
			free(tmp_id_mappings);
		}
		gettimeofday(&tvEnd, NULL);
		if (DEBUG == 1) timeval_print(&tvEnd);
		timeval_subtract(&tvDiff, &tvEnd, &tvBegin);
		printf("Processed %s: %ld.%06ld sec.\n", rdf_dfile, (long int)tvDiff.tv_sec, (long int)tvDiff.tv_usec);


	}
	free(tmp_db_name);
	free(tmp_data_db_name);
	//		size = get_db_size(tmp_id_mappings_p);
	//		printf("tmp_id_mapping db size: %ld\n", size);
	//		if (DEBUG == 1) print_db(tmp_id_mappings_p, ID_TYPE, ID_TYPE, 100);
	//
	//		size = get_db_size(tmp_data_p);
	//		printf("tmp_data_p db before resolving size: %ld\n", size);
	//		if (DEBUG == 1) print_db(tmp_data_p, ID_TYPE, PO_ID, 100);

	//		printf("data_s2po_db_p db after resolving size:%ld\n", size);
	//		if (DEBUG == 1) print_db(data_s2po_db_p, ID_TYPE, PO_ID, 100);

	/* free the hash table contents */
	HASH_ITER(hh, myprefixes, prefix, tmp) {
		HASH_DEL(myprefixes, prefix);
		free(prefix);
	}

	if (DEBUG == 1) printf("Thread %d: Done processing.\n", tid);
	return 0;
	err:
	/* Close our database handle, if it was opened. */
	close_dbs();
	exit(0);
}

int
init_load_one_round_inmem_thread(int threads){

	/*
	 * Thread configuration
	 */
	/* Initialize a mutex. Used to help provide thread ids. */
	(void)mutex_init(&write_data_s2po_db_lock, NULL);
	(void)mutex_init(&write_dict_db_lock, NULL);
	(void)mutex_init(&fetch_file_lock, NULL);
	(void)mutex_init(&update_prefix_ns_lock, NULL);

	pthread_attr_t attr;
	/* Initialize and set thread detached attribute */
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	if (DEBUG == 1) printf("Start creating threads \n");

	pthread_t threadids[threads];
	int err, i = 0;
	void *status;
	while (i < threads) {
		if (DEBUG == 1) printf("Creating thread %d\n", i);
		err = pthread_create(&(threadids[i]), &attr, &load_one_round_inmem_pthread, (void *)(intptr_t)i);
		if (err != 0)
			printf("\ncan't create thread %d:[%s]", i, strerror(err));
		else
			if (DEBUG == 1) printf("Creating thread %d successfully\n", i);
		i++;
	}

	pthread_attr_destroy(&attr);
	if (DEBUG == 1) printf("Start joining threads \n");
	for (i = 0; i < threads; i++) {
		err = pthread_join(threadids[i], &status);
		if (err) {
			printf("ERROR; return code from pthread_join() is %d\n", err);
			exit(-1);
		}
	}
	if (DEBUG == 1) printf("End threads.\n");
	pthread_mutex_destroy(&write_data_s2po_db_lock);
	pthread_mutex_destroy(&write_dict_db_lock);
	pthread_mutex_destroy(&fetch_file_lock);
	pthread_mutex_destroy(&update_prefix_ns_lock);

}
int
init_load_one_round_thread(int threads){

	/*
	 * Thread configuration
	 */
	/* Initialize a mutex. Used to help provide thread ids. */
	(void)mutex_init(&write_data_s2po_db_lock, NULL);
	(void)mutex_init(&write_dict_db_lock, NULL);
	pthread_attr_t attr;
	/* Initialize and set thread detached attribute */
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	if (DEBUG == 1) printf("Start creating threads \n");

	pthread_t threadids[threads];
	int err, i = 0;
	void *status;
	while (i < threads) {
		if (DEBUG == 1) printf("Creating thread %d\n", i);
		err = pthread_create(&(threadids[i]), &attr, &load_one_round_pthread, (void *)(intptr_t)i);
		if (err != 0)
			printf("\ncan't create thread %d:[%s]", i, strerror(err));
		else
			if (DEBUG == 1) printf("Creating thread %d successfully\n", i);
		i++;
	}

	pthread_attr_destroy(&attr);
	if (DEBUG == 1) printf("Start joining threads \n");
	for (i = 0; i < threads; i++) {
		err = pthread_join(threadids[i], &status);
		if (err) {
			printf("ERROR; return code from pthread_join() is %d\n", err);
			exit(-1);
		}
	}
	if (DEBUG == 1) printf("End threads.\n");
	pthread_mutex_destroy(&write_data_s2po_db_lock);
	pthread_mutex_destroy(&write_dict_db_lock);

}
int
init_load_dict_thread(int threads){
	/*
	 * Thread configuration
	 */
	/* Initialize a mutex. Used to help provide thread ids. */
	(void)mutex_init(&write_dict_db_lock, NULL);
	pthread_attr_t attr;
	/* Initialize and set thread detached attribute */
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	if (DEBUG == 1) printf("Start creating threads \n");

	pthread_t threadids[threads];
	int err, i = 0;
	void *status;
	while (i < threads) {
		if (DEBUG == 1) printf("Creating thread %d\n", i);
		err = pthread_create(&(threadids[i]), &attr, &load_dict_pthread, (void *)(intptr_t)i);
		if (err != 0)
			printf("\ncan't create thread %d:[%s]", i, strerror(err));
		else
			if (DEBUG == 1) printf("Creating thread %d successfully\n", i);
		i++;
	}

	pthread_attr_destroy(&attr);
	if (DEBUG == 1) printf("Start joining threads \n");
	for (i = 0; i < threads; i++) {
		err = pthread_join(threadids[i], &status);
		if (err) {
			printf("ERROR; return code from pthread_join() is %d\n", err);
			exit(-1);
		}
	}
	if (DEBUG == 1) printf("End threads.\n");
	pthread_mutex_destroy(&write_dict_db_lock);

}

int
init_load_data_thread(int threads){
	/*
	 * Thread configuration
	 */
	/* Initialize a mutex. Used to help provide thread ids. */
	(void)mutex_init(&write_data_s2po_db_lock, NULL);
	pthread_attr_t attr;
	/* Initialize and set thread detached attribute */
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	if (DEBUG == 1) printf("Start creating threads \n");

	pthread_t threadids[threads];
	int err, i = 0;
	void *status;
	while (i < threads) {
		if (DEBUG == 1) printf("Creating thread %d\n", i);
		err = pthread_create(&(threadids[i]), &attr, &load_data_db_pthread, (void *)(intptr_t)i);
		if (err != 0)
			printf("\ncan't create thread %d:[%s]", i, strerror(err));
		else
			if (DEBUG == 1) printf("Creating thread %d successfully\n", i);
		i++;
	}

	pthread_attr_destroy(&attr);
	if (DEBUG == 1) printf("Start joining threads \n");
	for (i = 0; i < threads; i++) {
		err = pthread_join(threadids[i], &status);
		if (err) {
			printf("ERROR; return code from pthread_join() is %d\n", err);
			exit(-1);
		}
	}
	if (DEBUG == 1) printf("End threads.\n");
	pthread_mutex_destroy(&write_data_s2po_db_lock);

}

/*
 * Generating dictionary db from a single file
 */



void* load_dict_pthread(void* thread_id) {
	int tid = (intptr_t)thread_id;
	if (tid > num_files){
		if (DEBUG == 1) printf("Thread %d: no more file to process. Done.\n", tid);
		return 0;
	}
	int file;
	struct timeval tvBegin, tvEnd, tvDiff;
	if (DEBUG == 1) printf("Thread %d with ID %ld started\n", tid, (long)pthread_self());
	DBT key, value, vtmp;
	int ret, ret_t;

	id_type node_id = tid * 10000000000 + 2;
	id_type node_id_odd = node_id + 1;
	id_type triple_id = (tid == 0)?1:tid*10000000000;

	if (DEBUG == 1) printf("Triple's ID starts at ");
	if (DEBUG == 1) printf(ID_TYPE_FORMAT, triple_id);
	if (DEBUG == 1) printf("URI's ID starts at ");
	if (DEBUG == 1) printf(ID_TYPE_FORMAT, node_id);
	if (DEBUG == 1) printf("\t Literal's ID starts at ");
	if (DEBUG == 1) printf(ID_TYPE_FORMAT, node_id_odd);
	if (DEBUG == 1) printf("\n");

	char *tmp_db_name = malloc(sizeof(tid) + 10 * sizeof(char));
	DB *tmp_db_p;


	char strtid[4];
	sprintf(strtid, "%d", tid);
	strcpy(tmp_db_name, "tmp_");
	strcat(tmp_db_name, strtid);
	strcat(tmp_db_name, "_dict");


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

		/* Init a temporary database*/
		/* Initialize the DB handle */
		if (tmp_dbs_enabled == 1){

			tmp_db_p = init_tmp_db(tmp_db_name, 0, 0);

		} else {
			tmp_db_p = dict_db_p;
		}

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
			exit (-1);
		}

		/*
		 * Create a cursor for the dictionary database
		 */
		if (DEBUG == 1) printf("Start loading file %s ...\n", rdf_dfile);

		/* Iterate over the input RDF file*/
		while ((linelen = getline(&linebuff, &linesize, ifp)) > 0) {
			if (linebuff[0] == '#') {
				// Skip the comment line
			} else if (linelen > 2){
				//				if (DEBUG == 1) printf("Start processing line %s\n", linebuff);
				//			if (DEBUG == 1) printf("Start processing line %d of len %d and of size %d...\n", line, linelen, linesize);
				//								if (DEBUG == 1) printf("%d:Line: %d\n", tid,line);
				for (i = 0; i < linelen; i++) {
					/*
					 * Identify the term from start_term to end_term
					 * If the cur pointer is within subject, predicate, or object
					 */
					if (isblank(linebuff[i])){
						//						if (DEBUG == 1) printf("Thread %d: char %c at %d is blank \n", tid, linebuff[i], i);
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
								//								if (DEBUG == 1) printf("Thread %d: from %d to %d with spo %d literal %d\n", tid, (long)start_term_idx, (long)end_term_idx, spo, literal);

								/* Extract the found term */
								res_p[spo] = NULL;
								res_p[spo] = extract_term(linebuff, start_term_idx, end_term_idx);

								//								if (DEBUG == 1) printf("Thread %d: Found spo %d\t %s \n", tid, spo, res_p[spo]);

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
							//							if (DEBUG == 1) printf("spo %d start at %d with %c\n", spo, (int)i, linebuff[i]);
							term_started = 1;
							start_term_idx = i;
							// Identify the literal for object
						}
						if (linebuff[i] == '"' && (( linebuff[i-1] != '\\' ) || (linebuff[i-1] == '\\' && linebuff[i-2] == '\\'))){
							literal++;
						}
						if (linebuff[i] == ':' && linebuff[i-1] == '_'){
							bnode = 1;
							term_started = 1;
							start_term_idx = i - 1;
						}
					}
				} // Finish identifying the resources in the line

				//				if (DEBUG == 1) printf("Thread %d: %s:\t%s\t%s\t\n", tid, res_p[0], res_p[1], res_p[2]);

				// Processing the dictionary and indexes

				for (i = 0; i < 3; i++) {

					// Check if this is a valid term
					if (res_p[i] != NULL){

						memset(&key, 0, sizeof(DBT));
						memset(&value, 0, sizeof(DBT));
						//					memset(&vtmp, 0, sizeof(DBT));
						//					if (DEBUG == 1) printf("one\n");

						key.data = res_p[i];
						key.size = sizeof(char) * (strlen(res_p[i]) + 1);
						key.flags = DB_DBT_MALLOC;
						//					if (DEBUG == 1) printf("two\n");

						if (strchr(res_p[i], '"') != NULL){
							value.data = &node_id_odd;
						} else {
							value.data = &node_id;
						}

						value.size = sizeof(id_type);
						//					if (DEBUG == 1) printf("three\n");

						vtmp.size = sizeof(id_type);
						//					if (DEBUG == 1) printf("four\n");
						vtmp.flags = DB_DBT_MALLOC;

						//					if (DEBUG == 1) printf("Thread %d: inserting : key ", tid);
						//					if (DEBUG == 1) print_dbt((void*)key.data, CHAR_P);
						//					if (DEBUG == 1) printf("\t and value: ");
						//					if (DEBUG == 1) print_dbt((void*)value.data, ID_TYPE);
						//					if (DEBUG == 1) printf("\n");

						if (tmp_dbs_enabled == 0){
							(void)mutex_lock(&thread_node_id_lock);
						}
						ret = tmp_db_p->get(tmp_db_p, 0, &key, &vtmp, 0);

						if (ret == DB_NOTFOUND){
							ret = tmp_db_p->put(tmp_db_p, 0, &key, &value, 0);
							if (strchr(res_p[i], '"') != NULL){
								node_id_odd += 2;
							} else {
								node_id += 2;
							}
							//						if (DEBUG == 1) printf("Thread %d: node_id %lld\n", tid, *(id_type*)value.data);
							if (ret != 0){
								printf("insert error for key:");
								print_dbt((void*)key.data, CHAR_P);
								printf("\t and value: ");
								print_dbt((void*)value.data, ID_TYPE);
								printf("\n");
								close_dbs();
								exit (ret);
							}
						} else {
							free(vtmp.data);
						}
						//				if (DEBUG == 1) printf("finished inserting %s\t%d\n", key.data, *(long *)value.data);
						if (tmp_dbs_enabled == 0){
							(void)mutex_unlock(&thread_node_id_lock);
						}

						free(res_p[i]);
					} else {
						printf("This triple %s is invalid\n", linebuff);
					}
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
		if (tmp_dbs_enabled == 1){
			if (DEBUG == 1) printf("Thread %d: Printing temporary dictionary db.\n", tid);
			if (DEBUG == 1) print_db(tmp_db_p, CHAR_P, ID_TYPE, 10);

			/* Putting the content of this db to the main dict db*/
			(void)mutex_lock(&write_dict_db_lock);
			if (DEBUG == 1) printf("Thread %d: get the merge dict lock\n", tid);
			merge_dbs(tmp_db_p, dict_db_p);
			(void)mutex_unlock(&write_dict_db_lock);
			if (DEBUG == 1) printf("Thread %d: released the merge dict lock\n", tid);

			remove_tmp_db(tmp_db_p, tmp_db_name, 0);

		}

		gettimeofday(&tvEnd, NULL);
		if (DEBUG == 1) timeval_print(&tvEnd);
		timeval_subtract(&tvDiff, &tvEnd, &tvBegin);
		if (DEBUG == 1) printf("Processed %s: %ld.%06ld sec.\n", rdf_dfile, (long int)tvDiff.tv_sec, (long int)tvDiff.tv_usec);
	}
	free(tmp_db_name);

	if (DEBUG == 1) printf("Thread %d: Done processing.\n", tid);
	return 0;
	err:
	/* Close our database handle, if it was opened. */
	close_dbs();
	exit(0);
}

//int process_triple(DB* tmp_dict_p, DB* tmp_data_p, id_type)

int put_data_item(DB* dbp, id_type sub, id_type pred, id_type obj){
	int ret = -1;
	int update = 0;

	if (dbp != NULL){
		DBT key, value;
		memset(&key, 0, sizeof(DBT));
		memset(&value, 0, sizeof(DBT));

		POID *po = malloc(sizeof(POID));
		po->predicate = pred;
		po->object = obj;
		id_type subject = sub;

		key.size = sizeof(id_type);
		key.flags = DB_DBT_MALLOC;
		key.data = &subject;

		value.flags = DB_DBT_MALLOC;
		value.size = sizeof(POID);
		value.data = po;

		//		if (DEBUG == 1) {
		//			printf("Putting data item key: ");
		//			print_dbt((void*)key.data, ID_TYPE);
		//			printf("\t value: ");
		//			print_dbt((void*)value.data, PO_ID);
		//			printf("\n");
		//		}

		ret = dbp->put(dbp, 0, &key, &value, 0);

		if (ret != 0){
			printf("insert error for key:");
			print_dbt((void*)key.data, ID_TYPE);
			printf("\t and value: ");
			print_dbt((void*)value.data, PO_ID);
			printf("\n");
			close_dbs();
			exit (ret);
		}
		//		if (DEBUG == 1) {
		//			printf("Inserted data item key: ");
		//			print_dbt(key.data, ID_TYPE);
		//			printf("\t value: ");
		//			print_dbt(value.data, PO_ID);
		//			printf("\n");
		//		}

	}


	return ret;
}


int put_dict_item(DB* dbp, char* resource, id_type id){
	int ret = -1;
	int update = 0;

	DBT key, value;
	memset(&key, 0, sizeof(DBT));
	memset(&value, 0, sizeof(DBT));

	key.size = strlen(resource) + 1;
	key.flags = DB_DBT_MALLOC;
	key.data = resource;

	value.flags = DB_DBT_MALLOC;
	value.size = sizeof(id_type);
	value.data = &id;

	//	if (DEBUG == 1) {
	//		printf("Getting key: ");
	//		printf(ID_TYPE_FORMAT, id);
	//		printf("\t previd: ");
	//		printf(ID_TYPE_FORMAT, prev);
	//		printf("\t dis: %d\n", dis);
	//	}
	//
	ret = dbp->put(dbp, 0, &key, &value, 0);

	if (ret != 0){
		printf("insert error for key:");
		print_dbt((void*)key.data, CHAR_P);
		printf("\t and value: ");
		print_dbt((void*)value.data, ID_TYPE);
		printf("\n");
		close_dbs();
		exit (ret);
	}
	//	if (DEBUG == 1) {
	//		printf("Putting dict item key: ");
	//		print_dbt(key.data, CHAR_P);
	//		printf("\t value: ");
	//		print_dbt(value.data, ID_TYPE);
	//		printf("\n");
	//	}

	return ret;
}

int gen_stat_db(DB* dbin, DB *dbout){
	struct timeval start, end, diff;
	gettimeofday(&start, NULL);
	/* Setting up the BerkeleyDB parameters for bulk insert */
	DBT key, data, ktmp, vtmp, k, v;
	int ret, ret_t;
	void *ptrk, *ptrd;
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	u_int32_t flag;
	flag = DB_MULTIPLE_KEY;

	key.ulen = bulk_size * 1024 * 1024;
	key.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	key.data = malloc(key.ulen);
	memset(key.data, 0, key.ulen);

	data.ulen = 1 * 1024 * 1024;
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
	//	if (DEBUG == 1) printf("inserting reverse item");

	id_type cur_key, next_key;
	memset(&ktmp, 0, sizeof(DBT));
	memset(&vtmp, 0, sizeof(DBT));
	ret = cursorp->c_get(cursorp, &ktmp, &vtmp, DB_NEXT);
	cur_key = *(id_type*) ktmp.data;
	next_key = *(id_type*) ktmp.data;
	long count = 1;
	do {
		memset(&ktmp, 0, sizeof(DBT));
		memset(&vtmp, 0, sizeof(DBT));
		ret = cursorp->c_get(cursorp, &ktmp, &vtmp, DB_NEXT);
		switch (ret) {
		case 0:
			next_key = *(id_type*) ktmp.data;
			//			if (DEBUG == 1) printf("Cur key is %lld : next key is : %lld \n", cur_key, next_key);
			if (cur_key != next_key){

				memset(&k, 0, sizeof(DBT));
				k.flags = DB_DBT_MALLOC;
				k.size = sizeof(id_type);
				k.data = &cur_key;

				memset(&v, 0, sizeof(DBT));
				v.flags = DB_DBT_MALLOC;
				v.size = sizeof(long);
				v.data = &count;

				DB_MULTIPLE_KEY_WRITE_NEXT(ptrk, &key, k.data, k.size, v.data, v.size);
				assert(ptrk != NULL);
				//				if (DEBUG == 1) printf("Inserting : key ");
				//				print_dbt((void*)k.data, ID_TYPE);
				//				if (DEBUG == 1) printf("\t and value: ");
				//				print_dbt((void*)v.data, ID_TYPE);
				//				if (DEBUG == 1) printf("\n");

				if ((num + 1) % UPDATES_PER_BULK_PUT == 0) {
					//					if (DEBUG == 1) printf("flush buffer to file\n");
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
			//			if (DEBUG == 1) printf("not found\n");
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
			//			if (DEBUG == 1) printf("Inserting last : key ");
			//			if (DEBUG == 1) print_dbt((void*)k.data, ID_TYPE);
			//			if (DEBUG == 1) printf("\t and value: ");
			//			if (DEBUG == 1) print_dbt((void*)v.data, ID_TYPE);
			//			if (DEBUG == 1) printf("\n");

			break;
		default:
			dbin->err(dbin, ret, "Gen_stat unspecified error");
			goto err;
		}
	} while (ret == 0);

	if (num % UPDATES_PER_BULK_PUT != 0) {
		//		if (DEBUG == 1) printf("Committed last buffer of data.\n");
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
	//	if (DEBUG == 1) printf("freeing gen_stat_db\n");
	free(key.data);
	free(data.data);
	//	if (DEBUG == 1) printf("finish gen_stat_db\n");
	gettimeofday(&end, NULL);
	timeval_subtract(&diff, &end, &start);
	if (DEBUG == 1) timeval_print(&end);
	if (DEBUG == 1) printf("Finished generating stat in: %ld.%06ld sec.\n", (long int)diff.tv_sec, (long int)diff.tv_usec);
	return 0;

	err: if (cursorp != NULL) {
		ret = cursorp->close(cursorp);
		if (ret != 0) {
			dbin->err(dbin, ret, "Gen_stat: cursor close failed.");
		}
	}
//	close_dbs();
}


char *lookup_id_reverse_full_form(DB *db, id_type id){
	int ret;
	DBT key, value;

	char *str = NULL;
	memset(&key, 0, sizeof(DBT));
	memset(&value, 0, sizeof(DBT));

//	if (DEBUG == 1){
//		printf("Looking for id ");
//		printf(ID_TYPE_FORMAT, id);
//		printf("\n");
//	}
	key.size = sizeof(id_type);
	key.data = &id;
	key.flags = DB_DBT_MALLOC;

	value.flags = DB_DBT_MALLOC;

	ret = db->get(db, 0, &key, &value, 0);

	if (ret == 0){
		str = get_full_form_term_from_db((char*)value.data, 0);
		free(value.data);
//		str = (char*)value.data;
	}

//	if (DEBUG == 1){
//		printf("Got full term %s \n", str);
//	}
	return str;
}

char *lookup_id_reverse(DB *db, id_type id){
	int ret;
	DBT key, value;

	char *str = NULL;
	memset(&key, 0, sizeof(DBT));
	memset(&value, 0, sizeof(DBT));

//	if (DEBUG == 1){
//		printf("Looking for id ");
//		printf(ID_TYPE_FORMAT, id);
//		printf("\n");
//	}
	key.size = sizeof(id_type);
	key.data = &id;
	key.flags = DB_DBT_MALLOC;

	value.flags = DB_DBT_MALLOC;

	ret = db->get(db, 0, &key, &value, 0);

	if (ret == 0){
		str = (char*) value.data;
	}
//	if (DEBUG == 1){
//		printf("Got full term %s \n", str);
//	}
	return str;
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

//	if (DEBUG == 1) printf("Getting shorted term for :%s:\n", str);
	char *newstr = get_short_form_term_from_db(str);
//	char *newstr = str;
	if (newstr != NULL){
//		if (DEBUG == 1) printf("Looking for shorted term %s\n", newstr);
		key.size = sizeof(char) * (strlen(newstr) + 1);
		key.data = newstr;

		value.flags = DB_DBT_MALLOC;

		//	if (DEBUG == 1) printf("Lookup id for:%s\n",str);
		ret = db->get(db, 0, &key, &value, 0);
		//	if (DEBUG == 1) printf(".....:");

		if (ret == 0){
			id = *(id_type*) value.data;
			//		if (DEBUG == 1) printf("Found:");
			//		if (DEBUG == 1) printf(ID_TYPE_FORMAT, id);
			//		if (DEBUG == 1) printf("\n");
		} else ret = -1;
		free(value.data);
		free(newstr);

		return id;
	} else {
		return -1;
	}
}


char* lookup_prefix(char *str){
	char* pref = NULL;
	int ret;
	DBT key, value;
	id_type id;

	memset(&key, 0, sizeof(DBT));
	memset(&value, 0, sizeof(DBT));

	key.size = sizeof(char) * (strlen(str) + 1);
	key.data = str;

	value.flags = DB_DBT_MALLOC;

	//	if (DEBUG == 1) printf("Lookup id for:%s\n",str);
	ret = dict_prefixes_db_p->get(dict_prefixes_db_p, 0, &key, &value, 0);
	//	if (DEBUG == 1) printf(".....:");

	if (ret == 0){
		pref = (char*) value.data;
		//		if (DEBUG == 1) printf("Found:");
		//		if (DEBUG == 1) printf(ID_TYPE_FORMAT, id);
		//		if (DEBUG == 1) printf("\n");
	}

	return pref;
}

char* lookup_prefixuri(char *str){
	char* pref = NULL;
	int ret;
	DBT key, value;
	id_type id;

	memset(&key, 0, sizeof(DBT));
	memset(&value, 0, sizeof(DBT));

	key.size = sizeof(char) * (strlen(str) + 1);
	key.data = str;

	value.flags = DB_DBT_MALLOC;

	//	if (DEBUG == 1) printf("Lookup id for:%s\n",str);
	ret = dict_prefixes_db_p->get(dict_prefixes_db_p, 0, &key, &value, 0);
	//	if (DEBUG == 1) printf(".....:");

	if (ret == 0){
		pref = (char*) value.data;
		free(value.data);
		//		if (DEBUG == 1) printf("Found:");
		//		if (DEBUG == 1) printf(ID_TYPE_FORMAT, id);
		//		if (DEBUG == 1) printf("\n");
	}

	return pref;
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

	//	if (DEBUG == 1) {
	//		printf("Lookup id for:");
	//		printf(ID_TYPE_FORMAT, id);
	//		printf("\n");
	//	}
	ret = db->get(db, 0, &key, &value, 0);
	//	if (DEBUG == 1) printf(".....:");

	if (ret == DB_NOTFOUND){
		id = -1;
	} else {
		id = *(id_type*) value.data;
		free(value.data);
		//				if (DEBUG == 1) printf("Found:");
		//				if (DEBUG == 1) printf(ID_TYPE_FORMAT, id);
		//				if (DEBUG == 1) printf("\n");
	}
	return id;
}


void* load_data_db_pthread(void* thread_id) {
	int tid = (intptr_t)thread_id;
	if (tid > num_files){
		if (DEBUG == 1) printf("Thread %d: no more file to process. Done.\n", tid);
		return 0;
	}
	int file;
	struct timeval tvBegin, tvEnd, tvDiff;
	if (DEBUG == 1) printf("Thread %d with ID %ld started\n", tid, (long)pthread_self());
	DBT key, value, vtmp;
	int ret, ret_t;

	DB *tmp_db_p;
	id_type node_id = tid * 10000000000 + 2;
	id_type node_id_odd = node_id + 1;
	if (DEBUG == 1) printf("URI's ID starts at ");
	if (DEBUG == 1) printf(ID_TYPE_FORMAT, node_id);
	if (DEBUG == 1) printf("\t Literal's ID starts at ");
	if (DEBUG == 1) printf(ID_TYPE_FORMAT, node_id_odd);
	if (DEBUG == 1) printf("\n");

	char *tmp_db_name;
	char strtid[4];
	sprintf(strtid, "%d", tid);
	tmp_db_name = malloc(strlen(strtid) + 12 * sizeof(char));
	strcpy(tmp_db_name, "tmp_data");
	strcat(tmp_db_name, strtid);
	strcat(tmp_db_name, "_db");

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
		if (DEBUG == 1) timeval_print(&tvBegin);
		rdf_dfile = files_p[file];

		/* Init a temporary database*/
		/* Initialize the DB handle */
		if (tmp_dbs_enabled == 1){

			if (DEBUG == 1) printf("Thread %d: Open temporary data db %s\n", tid, tmp_db_name);

			tmp_db_p = init_tmp_db(tmp_db_name, 0, 1);

		} else {
			tmp_db_p = data_s2po_db_p;
		}
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
			exit (-1);
		}

		/*
		 * Create a cursor for the dictionary database
		 */
		if (DEBUG == 1) printf("Start loading file %s ...\n", rdf_dfile);

		/* Iterate over the input RDF file*/
		while ((linelen = getline(&linebuff, &linesize, ifp)) > 0) {
			if (linebuff[0] == '#') {
				// Skip the comment line
			} else if (linelen > 2){
				//				if (DEBUG == 1) printf("Start processing line %s\n", linebuff);
				//			if (DEBUG == 1) printf("Start processing line %d of len %d and of size %d...\n", line, linelen, linesize);
				//				if (DEBUG == 1) printf("Line: %d\n", line);
				for (i = 0; i < linelen; i++) {
					/*
					 * Identify the term from start_term to end_term
					 * If the cur pointer is within subject, predicate, or object
					 */
					if (isblank(linebuff[i])){
						//						if (DEBUG == 1) printf("Thread %d: char %c at %d is blank \n", tid, linebuff[i], i);
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
								//								if (DEBUG == 1) printf("Thread %d: from %d to %d with spo %d literal %d\n", tid, (long)start_term_idx, (long)end_term_idx, spo, literal);

								/* Extract the found term */
								res_p[spo] = NULL;
								res_p[spo] = extract_term(linebuff, start_term_idx, end_term_idx);

								//								if (DEBUG == 1) printf("Thread %d: Found spo %d\t %s \n", tid, spo, res_p[spo]);

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
							//							if (DEBUG == 1) printf("spo %d start at %d with %c\n", spo, (int)i, linebuff[i]);
							term_started = 1;
							start_term_idx = i;
							// Identify the literal for object
						}
						if (linebuff[i] == '"' && (( linebuff[i-1] != '\\' ) || (linebuff[i-1] == '\\' && linebuff[i-2] == '\\'))){
							literal++;
						}
						if (linebuff[i] == ':' && linebuff[i-1] == '_'){
							bnode = 1;
							term_started = 1;
							start_term_idx = i - 1;
						}
					}
				} // Finish identifying the resources in the line

				//				if (DEBUG == 1) printf("Thread %d: %s\t%s\t%s\t\n", tid, res_p[0], res_p[1], res_p[2]);

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
				//					if (DEBUG == 1) printf("one\n");

				key.data = &subject;
				key.size = sizeof(id_type);
				value.data = po;
				value.size = sizeof(POID);

				//					if (DEBUG == 1) printf("Putting key: ");
				//					if (DEBUG == 1) print_dbt((void*)key.data, ID_TYPE);
				//					if (DEBUG == 1) printf("\t and value: ");
				//					if (DEBUG == 1) print_dbt((void*)value.data, PO_ID);
				//					if (DEBUG == 1) printf("\n");

				if (tmp_dbs_enabled == 0){
					(void)mutex_unlock(&thread_node_id_lock);
				}

				ret = tmp_db_p->put(tmp_db_p, 0, &key, &value, 0);
				if (ret != 0){
					printf("insert error for key:");
					print_dbt((void*)key.data, ID_TYPE);
					printf("\t and value: ");
					print_dbt((void*)value.data, PO_ID);
					printf("\n");

					close_dbs();
					exit (ret);
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

		if (tmp_dbs_enabled == 1){
			if (DEBUG == 1) printf("Thread %d: Printing temporary data db.\n", tid);
			if (DEBUG == 1) print_db(tmp_db_p, ID_TYPE, PO_ID,10);

			/* Putting the content of this db to the main dict db*/
			(void)mutex_lock(&write_data_s2po_db_lock);
			if (DEBUG == 1) printf("Thread %d: get the merge data lock\n", tid);
			merge_dbs(tmp_db_p, data_s2po_db_p);
			(void)mutex_unlock(&write_data_s2po_db_lock);
			if (DEBUG == 1) printf("Thread %d: released the merge datalock\n", tid);

			remove_tmp_db(tmp_db_p, tmp_db_name, 0);
		}

		gettimeofday(&tvEnd, NULL);
		if (DEBUG == 1) timeval_print(&tvEnd);
		timeval_subtract(&tvDiff, &tvEnd, &tvBegin);
		if (DEBUG == 1) printf("Processed %s: %ld.%06ld sec.\n", rdf_dfile, (long int)tvDiff.tv_sec, (long int)tvDiff.tv_usec);
	}
	free(tmp_db_name);

	if (DEBUG == 1) printf("Thread %d: Done processing.\n", tid);
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

	key.ulen = bulk_size * 1024 * 1024;
	key.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	key.data = malloc(key.ulen);
	memset(key.data, 0, key.ulen);

	data.ulen = 1 * 1024 * 1024;
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
	//	if (DEBUG == 1) printf("inserting reverse item");
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
			//				if (DEBUG == 1) printf("Insert key: %s, \t data: %lld\n", res_p[i], cur_node_id);

			if ((num + 1) % UPDATES_PER_BULK_PUT == 0) {
				//					if (DEBUG == 1) printf("flush buffer to file\n");
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
		//		if (DEBUG == 1) printf("Committed last buffer of data.\n");
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


