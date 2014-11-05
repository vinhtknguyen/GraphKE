/*
 * GraphKE

 * Copyrights 2014 Vinh Nguyen, Wright State University, USA.
 *
 */

#include <errno.h>
#include <readline/readline.h>
#include <readline/history.h>
#include<signal.h>
#include <graphke.h>

void print_header(){
	puts("---------------------------------------------------------------");
	puts("\tGraph engine for knowledge management");
	puts("\tCopyright(C) 2014 by Vinh Nguyen, vinh@knoesis.org.");
	puts("---------------------------------------------------------------");
}

void print_usage(){
	puts("\nSelect one of the following commands:\n");
	puts(" print_db_str\t Print key/value pairs in full URI term from a selected databases");
	puts(" print_db_id\t Print key/value pairs as internal id from a selected databases");
	puts(" print_key_value_str\t Print key/value pairs in full URI term from a selected key");
	puts(" print_key_value_id\t Print key/value pairs as internal id from a selected key");
	puts(" size \t\t Getting the size of a selected database");
	puts(" neighbors\t Find all the outgoing nodes");
	puts(" node_path\t\t Find the shortest node/triple path between two nodes");
	puts(" bulk_path\t\t Find the shortest node/triple path between every two nodes from a file\n"
			"\t\t\tThe file should be in the format <http://example.com/uri1>\t<http://example.com/uri2>\t ");
	puts("");
}
void INThandler(int);

void  INThandler(int sig)
{
     char  c;

     signal(sig, SIG_IGN);
     printf("\nOUCH, did you hit Ctrl-C?\n"
            "Do you really want to quit? [y/n] ");
     c = getchar();
     if (c == 'y' || c == 'Y'){
         close_dbs();
    	 exit(0);
     } else {
    	 signal(SIGINT, INThandler);
     }

	 printf(">\n");
     getchar(); // Get new line character
}

/* A static variable for holding the line. */
static char *line_read = (char *)NULL;

/* Read a string, and return a pointer to it.  Returns NULL on EOF. */
char *
rl_gets ()
{
  /* If the buffer has already been allocated, return the memory
     to the free pool. */
  /* Get a line from the user. */
  line_read = readline (">");

  /* If the line has any text in it, save it on the history. */
  if (line_read && *line_read)
    add_history (line_read);

  return (line_read);
}


char* remove_newline(char *s)
{
	int len = strlen(s);

	if (len > 0 && s[len-1] == '\n')  // if there's a newline
		s[len-1] = '\0';          // truncate the string

	return s;
}

char *trimwhitespace(char *str)
{
  char *end;

  // Trim leading space
  while(isspace(*str)) str++;

  if(*str == 0)  // All spaces?
    return str;

  // Trim trailing space
  end = str + strlen(str) - 1;
  while(end > str && isspace(*end)) end--;

  // Write new null terminator
  *(end+1) = 0;

  return str;
}
int main(int argc, char *argsv[]) {

	/* Input from user*/
	DEBUG = 0;
	print_header();
	char *db_name = argsv[1];
	char *db_home_str = get_current_dir(DEBUG);
	char *mode = argsv[2];

	if (DEBUG == 1) printf("Current directory is:%s\n", db_home_str);

	if (db_name == NULL){
		printf("Please specify a database to load\n");
		exit(0);
	}
	if (db_home_str != NULL){
		int ret;
		printf("Connecting to database %s ...\n", db_name);
		if (argsv[2] != NULL){
			if (strcmp(mode, "debug") == 0){
				printf("In debug mode\n");
				DEBUG = 1;
			} else {
				printf("In production mode\n");
				DEBUG = 0;
			}
		}
		u_int32_t db_flags = DB_THREAD | DB_RDONLY;

		ret = init_variables(db_name, db_home_str, DB_HASH, DEBUG);
		ret = init_dbs(db_name, db_flags);
		if (ret < 0){
			printf("Cannot access %s db. \n", db_name);
		} else {
			print_usage();
		    signal(SIGINT, INThandler);
			char *buffer = NULL, *para = NULL;  // getline will alloc
			int stop = 0;
			while(stop == 0) /* break with ^D or ^Z */
			{
			    signal(SIGINT, INThandler);
				buffer = readline (">");
				buffer = trimwhitespace(buffer);
				add_history(buffer);
				if (DEBUG == 1) printf("Got it:%s:\n", buffer);

				if ((para = strstr(buffer, "node_path")) != NULL){

					char *start = NULL, *end = NULL;
					// Getting the input
					start = readline ("start node:");
					start = trimwhitespace(start);
					add_history(start);
					if (DEBUG == 1) printf("Start at %s:\n", start);

					end = readline ("end node:");
					end = trimwhitespace(end);
					add_history(end);

					if (DEBUG == 1) printf("End at %s:\n", end);
					//						if (DEBUG == 1) printf("Finding the shortest path from %s to %s\n", start, end);

					struct timeval tvBegin, tvEndDict, tvEndReverse, tvDiff;
					gettimeofday(&tvBegin, NULL);
					if (DEBUG == 1) timeval_print(&tvBegin);
					int path = NODE_PATH;

					find_shortest_path(start, end, path);
					free(start);
					free(end);

					//end of generating dictionary
					gettimeofday(&tvEndDict, NULL);
					if (DEBUG == 1) timeval_print(&tvEndDict);

					// diff
					timeval_subtract(&tvDiff, &tvEndDict, &tvBegin);
					printf("Total: %ld.%06ld sec\n", (long int)tvDiff.tv_sec,
							(long int)tvDiff.tv_usec);
				} else if ((para = strstr(buffer, "nlan_path")) != NULL){

					char *start = NULL, *end = NULL;
					// Getting the input
					start = readline ("start node:");
					start = trimwhitespace(start);
					add_history(start);
					if (DEBUG == 1) printf("Start at %s:\n", start);

					end = readline ("end node:");
					end = trimwhitespace(end);
					add_history(end);

					if (DEBUG == 1) printf("End at %s:\n", end);
					//						if (DEBUG == 1) printf("Finding the shortest path from %s to %s\n", start, end);

					struct timeval tvBegin, tvEndDict, tvEndReverse, tvDiff;
					gettimeofday(&tvBegin, NULL);
					if (DEBUG == 1) timeval_print(&tvBegin);

					int path = NLAN_PATH;
					find_shortest_path(start, end, path);
					free(start);
					free(end);

					//end of generating dictionary
					gettimeofday(&tvEndDict, NULL);
					if (DEBUG == 1) timeval_print(&tvEndDict);

					// diff
					timeval_subtract(&tvDiff, &tvEndDict, &tvBegin);
					printf("Total: %ld.%06ld sec\n", (long int)tvDiff.tv_sec,
							(long int)tvDiff.tv_usec);
				} else if ((para = strstr(buffer, "generate_pairs")) != NULL){

					char *start = NULL, *end = NULL;
					// Getting the input
					start = readline ("filein:");
					start = trimwhitespace(start);
					add_history(start);
					if (DEBUG == 1) printf("File in %s:\n", start);

					end = readline ("fileout:");
					end = trimwhitespace(end);
					add_history(end);

					if (DEBUG == 1) printf("File out %s:\n", end);
					//						if (DEBUG == 1) printf("Finding the shortest path from %s to %s\n", start, end);

					struct timeval tvBegin, tvEndDict, tvEndReverse, tvDiff;
					gettimeofday(&tvBegin, NULL);
					if (DEBUG == 1) timeval_print(&tvBegin);


					generate_term_pairs(start, end);
					free(start);
					free(end);

					//end of generating dictionary
					gettimeofday(&tvEndDict, NULL);
					if (DEBUG == 1) timeval_print(&tvEndDict);

					// diff
					timeval_subtract(&tvDiff, &tvEndDict, &tvBegin);
					printf("Total: %ld.%06ld sec\n", (long int)tvDiff.tv_sec,
							(long int)tvDiff.tv_usec);
				} else if ((para = strstr(buffer, "neighbors")) != NULL){
					char *node = NULL;
					node = readline("node:");
					node = trimwhitespace(node);
					add_history(node);

					if (DEBUG == 1) printf("Getting neighbors for %s:\n", node);
					struct timeval tvBegin, tvEndDict, tvEndReverse, tvDiff;
					gettimeofday(&tvBegin, NULL);
					if (DEBUG == 1) timeval_print(&tvBegin);

					get_neighbors_str(node);
					free(node);

					//end of generating dictionary
					gettimeofday(&tvEndDict, NULL);
					if (DEBUG == 1) timeval_print(&tvEndDict);

					// diff
					timeval_subtract(&tvDiff, &tvEndDict, &tvBegin);
					printf("Total: %ld.%06ld sec\n", (long int)tvDiff.tv_sec,
							(long int)tvDiff.tv_usec);
				} else if (strstr(buffer, "get_neighbors_str") != NULL){

				} else if ((para = strstr(buffer, "print_db_id")) != NULL){
					char *db = NULL, *size_str = NULL;
					db = readline("db name [dict rdict dict_prefixes data_s2po stat_s2po]:");
					db = trimwhitespace(db);
					add_history(db);

					size_str = readline("how many items:");
					size_str = trimwhitespace(size_str);
					add_history(size_str);

					if (DEBUG == 1) printf("Printing %s items in db %s:\n", size_str, db);
					struct timeval tvBegin, tvEndDict, tvEndReverse, tvDiff;
					gettimeofday(&tvBegin, NULL);
					if (DEBUG == 1) timeval_print(&tvBegin);

					db_op(db, size_str, "print_db_id");
					free(db);
					free(size_str);

					//end of generating dictionary
					gettimeofday(&tvEndDict, NULL);
					if (DEBUG == 1) timeval_print(&tvEndDict);

					// diff
					timeval_subtract(&tvDiff, &tvEndDict, &tvBegin);
					printf("Total: %ld.%06ld sec\n", (long int)tvDiff.tv_sec,
							(long int)tvDiff.tv_usec);
				} else if ((para = strstr(buffer, "print_db_str")) != NULL){
					char *db = NULL, *size_str = NULL;
					db = readline("db name [dict rdict dict_prefixes data_s2po stat_s2po]:");
					add_history(db);

					size_str = readline("how many items:");
					size_str = trimwhitespace(size_str);
					add_history(size_str);

					if (DEBUG == 1) printf("Printing %s items in db %s:\n", size_str, db);
					struct timeval tvBegin, tvEndDict, tvEndReverse, tvDiff;
					gettimeofday(&tvBegin, NULL);
					if (DEBUG == 1) timeval_print(&tvBegin);

					db_op(db, size_str, "print_db_str");
					free(db);
					free(size_str);

					//end of generating dictionary
					gettimeofday(&tvEndDict, NULL);
					if (DEBUG == 1) timeval_print(&tvEndDict);

					// diff
					timeval_subtract(&tvDiff, &tvEndDict, &tvBegin);
					printf("Total: %ld.%06ld sec\n", (long int)tvDiff.tv_sec,
							(long int)tvDiff.tv_usec);
				} else if (strstr(buffer, "print_key_value_str") != NULL){
					char *db = NULL, *key = NULL;
					db = readline("db name [dict rdict dict_prefixes data_s2po stat_s2po]:");
					db = trimwhitespace(db);
					add_history(db);

					key = readline("key:");
					key = trimwhitespace(key);
					add_history(key);

					struct timeval tvBegin, tvEndDict, tvEndReverse, tvDiff;
					gettimeofday(&tvBegin, NULL);
					if (DEBUG == 1) timeval_print(&tvBegin);

					db_op(db, key, "print_key_value_str");
					free(db);
					free(key);

					//end of generating dictionary
					gettimeofday(&tvEndDict, NULL);
					if (DEBUG == 1) timeval_print(&tvEndDict);

					// diff
					timeval_subtract(&tvDiff, &tvEndDict, &tvBegin);
					printf("Total: %ld.%06ld sec\n", (long int)tvDiff.tv_sec,
							(long int)tvDiff.tv_usec);

				} else if (strstr(buffer, "print_key_value_id") != NULL){
					char *db = NULL, *key = NULL;
					db = readline("db name [dict rdict dict_prefixes data_s2po stat_s2po]:");
					db = trimwhitespace(db);
					add_history(db);

					key = readline("key id:");
					key = trimwhitespace(key);
					add_history(key);

					struct timeval tvBegin, tvEndDict, tvEndReverse, tvDiff;
					gettimeofday(&tvBegin, NULL);
					if (DEBUG == 1) timeval_print(&tvBegin);

					db_op(db, key, "print_key_value_id");
					free(db);
					free(key);

					//end of generating dictionary
					gettimeofday(&tvEndDict, NULL);
					if (DEBUG == 1) timeval_print(&tvEndDict);

					// diff
					timeval_subtract(&tvDiff, &tvEndDict, &tvBegin);
					printf("Total: %ld.%06ld sec\n", (long int)tvDiff.tv_sec,
							(long int)tvDiff.tv_usec);

				} else if (strstr(buffer, "getid") != NULL){
					char *str = NULL;
					str = readline("string:");
					str = trimwhitespace(str);
					add_history(str);

					struct timeval tvBegin, tvEndDict, tvEndReverse, tvDiff;
					gettimeofday(&tvBegin, NULL);

					printf(ID_TYPE_FORMAT,lookup_id(dict_db_p, str));
					printf("\n");

					free(str);

					//end of generating dictionary
					gettimeofday(&tvEndDict, NULL);
					if (DEBUG == 1) timeval_print(&tvEndDict);

					// diff
					timeval_subtract(&tvDiff, &tvEndDict, &tvBegin);
					printf("Total: %ld.%06ld sec\n", (long int)tvDiff.tv_sec,
							(long int)tvDiff.tv_usec);
//						printf("Size: %d\n", get_db_size(dict_db_p));
				}else if (strstr(buffer, "getresource") != NULL){
					char *str = NULL;
					str = readline("id:");
					add_history(str);

					struct timeval tvBegin, tvEndDict, tvEndReverse, tvDiff;
					gettimeofday(&tvBegin, NULL);

					char *e;
					id_type id = strtoll(str, &e, 0);
					e = lookup_id_reverse(rdict_db_p, id);
					if (e != NULL) printf("%s\n",e) ;
					free(str);
					free(e);

					//end of generating dictionary
					gettimeofday(&tvEndDict, NULL);
					if (DEBUG == 1) timeval_print(&tvEndDict);

					// diff
					timeval_subtract(&tvDiff, &tvEndDict, &tvBegin);
					printf("Total: %ld.%06ld sec\n", (long int)tvDiff.tv_sec,
							(long int)tvDiff.tv_usec);
//						printf("Size: %d\n", get_db_size(dict_db_p));
				} else if (strstr(buffer, "size") != NULL){
					char *db = NULL;
					db = readline("db name [dict rdict dict_prefixes data_s2po stat_s2po]:");
					db = trimwhitespace(db);
					add_history(db);

					struct timeval tvBegin, tvEndDict, tvEndReverse, tvDiff;
					gettimeofday(&tvBegin, NULL);

					db_op(db, 0, "size");
					free(db);

					//end of generating dictionary
					gettimeofday(&tvEndDict, NULL);
					if (DEBUG == 1) timeval_print(&tvEndDict);

					// diff
					timeval_subtract(&tvDiff, &tvEndDict, &tvBegin);
					printf("Total: %ld.%06ld sec\n", (long int)tvDiff.tv_sec,
							(long int)tvDiff.tv_usec);
//						printf("Size: %d\n", get_db_size(dict_db_p));
				} else if (strstr(buffer, "bulk_path") != NULL){
					char *filein, *fileout, *readthreads;
					filein = readline("file in:");
					filein = trimwhitespace(filein);
					add_history(filein);
					fileout = readline("file out:");
					fileout = trimwhitespace(fileout);
					add_history(fileout);
					readthreads = readline("nthreads[10]:");
					readthreads = trimwhitespace(readthreads);
					add_history(readthreads);

					struct timeval tvBegin, tvEndDict, tvEndReverse, tvDiff;
					gettimeofday(&tvBegin, NULL);

					int nthreads = atoi(readthreads);
					path_bulk(filein, fileout, nthreads);
					free(filein);
					free(fileout);
					free(readthreads);

					//end of generating dictionary
					gettimeofday(&tvEndDict, NULL);
					if (DEBUG == 1) timeval_print(&tvEndDict);

					// diff
					timeval_subtract(&tvDiff, &tvEndDict, &tvBegin);
					printf("Total: %ld.%06ld sec\n", (long int)tvDiff.tv_sec,
							(long int)tvDiff.tv_usec);
//						printf("Size: %d\n", get_db_size(dict_db_p));
				} else if (strstr(buffer, "quit") != NULL){
					printf("quitting ...");
					free(buffer);
					clear_history();
					stop = 1;
					break;
				} else {
					print_usage();
				}
				free(buffer);
			}
//			free(db_home_str);
			close_dbs();
			printf(" done.\nGood bye.\n");
		}
	}
	// begin

	exit(0);
	err:
	close_dbs();
}

