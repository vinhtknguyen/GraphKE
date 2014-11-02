/*
 * load_db.c
 * Using zlib to read the RDF file.
 *  Created on: Jun 30, 2014
 *      Author: Vinh Nguyen
 */

#include <graphke.h>

/* Function implementation */


print_header(){
	puts("GraphKE:\t A knowledge management");
	puts("Copyrighted by Vinh Nguyen, 2014");
}

print_usage(){
	puts("graphke_load db_name data_folder data_format");
	puts("Arguments\n");
	puts("\tdb_name \tName of the database to be created");
	puts("\tdata_folder\t Location to the data directory");
	puts("Options\n");
	puts("\tdata_format\t Format of data input, default is ntriples\n");

}

int main(int argc, char *argsv[]) {

	/* Validate Input from user*/

	if (argc < 3) {
		print_usage();
	} else {
		printf("Start the program\n");
		char *db_name = argsv[1];
		char *rdf_datadir = argsv[2]; /* Full path to the data directory */

		char *parser = "ntriples";
		// Pre-check the database

		int nrounds = 1;
		bulk_size = BULK_SIZE;
		DEBUG = 0;
		// Pre-check the data dir
		int count = count_data_files(rdf_datadir);
		if (count < 1){
			printf("No data file to load\n");
			exit(0);
		}

		char *env_home = get_current_dir(DEBUG);

		DBTYPE db_type = DB_HASH;
		int nthreads, debug;

		int opt = 3;
		while (argsv[opt] != NULL){
			if (DEBUG == 1) printf("Parsing option\n:");
			char *option;
			char *name;
			char *value;
			option = strtok(argsv[opt], "=");
			if (option != NULL){
				if (DEBUG == 1) printf("Option %s:", option);
				value = strtok(NULL, "=");
				if (DEBUG == 1) printf("\t Value %s:\n", value);
				if (value != NULL){
					if (strcmp(option, "nthreads") == 0){
						nthreads = atoi(value);
						if (nthreads < 1) {
							nthreads = (count > 3)?3:count;
						}
					} if (strcmp(option, "parser") == 0){
						parser = value;
					} else if (strcmp(option, "env_home") == 0){
						env_home = value;
					} else if (strcmp(option, "db_type") == 0){
						db_type = get_dbtype(value);
					} else if (strcmp(option, "nrounds") == 0){
						nrounds = atoi(value);
					} else if (strcmp(option, "debug") == 0){
						debug = atoi(value);
					} else if (strcmp(option, "bulksize") == 0){
						bulk_size = atoi(value);
					}
				}
			}
			opt++;
		}
		DEBUG = debug;

		printf("Data files to load: %d\n", count);
		printf("Threads to be created: %d\n", nthreads);
		printf("Environment home is at %s\n", env_home);
		printf("Initializing environment and databases\n");

		init_variables(db_name, env_home, db_type, DEBUG);

		u_int32_t flags = DB_THREAD | DB_CREATE;
		init_dbs(db_name, flags);

		// begin
		struct timeval tvBegin, tvEndDict, tvEndReverse, tvDiff;
		gettimeofday(&tvBegin, NULL);
		if (DEBUG == 1) timeval_print(&tvBegin);

		// Generate a dictionary
		if (DEBUG == 1) printf("Starting loading ... \n");

		if (nrounds == 1){
			load_dir_one_round_inmem(rdf_datadir, parser, nthreads, count);
		} else {
			load_dir_two_rounds(rdf_datadir, parser, nthreads, count);
		}


		if (DEBUG == 1) printf("Done.");

		//end of generating dictionary
		gettimeofday(&tvEndDict, NULL);
		if (DEBUG == 1) timeval_print(&tvEndDict);

		// diff
		timeval_subtract(&tvDiff, &tvEndDict, &tvBegin);
		printf("Total: %ld.%06ld sec\n", (long int)tvDiff.tv_sec,
				(long int)tvDiff.tv_usec);
	}

	return 0;
}

int count_data_files(char *rdf_datadir){
	DIR *d;
	struct dirent *entry;
	int i = 0, err, count = 0;
	void *status;

	d = opendir(rdf_datadir);
	if (d) {
		while ((entry = readdir(d)) != NULL) {
			if (entry->d_type == DT_REG){
				if (DEBUG == 1) printf("File %d: %s\n", count, entry->d_name);
				count++;
			}
		}
		closedir(d);
	}

	return count;

}

