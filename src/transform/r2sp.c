/*
 * ============================================================================
 *
 * Name        : r2sp.c
 * Author      : Vinh Nguyen
 * Copyright (C) 2014 Vinh Nguyen @http://graphke.org
 * Copyright (C) 2014 Wright State University, USA @http://www.wright.edu/
 *
 * Description : Transform an RDF data file from using reification representation
 *  to using Singleton Property representation. More information about the Singleton Property,
 *  please refer to http://wiki.knoesis.org/RDF_Singleton_Property
 *  or the paper http://dl.acm.org/citation.cfm?id=2567973
 ============================================================================
 */


#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <regex.h>
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

#ifndef NULL
#define NULL   ((void *) 0)
#endif

/* The following is the size of a buffer to contain any error messages
   encountered when the regular expression is compiled. */

#define MAX_ERROR_MSG 0x1000
#define MAX_DESCRIPTIONS 100000000
#define MAX_PTHREADS 8
#define PTHREAD_ENABLED 1
#define PTHREAD_DISABLED 0
#define RDFXML 1
#define RDFXML_GZ 2
#define XMLNS_COLON 6
#define RDF_COLON_ID_QUOTE 7

int load_threads;
char **files_p;
char **outfiles_p;
int num_files;
char *sp;
int ndescriptions;
/* Compile the regular expression described by "regex_text" into
   "r". */

static int compile_regex (regex_t * r, const char * regex_text)
{
    int status = regcomp (r, regex_text, REG_EXTENDED|REG_NEWLINE);
    if (status != 0) {
	char error_message[MAX_ERROR_MSG];
	regerror (status, r, error_message, MAX_ERROR_MSG);
        printf ("Regex error compiling '%s': %s\n",
                 regex_text, error_message);
        return 1;
    }
    return 0;
}
char
*str_replace(char *orig, char *rep, char *with) {
    char *result; // the return string
    char *ins;    // the next insert point
    char *tmp;    // varies
    int len_rep;  // length of rep
    int len_with; // length of with
    int len_front; // distance between rep and end of last rep
    int count;    // number of replacements

    if (!orig)
        return NULL;
    if (!rep)
        rep = "";
    len_rep = strlen(rep);
    if (!with)
        with = "";
    len_with = strlen(with);

    ins = orig;
    for (count = 0; tmp = strstr(ins, rep); ++count) {
        ins = tmp + len_rep;
    }

    // first time through the loop, all the variable are set correctly
    // from here on,
    //    tmp points to the end of the result string
    //    ins points to the next occurrence of rep in orig
    //    orig points to the remainder of orig after "end of rep"
    tmp = result = malloc(strlen(orig) + (len_with - len_rep) * count + 1);

    if (!result)
        return NULL;

    while (count--) {
        ins = strstr(orig, rep);
        len_front = ins - orig;
        tmp = strncpy(tmp, orig, len_front) + len_front;
        tmp = strcpy(tmp, with) + len_with;
        orig += len_front + len_rep; // move to next "end of rep"
    }
    strcpy(tmp, orig);
    return result;
}

/*
  Match the string in "to_match" against the compiled regular
  expression in "r".
 */

static int match_regex (regex_t * r, char * to_match)
{
    /* "P" is a pointer into the string which points to the end of the
       previous match. */
    const char * p = to_match;
    /* "N_matches" is the maximum number of matches allowed. */
    const int n_matches = 10;
    /* "M" contains the matches found. */
    regmatch_t m[n_matches];

    while (1) {
        int i = 0;
        int nomatch = regexec (r, p, n_matches, m, 0);
        if (nomatch) {
            printf ("No more matches.\n");
            return nomatch;
        }
        for (i = 0; i < n_matches; i++) {
            int start;
            int finish;
            if (m[i].rm_so == -1) {
                break;
            }
            start = m[i].rm_so + (p - to_match);
            finish = m[i].rm_eo + (p - to_match);
            if (i == 0) {
                printf ("$& is ");
            }
            else {
                printf ("$%d is ", i);
            }
            printf ("'%.*s' (bytes %d:%d)\n", (finish - start),
                    to_match + start, start, finish);
        }
        p += m[0].rm_eo;
    }
    return 0;
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

int endsWith(char* base, char* str) {
    int blen = strlen(base);
    int slen = strlen(str);
    return (blen >= slen) && (0 == strcmp(base + blen - slen, str));
}

char *getPrefixNamespace(char *prefix, int k, char **prefixes, char **namespaces){
//	printf("getns:%s", prefix);
	char* ns;
	int i = 0;
	for (i = 0; i < k; i++){
//		printf("compare %s vs. %s", prefix, prefixes[i]);
		if (strcmp(prefix, prefixes[i]) == 0){
			return namespaces[i];
		}
	}
	return ns;
}

char *gen_new_ns_line(int size, const char* sp, char **prefixes, char **namespaces){

//	printf("num of prefixes: %d\n", size);
	char *rdfdoc = "<rdf:RDF";
	char *xmlns = "xmlns:";

	int i = 0;
	size_t len = strlen(rdfdoc);

	for (i = 0; i < size; i++){
		len += 1; // for the space
		if (strcmp(prefixes[i], "xmlns") != 0){
			len += strlen(xmlns); // for two double quote "s
		}
		len += strlen(prefixes[i]);
		len += 2; // for ="
		len += strlen(namespaces[i]);
		len += 1;
	}
	len += 1; // for the space
	len += strlen(xmlns) + 2; // for singleton property prefix
	len += 2; // for ="
	len += strlen(sp);
	len += 1; // for "
	len += 3; // for space and >
//	printf("len: %d\n", len);

	char *newrdfline = (char*) malloc((len + 1) * sizeof(char));
	strcpy(newrdfline, rdfdoc);

	for (i = 0; i < size; i++){
		strcat(newrdfline, " ");
		if (strcmp(prefixes[i], "xmlns") != 0){
			strcat(newrdfline, xmlns);
		}
		strcat(newrdfline, prefixes[i]);
		strcat(newrdfline, "=\"");
		strcat(newrdfline, namespaces[i]);
		strcat(newrdfline, "\"");
	}
	strcat(newrdfline, " xmlns:sp=\"");
	strcat(newrdfline, sp);
	strcat(newrdfline, "\" >\n");
//	printf("%s\n", newrdfline);
	return newrdfline;
}

void extract_kv(const char* str, int k, char **prefixes, char**namespaces){
	static char* rest1;
	char *tok, *ptr = str;
	int bool = 0;
	while ((tok = strtok_r(ptr, "=\"", &rest1)) != NULL && bool < 2){
		if (bool == 0){
			if (strstr(tok, ":") != NULL){
				prefixes[k] = malloc(strlen(tok + XMLNS_COLON) + 1);
				strcpy(prefixes[k], tok + XMLNS_COLON);
//				printf("%d non-xmlns prefix:%s\n", k,prefixes[k]);
			} else {
				prefixes[k] = malloc(strlen(tok) + 1);
				strcpy(prefixes[k], tok);
//				printf("%d xmlns prefix:%s\n", k, prefixes[k]);
			}
			bool++;
//			printf("Token bool 0 :%s\n", tok);
			ptr = rest1;
		} else if (bool == 1){
			namespaces[k] = malloc(strlen(tok) + 1);
			strcpy(namespaces[k], tok);
//			printf("%d ns :%s\n", k, namespaces[k]);
//			printf("Token bool 1 :%s\n", tok);
			bool++;
		}
	}
}


void process_ns(const char* linebuff, int size, char **prefixes, char **namespaces){
//	printf("processing namespace %s\n", linebuff);
//	printf("buffline: %s\n", linebuff);

	static char* rest3;
	char *ptr = linebuff;
	char* kv;
	int k = 0;
	while ((kv = strtok_r(ptr, " ", &rest3)) != NULL){
//		printf("KV :%s\n", kv);
		if (strstr(kv, "=\"") != NULL){
//			printf("kvtmp:%s",kvtmp);
			extract_kv(kv, k, prefixes, namespaces);
//			printf("%s:%s\n", prefixes[k], namespaces[k]);
			k++;
		}
		ptr = rest3;
//		printf("End KV :%s\n", kv);
	}

	// Verify the arrays
//	printf("Verifying array size: %d\n",k);
//	int i = 0;
//	for (i = 0; i < k; i++){
//		printf("%s:%s\n", prefixes[i], namespaces[i]);
//	}
//	printf("end printing\n");
}

int read_xml(void *threadid){
	int tid = (long) threadid;
	int file = tid;
	size_t linesize = 20000;
	char *linebuff = NULL;
	ssize_t linelen = 0;

	// Regular expression
    regex_t r, r1;
    const char * regex_text_rdf_id = "rdf:ID=";
    const char * regex_text_ns = "xmlns=\"";
    const char * p;
	compile_regex(& r, regex_text_rdf_id);
	compile_regex(& r1, regex_text_ns);
    /* "N_matches" is the maximum number of matches allowed. */
    const int n_matches = 1;
    /* "M" contains the matches found. */
    regmatch_t m[n_matches];

	// Reading files one by one until all files are processed

	for (file = tid; file < num_files; file = file + load_threads){
		char *fi_str = files_p[file];

		char *fo2_str = malloc(strlen(outfiles_p[file]) + 5);
		strcpy(fo2_str, outfiles_p[file]);
		strcat(fo2_str, "_sp0");
		printf("fo2: %s\n", fo2_str);

		FILE *fi = fopen(fi_str, "r");
		if (fi == NULL)
		{
		    printf("Error opening file!\n");
		    exit(-1);
		}

		FILE *fo2 = fopen(fo2_str, "w+");
		if (fo2 == NULL)
		{
		    printf("Error opening file at 2 %s!\n", fo2_str);
		    exit(-1);
		}

		int cnt = 0, i;
		char *generic_prop = NULL;
		char *singleton_prop = NULL;
		char *before_rdfid = NULL;
		char *after_rdfid = NULL;
		char *ending_tag = NULL;
		char *new_ending_tag = NULL;
		char *new_after_rdfid = NULL;
		int found_rei = 0, found_ns = 0;
		char ** prefixes, ** namespaces;
		int size = 0;
		char *newlinens;
		int ndesc = 1;
		long long fdesc = 1;
		char *fo1_str = malloc(strlen(outfiles_p[file]) + 5);
		strcpy(fo1_str, outfiles_p[file]);
		strcat(fo1_str, "_sp1");
		printf("fo1: %s\n", fo1_str);
		FILE *fo1 = fopen(fo1_str, "w+");
		if (fo1 == NULL)
		{
		    printf("Error opening file at 1 %s!\n", fo1_str);
		    exit(-1);
		}
		while ((linelen = getline(&linebuff, &linesize, fi)) > 0) {
			p = linebuff;
//			printf("linebuff:%s\n", linebuff);
			cnt++;
			found_rei = 0;
			char *line = malloc(linelen + 1);
			strcpy(line, linebuff);
			if (found_ns == 0){
				if (strstr(linebuff, "<rdf:RDF") != NULL){
					static char* rest2;
					char *kv1;
					char *ptr2 = line;
					while ((kv1 = strtok_r(ptr2, " ", &rest2)) != NULL){
						if (strstr(kv1, "=\"") != NULL){
							size++;
						}
				//		printf("KV1 :%s\n", kv1);
						ptr2 = rest2;
					}
				//	printf("k1: %d\n", k1);
					// Initialize the array
					prefixes = (char**) malloc(size * sizeof(char*));
					namespaces = (char**) malloc(size * sizeof(char*));
//					printf("linebuff after:%s\n", p);
					process_ns(linebuff, size, prefixes, namespaces);
					newlinens =  gen_new_ns_line(size, sp, prefixes, namespaces);
					fprintf(fo1, "%s", newlinens);
					fprintf(fo2, "%s", newlinens);
//					for (i = 0; i < size; i++){
//						printf("%s||%s\n", prefixes[i], namespaces[i]);
//					}
					found_ns++;
				} else if (strstr(linebuff, "<?xml") != NULL){
					fprintf(fo2, "%s", linebuff);
					fprintf(fo1, "%s", linebuff);
				}
				if (line != NULL) free(line);
			} else if (strstr(line, "<rdf:Description") != NULL){
				if (ndesc % ndescriptions == 0){
					fdesc++;

					// Close the current file
					fprintf(fo1, "</rdf:RDF>\n");
					fclose(fo1);
					free(fo1_str);

					// Create a new file
					char apx[5];
					sprintf(apx, "%lld", fdesc);
					fo1_str = malloc(strlen(outfiles_p[file]) + strlen(apx) + 3 + 1);
					strcpy(fo1_str, outfiles_p[file]);
					strcat(fo1_str, "_sp");
					strcat(fo1_str, apx);
					printf("Generating file : %s\n", fo1_str);

					// Open the new file
					fo1 = fopen(fo1_str, "w+");
					if (fo1 == NULL)
					{
					    printf("Error opening file at 1 %s!\n", fo1_str);
					    exit(-1);
					}
					fprintf(fo1, newlinens);
					// Reset the counter of descriptions
					ndesc = 0;
				}
				// Write to file
				fprintf(fo1, "%s", linebuff);
				if (line != NULL) free(line);
				ndesc++;
			} else {
				if (line != NULL){
					free(line);
				}
				p = linebuff;
				while (1) {
					int nomatch = regexec(&r, p, n_matches, m, 0);
					if (nomatch) {
	//					printf ("No matches for %s.\n", linebuff);
						break;
					} else {
						for (i = 0; i < n_matches; i++) {
							int start;
							int finish;
							if (m[i].rm_so == -1) {
								break;
							}
							start = m[i].rm_so + (p - linebuff);
							finish = m[i].rm_eo + (p - linebuff);
							generic_prop = NULL;
							singleton_prop = NULL;
							before_rdfid = NULL;
							after_rdfid = NULL;

							int accu_len = 0;
							// generic property
							char *chr_ptr = strchr(linebuff, ' ');
							int chr_pos = -1;
							if (chr_ptr != NULL)
							{
								chr_pos = chr_ptr - linebuff - 1;
							}
//							printf("%s:%s:%d\n", linebuff, chr_ptr, chr_pos);
//							printf("chr_pos for malloc:%d\n", chr_pos);
							if (chr_pos > 0){
								generic_prop = malloc(chr_pos*sizeof(char) + 1);
								strncpy(generic_prop, linebuff + 1, chr_pos);
								generic_prop[chr_pos] = '\0';
							}
//							accu_len += chr_pos + 1;
//							printf("generic:%d:%d\n", chr_pos, sizeof(generic_prop));

							// before rdf id
							before_rdfid = malloc((start - chr_pos - 2) * sizeof(char) + 1);
							strncpy(before_rdfid, linebuff + chr_pos + 1, start - chr_pos - 2);
							before_rdfid[start - chr_pos - 2] = '\0';
//							accu_len += start - chr_pos - 2;
//							accu_len += strlen("rdf:ID=\"");
//							printf("before:%d:%d:%s:\n", start - chr_pos - 2, strlen(before_rdfid), before_rdfid);

							// singleton property
							chr_ptr = strchr(linebuff + start + 8, '\"');
							chr_pos = -1;
							if (chr_ptr != NULL)
							{
								chr_pos = chr_ptr - (linebuff + start + 8);
							}
//							printf("sp start:%s\n", chr_ptr + 1);
							if (chr_pos > 0){
								singleton_prop = malloc(chr_pos*sizeof(char) + 1);
								strncpy(singleton_prop, linebuff + start + 8, chr_pos);
								singleton_prop[chr_pos] = '\0';
								accu_len = start + 8 + chr_pos + 1; // including " at the end of singleton property
							}
//							printf("len:%d\n", accu_alen);
//							printf("linelen:%d\n", linelen);
//							printf("linebuff:%s", linebuff);
//							printf("singleton:%d:%d\n", chr_pos, strlen(singleton_prop));

							// after rdf id
							after_rdfid = malloc(linelen - accu_len);
							strncpy(after_rdfid, linebuff + accu_len, linelen - accu_len - 1);
							after_rdfid[linelen - accu_len - 1] = '\0';
//							printf("after:%d:%d:%d:%d:%s\n", strlen(after_rdfid), accu_len, linelen, linelen - accu_len, after_rdfid);

							// handle the ending tag for singleton property
							ending_tag = malloc(strlen(generic_prop) + 5);
							strcpy(ending_tag, "</");
							strcat(ending_tag, generic_prop);
							strcat(ending_tag, ">\n");
//							printf("ending_tag:%d\n", strlen(ending_tag));

							if (strstr(after_rdfid, ending_tag) != NULL){
								size_t s = strlen(singleton_prop) + 7;
								new_ending_tag = malloc((s + 1) * sizeof(char));
								strcpy(new_ending_tag, "</sp:");
								strcat(new_ending_tag, singleton_prop);
								strcat(new_ending_tag, ">\n");
//								printf("old:%d:%s\n", strlen(after_rdfid), after_rdfid);
								new_after_rdfid = str_replace(after_rdfid, ending_tag, new_ending_tag);
//			        			printf("new:%d:%s\n", strlen(new_after_rdfid), new_after_rdfid);
								fprintf(fo1, "<sp:%s %s %s", singleton_prop, before_rdfid, new_after_rdfid);
								if (new_ending_tag != NULL) free(new_ending_tag);
								if (new_after_rdfid != NULL) free(new_after_rdfid);
							} else {
	//		        			printf("without new line \\n \n %s", ending_tag);
								fprintf(fo1, "<sp:%s %s %s", singleton_prop, before_rdfid, after_rdfid);
							}
//					        printf("%s|%s|%s|%s\n", generic_prop, before_rdfid, singleton_prop, after_rdfid);
							// Printing the line
							fprintf(fo2, "<rdf:Description rdf:about=\"%s%s\">\n", sp, singleton_prop);
//							printf("getting ns\n");
//							int m;
//							for (m = 0; m < size; m++){
//								printf("%s\t%s\n", prefixes[m], namespaces[m]);
//							}
							if (strstr(generic_prop, ":") != NULL){
								char *chr_ptr = strchr(generic_prop, ':');
								int chr_pos = -1;
								if (chr_ptr != NULL)
								{
									chr_pos = chr_ptr - generic_prop;
								}
								char* prefix = malloc((chr_pos + 1) * sizeof(char));
								strncpy(prefix, generic_prop, chr_pos);
								prefix[chr_pos] = '\0';
								char* prop = chr_ptr + 1;
//								printf("prefix:%s\t%s\n", prefix, generic_prop);
								char *prefixns = getPrefixNamespace(prefix, size, prefixes, namespaces);
//								printf("prefixns:%s\n", prefixns);
								fprintf(fo2, "<rdf:singletonPropertyOf rdf:resource=\"%s%s\" />\n", prefixns, prop);
								if (prefix != NULL) free(prefix);
							} else {
								char *prefixns = getPrefixNamespace("xmlns", size, prefixes, namespaces);
//								printf("xmlns:%s", prefixns);
								fprintf(fo2, "<rdf:singletonPropertyOf rdf:resource=\"%s%s\" />\n", prefixns, generic_prop);
							}
							fprintf(fo2, "</rdf:Description>\n");
							found_rei = 1;
							if (singleton_prop != NULL) free(singleton_prop);
							if (generic_prop != NULL) free(generic_prop);
							if (before_rdfid != NULL) free(before_rdfid);
							if (after_rdfid != NULL) free(after_rdfid);
							if (ending_tag != NULL) free(ending_tag);
						}
						p += m[0].rm_eo;

					}
				}
				if (found_rei == 0){
					fprintf(fo1, "%s", linebuff);
				}
			}
		}
		for (i = 0; i < size; i++){
			free(prefixes[i]);
			free(namespaces[i]);
		}
		free(prefixes);
		free(namespaces);
		if (linebuff != NULL){
			free(linebuff);
		}
		free(newlinens);
		free(fo1_str);
		free(fo2_str);
		fprintf(fo2, "</rdf:RDF>\n");
	    regfree (& r);
	    regfree (& r1);
	    fclose(fi);
	    fclose(fo1);
	    fclose(fo2);
	}
	return (0);
}

int load_dir(char *indir, char *outdir, int threads) {
	printf("Start loading %s directory...\n", indir);
	struct timeval tvBegin, tvEnd, tvDiff;

	DIR *d;
	struct dirent *entry;
	int i = 0, err, count = 0;
	void *status;

	d = opendir(indir);
	if (d) {
		while ((entry = readdir(d)) != NULL) {
			if (entry->d_type == DT_REG){
				printf("File %d: %s\n", count, entry->d_name);
				count++;
			}
		}
		closedir(d);
	}
	num_files = count;

	char *files[num_files];
	char *out_files[num_files];
	d = opendir(indir);
	i = 0;
	if (d) {
		while ((entry = readdir(d)) != NULL) {
			if (entry->d_type == DT_REG){
				files[i] = malloc(strlen(entry->d_name) + strlen(indir) + 2);
				strcpy(files[i], indir);
				strcat(files[i], "/");
				strcat(files[i], entry->d_name);
				out_files[i] = malloc(strlen(entry->d_name) + strlen(outdir) + 2);
				strcpy(out_files[i], outdir);
				strcat(out_files[i], "/");
				strcat(out_files[i], entry->d_name);
				//				strcpy(files[i], '\0');
				i++;
			}
		}
		closedir(d);
	}

	files_p = files;
	outfiles_p = out_files;
	/*
	 * Thread configuration
	 */
	pthread_attr_t attr;
	/* Initialize and set thread detached attribute */
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	printf("Start creating threads \n");

	pthread_t threadids[threads];
	i = 0;
	while (i < threads) {
		printf("Creating thread %d\n", i);
		err = pthread_create(&(threadids[i]), &attr, &read_xml, (void *)i);
		if (err != 0)
			printf("\ncan't create thread %d:[%s]", i, strerror(err));
		else
			printf("Creating thread %d successfully\n", i);
		i++;
	}

	pthread_attr_destroy(&attr);
	printf("Start joining threads \n");
	for (i = 0; i < threads; i++) {
		err = pthread_join(threadids[i], &status);
		if (err) {
			printf("ERROR; return code from pthread_join() is %d\n", err);
			exit(-1);
		}
	}
	printf("End threads.\n");

//	read_xml(0);
	/* Free the file array*/
	for (i = 0; i < num_files; i++){
		free(files[i]);
		free(out_files[i]);
	}

	printf("Done processing.\n");
	return (0);

}

int count_chr(const char *uri, char *chrs){
	int i = 0, cnt = 0;
	for (i = 0; i < strlen(uri); i++){
		if (strchr(chrs, uri[i])){
			cnt++;
		}
	}
	return cnt;
}

char *clean_uri(const char *uri, char *chrs){
	int i = 0, j = 0;
	int cnt = count_chr(uri, chrs);
	if (cnt > 0){
		char *new_uri = malloc((cnt + 1) * sizeof(char));
		for (i = 0; i < strlen(uri); i++){
			if (strchr(chrs, uri[i])){
				new_uri[j] = uri[i];
				j++;
			}
		}
		return new_uri;
	}
	return uri;
}

void print_header(){
	puts("---------------------------------------------------------------");
	puts("\tGRAPHKE: A graph engine for knowledge management");
	puts("\tCopyright(C) 2014 by Vinh Nguyen, vinh@knoesis.org.");
	puts("---------------------------------------------------------------");
}

int main(int argc, char ** argv)
{

	char * folder_in;
	char * folder_out;
//	printf("Total: %d\n1: %s \n2: %s\n 3: %s \n", argc, argv[1], argv[2], argv[3]);

	if (argc < 4 || argc == 1){
		/* prints From Reification to Singleton Property */
		puts("The purpose of this program is to convert RDF data file \nfrom reification representation into singleton property representation\n");
		puts("More information about the singleton property approach is available at http://wiki.knoesis.org/RDF_Singleton_Property\n");
		puts("Usage: r2sp base-uri-sp /path/to/folder/in /path/to/folder/out");
		puts("Arguments:");
		puts("\tbase-uri-sp\tA prefix URI for singleton properties");
		puts("\tpath-in\t\tInput folder contains the RDF files with reified representation");
		puts("\tpath-out\tOutput folder for RDF files with singleton property representation");
//		puts("\n");
		puts("Options");
		puts("\tnthreads\tNumber of threads to run concurrently");
		puts("\tndescriptions\tNumber of descriptions within an output file");
	} else {
//		printf("Getting parameters\n");
		sp = clean_uri(argv[1],"<>");
		printf("Singleton properties use %s as prefix.\n", sp);
		folder_in = argv[2];
		folder_out = argv[3];
		int t = atoi(argv[4]);
		if (argv[5] != NULL){
			load_threads = t;
		} else {
			load_threads = MAX_PTHREADS;
		}
		int d = atoi(argv[5]);
		if (argv[5] != NULL){
			ndescriptions = d;
		} else {
			ndescriptions = MAX_DESCRIPTIONS;
		}
		load_dir(folder_in, folder_out, load_threads);
	}
    return 0;
}
