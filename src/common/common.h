/*
 * common.h
 *
 *  Created on: Oct 22, 2014
 *      Author: vinh
 */

#ifndef COMMON_H_
#define COMMON_H_
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

/* Function declarations*/


u_int32_t hash(const void*, unsigned, u_int32_t);
void timeval_print(struct timeval*);
int timeval_subtract(struct timeval *result, struct timeval *t2, struct timeval *t1);
int count_data_files(char*);
char* get_current_dir(int debug);


#endif /* COMMON_H_ */
