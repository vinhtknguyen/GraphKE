/*
 * common.c
 *
 *  Created on: Oct 22, 2014
 *      Author: vinh
 */

#include <common.h>

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


char* get_current_dir(int debug){
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

u_int32_t hash(DB* db, const void* buffer, unsigned length){
   char* key = (char*)(buffer);
   u_int32_t a=0x9E3779B9,b=0x9E3779B9,c=0;
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
