/*
 * pqueue.h
 * 
 *  Created on: Oct 21, 2014
 *      Author: Vinh Nguyen
 */

#ifndef PQUEUE_H_
#define PQUEUE_H_
#include <stdio.h>
#include <stdlib.h>

typedef int pri_type;
typedef struct { void * data; int pri; } q_elem_t;
typedef struct { q_elem_t *buf; int n, alloc; } pri_queue_t, *pri_queue;


#define PRI_TYPE_FORMAT "%d";
#define priq_purge(q) (q)->n = 1
#define priq_size(q) ((q)->n - 1)

pri_queue priq_new(int);
void priq_push(pri_queue, void*, int);
void *priq_pop(pri_queue, int*);
void* priq_top(pri_queue q, int *pri);
void priq_combine(pri_queue q, pri_queue q2);

#endif /* PQUEUE_H_ */
