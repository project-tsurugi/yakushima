#pragma once

#include <pthread.h>
#include <stdio.h>

#include <cstdlib>

#define CCC(val)  do {fprintf(stderr, "%ld %16s %4d %16s %16s: %c\n", (long int)pthread_self(), __FILE__, __LINE__, __func__, #val, val); fflush(stderr);} while (0)
#define DDD(val)  do {fprintf(stderr, "%ld %16s %4d %16s %16s: %d\n", (long int)pthread_self(), __FILE__, __LINE__, __func__, #val, val); fflush(stderr);} while (0)
#define PPP(val)  do {fprintf(stderr, "%ld %16s %4d %16s %16s: %p\n", (long int)pthread_self(), __FILE__, __LINE__, __func__, #val, val); fflush(stderr);} while (0)
#define LLL(val)  do {fprintf(stderr, "%ld %16s %4d %16s %16s: %ld\n", (long int)pthread_self(), __FILE__, __LINE__, __func__, #val, val); fflush(stderr);} while (0)
#define SSS(val)  do {fprintf(stderr, "%ld %16s %4d %16s %16s: %s\n", (long int)pthread_self(), __FILE__, __LINE__, __func__, #val, val); fflush(stderr);} while (0)
#define FFF(val)  do {fprintf(stderr, "%ld %16s %4d %16s %16s: %f\n", (long int)pthread_self(), __FILE__, __LINE__, __func__, #val, val); fflush(stderr);} while (0)
#define NNN       do {fprintf(stderr, "%ld\t%16s\t%4d\t%16s\n", (long int)pthread_self(), __FILE__, __LINE__, __func__); fflush(stderr);} while (0)
#define ERR     do {printf("ERROR:"); NNN; exit(1);} while (0)

#define pdebug(str) printf("%16s %4d %s\n", __FILE__, __LINE__, str)
