#ifndef DATA_PAGE
#define DATA_PAGE

#include<bitset>
#include"pm_ehash.h"

using std::bitset;

#define DATA_PAGE_SLOT_NUM 16
// use pm_address to locate the data in the page

// uncompressed page format design to store the buckets of PmEHash
// one slot stores one bucket of PmEHash
typedef struct data_page {
    // fixed-size record design
    // uncompressed page format
    pm_bucket slots[DATA_PAGE_SLOT_NUM];
	bitset<DATA_PAGE_SLOT_NUM> bitmap;
} data_page;

#endif