#ifndef DATASET_INFO_H
#define DATASET_INFO_H

#include "dataset_acess.hpp"
#include <algorithm>

struct DatasetInfo {
    streampos size;
    size_t chunkSize;
};

extern DatasetInfo Info;

void getSizeInBytes();

#endif