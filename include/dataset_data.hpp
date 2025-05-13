#ifndef DATASET_DATA_H
#define DATASET_DATA_H

#include "dataset_info.hpp"

struct DatasetData {
    size_t chunkSize;
    string buffer{};
};

extern DatasetData Data;

void setChunkSize();

void reserveBufferMemory();

#endif