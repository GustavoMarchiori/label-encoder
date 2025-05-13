#include "dataset_data.hpp"


DatasetData Data;

void setChunkSize() {
    Data.chunkSize = 100 * 1024 * 1024;
}

void reserveBufferMemory() {
    setChunkSize();
    Data.buffer.resize(Data.chunkSize);
}