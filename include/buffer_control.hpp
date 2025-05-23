#ifndef BUFFER_CONTROL_H
#define BUFFER_CONTROL_H

#include <iostream>
#include <string>
#include <cmath>

enum DataUnit {
    KB = 1,
    MB,
    GB
};

namespace thread {
    struct Buffer{
        std::string lines{};
    };

    struct Chunk {
        size_t size;
    };
}

extern thread::Chunk Chunk;
extern thread::Buffer Buffer;

void setChunkSize(size_t size, DataUnit unit);

void reserveBufferMemory(thread::Buffer& Buffer);

#endif