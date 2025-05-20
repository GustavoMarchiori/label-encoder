#include "../include/buffer_control.hpp"
using namespace std;

thread::Chunk Chunk;

void setChunkSize(size_t size, DataUnit unit) {
    Chunk.size = size * pow(1024, static_cast<int>(unit));
}

void reserveBufferMemory(thread::Buffer& Buffer) {
    Buffer.lines.resize(Chunk.size);
}