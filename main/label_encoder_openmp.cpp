#include "../include/dataset_specs.hpp"
#include "../include/buffer_control.hpp"
#include <omp.h>
#include <chrono>
using namespace std;
using namespace chrono;


void readLines();

int main() {
    openFile("../data/dataset_00_sem_virg.csv", BIN_READ);
    getSizeInBytes();
    getColumnsHeaders();
    detectCategoricalColumns();
    setChunkSize(64, MB);

    auto inicio = high_resolution_clock::now();

    readLines();
    /*
    Stream.file.seekg(0, ios::beg);
    string line;
    while (Stream.file.peek() != EOF)
    {
        getline(Stream.file, line);
    }
    */

    auto fim = high_resolution_clock::now();
    duration<double> duracao = fim - inicio;

    cout << "Tempo de execucao: " << duracao.count() << " segundos\n";
    

    return 0;
}


void readLines() {
    Stream.file.seekg(0, ios::beg);
    thread::Buffer Buffer;
    reserveBufferMemory(Buffer);

    for (size_t i = 0; i < Metadata.size; i += Chunk.size) {
        Stream.file.read(&Buffer.lines[0], Chunk.size);
    }
}