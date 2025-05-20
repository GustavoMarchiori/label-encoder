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
    auto fim = high_resolution_clock::now();
    duration<double> duracao = fim - inicio;

    cout << "Tempo de execucao: " << duracao.count() << " segundos\n";
    
    return 0;
}


void readLines() {
    Stream.file.seekg(0, ios::beg);
    thread::Buffer Buffer;
    reserveBufferMemory(Buffer);
    string garbage{};

    int i = 0;
    while (Stream.file.read(&Buffer.lines[0], Chunk.size) || Stream.file.gcount() > 0) {
        size_t bytesRead = Stream.file.gcount();

        Buffer.lines.insert(0, garbage);

        size_t lastNewlinePos = Buffer.lines.rfind('\n');

        if (lastNewlinePos != string::npos) {
            garbage = Buffer.lines.substr(lastNewlinePos + 1);
            Buffer.lines.erase(lastNewlinePos + 1);
        }
    }
}