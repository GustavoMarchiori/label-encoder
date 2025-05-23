#include "../include/dataset_specs.hpp"
#include "../include/buffer_control.hpp"
#include <omp.h>
#include <unordered_map>
#include <chrono>
using namespace std;
using namespace chrono;


void readLines();

void abluble();

int main() {
    openFile("../data/dataset_00_sem_virg.csv", BIN_READ);
    getSizeInBytes();
    getColumnsHeaders();
    detectCategoricalColumns();
    setChunkSize(64, MB);

    openFile("../data/encoded_dataset.csv", WRITE);
    auto inicio = high_resolution_clock::now();
    readLines();
    abluble();
    auto fim = high_resolution_clock::now();
    duration<double> duracao = fim - inicio;

    cout << "Tempo de execucao: " << duracao.count() << " segundos\n";
    
    return 0;
}


void readLines() {
    Stream.file.seekg(0, ios::beg);
    reserveBufferMemory(Buffer);
    string garbage{};

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

void abluble() {
    stringstream lines(Buffer.lines);
    char delim;
    string columnValue;
    vector<unordered_map<string, int>> columnsValues;

    for (size_t i = 0; lines.peek() != EOF; i++) {
        delim = (i % Data.columnsCount - 1 != 0 ? ',' : '\n'); 
        getline(lines, columnValue, delim);
        
        if (Data.categoricalColumns[i] != " " && !columnsValues[i].count(columnValue)) {
            columnsValues[i][columnValue] = columnsValues[i].size();
            Stream.id_files[i] << columnsValues[i].size() << "," << columnValue << endl;
        }
        else if (Data.categoricalColumns[i] != " ") Stream.file << columnsValues[i][columnValue] << delim;
        else Stream.file << columnValue << delim;
    }
}