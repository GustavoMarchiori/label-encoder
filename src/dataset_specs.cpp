#include "../include/dataset_specs.hpp"
using namespace std;


dataset::Stream Stream;
dataset::Metadata Metadata;
dataset::Data Data;

void configFile(OperationMode opMode) {
    switch (opMode) {
        case READ:
            Stream.opMode = ios::in;
            break;
        case BIN_READ:
            Stream.opMode = ios::in | ios::binary;
            break;
        case WRITE:
            Stream.opMode = ios::out | ios::app;
            break;
        default:
            break;
    }
}

void openFile(string path, OperationMode opMode) {
    configFile(opMode);

    Stream.file = fstream(path.c_str(), Stream.opMode);
    if (!Stream.file.is_open()) {
        cout << "Arquivo não encontrado!" << endl;
        exit(1);
    }
}

void getSizeInBytes() {
    Stream.file.seekg(0, ios::end);
    Metadata.size = Stream.file.tellg();
}

void getColumnsHeaders() {
    string line, columnHeader;

    Stream.file.seekg(0, ios::beg);
    getline(Stream.file, line);

    getColumnsCount(line);
    Data.columnsHeaders.reserve(Data.columnsCount);

    stringstream columns(line);
    while (getline(columns, columnHeader, ',')) Data.columnsHeaders.push_back(columnHeader);
}

void getColumnsCount(string line) {
    Data.columnsCount = count(line.begin(), line.end(), ',') + 1;
}

void detectCategoricalColumns() {
    Data.categoricalColumns.resize(Data.columnsCount, " ");
    Stream.id_files.resize(Data.categoricalColumns.size());
    regex pattern = regex("[^0-9,.\\s]");
    string columnValue;

    for (size_t i = 0; i < 5; i++) {
        for (size_t j = 0; j < Data.columnsCount; j++) {
            j % Data.columnsCount != 0 ? getline(Stream.file, columnValue, ',') : getline(Stream.file, columnValue, '\n');
            if (Data.categoricalColumns[j] != " " && regex_search(columnValue, pattern)) {
                Data.categoricalColumns[j] = Data.columnsHeaders[j];
                Stream.id_files[j] = move(fstream(columnValue + "_table.csv", ios::out));
                Stream.id_files[j] << "ID," << columnValue << endl;
            }
        }   
    }
}

void closeFile() {
    Stream.file.close();
}