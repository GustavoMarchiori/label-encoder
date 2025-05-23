#ifndef DATASET_SPECS_H
#define DATASET_SPECS_H

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <set>
#include <algorithm>
#include <regex>


enum OperationMode {
    READ,
    BIN_READ,
    WRITE
};

namespace dataset {

    struct Stream {
        std::fstream file;
        std::ios_base::openmode opMode;
        std::vector<std::fstream> id_files;
    };

    struct Metadata {
        std::streampos size;
    };

    struct Data {
        std::vector<std::string> columnsHeaders;
        size_t columnsCount;
        std::vector<std::string> categoricalColumns;
    };

}

extern dataset::Stream Stream;
extern dataset::Metadata Metadata;
extern dataset::Data Data;

void configFile(OperationMode opMode);

void openFile(std::string path, OperationMode opMode);

void getSizeInBytes();

void getColumnsHeaders();

void getColumnsCount(std::string line);

void detectCategoricalColumns();

void closeFile();

#endif