#ifndef DATASET_ACESS_H
#define DATASET_ACESS_H

#include <iostream>
#include <fstream>
#include <string>
using namespace std;


enum OperationMode {
    READ,
    BIN_READ,
    WRITE
};

struct DatasetFileSettings {
    fstream file;
    ios_base::openmode opMode;
};

extern DatasetFileSettings Settings;

void configFile(OperationMode opMode);

void openFile(string path, OperationMode opMode);

void closeFile();

#endif