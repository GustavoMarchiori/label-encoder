#include "dataset_acess.hpp"


DatasetFileSettings Settings;

void configFile(OperationMode opMode) {

    switch (opMode) {
        case READ:
            Settings.opMode = ios::in;
            break;
        case BIN_READ:
            Settings.opMode = ios::in | ios::binary;
            break;
        case WRITE:
            Settings.opMode = ios::out;
            break;
        default:
            break;
    }

}

void openFile(string path, OperationMode opMode) {
    configFile(opMode);

    Settings.file = fstream(path, Settings.opMode);

    if (!Settings.file.is_open()) {
        cout << "Arquivo não encontrado!" << endl;
        exit(1);
    }
}

void closeFile() {
    Settings.file.close();
}