#include "dataset_info.hpp"


DatasetInfo Info;

void getSizeInBytes() {
    Settings.file.seekg(0, ios::end);
    Info.size = Settings.file.tellg();
}