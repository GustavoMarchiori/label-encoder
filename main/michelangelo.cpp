#include <iostream>
#include <windows.h>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <set>
#include <map>
#include <cctype>
#include <limits>
#include <omp.h>
#include <memory>
using namespace std;
using integer = numeric_limits<int>;


int main() {
    //Arquivo
    HANDLE hFile = CreateFileA (
        "../data/dataset_00_sem_virg.csv",
        GENERIC_READ,
        FILE_SHARE_READ,
        NULL,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL,
        NULL
    );

    //Mapeamento do arquivo
    HANDLE hMap = CreateFileMappingA(
        hFile,
        NULL,
        PAGE_READONLY,
        0,
        0,
        NULL
    );

    //Tamanho do arquivo
    LARGE_INTEGER fileSize;
    GetFileSizeEx(hFile, &fileSize);

    //Conteúdo do arquivo
    const unsigned char* data = (const unsigned char*) MapViewOfFile(
        hMap,
        FILE_MAP_READ,
        0, 0, 0
    );

    //Ponteiros para mapear os valores do arquivo e os ponteiros de escrita
    const unsigned char *valuesBegin, *startReadPtr, *endReadPtr, *valuesEnd;

    valuesEnd = data + fileSize.QuadPart;
    valuesBegin = (const unsigned char*) memchr(data, '\n', (size_t)valuesEnd);

    //Leitura de headers
    string line(data, valuesBegin); 
    startReadPtr = valuesBegin + 1;

    size_t columnsCount = count(line.begin(), line.end(), ',') + 1;
    vector<string> columns; columns.resize(columnsCount); 

    for (size_t i = columns.size() - 1; i > 0; i--) {
        size_t index = line.rfind(',');
        columns[i] = move(line.substr(index + 1)); line.erase(index);
    }

    columns[0] = move(line);

    //Identificação de colunas categóricas
    set<size_t> categoricalColumns;
    set<unsigned char> uniqueChars;
    unsigned char delimitator;

    for(size_t i = 0; i < 5; i++) {
        for(size_t j = 0; j < columnsCount; j++) {
            uniqueChars.clear();

            delimitator = (j != columnsCount - 1) ? ',' : '\n';
            endReadPtr = (const unsigned char*) memchr(startReadPtr, delimitator, (size_t) (valuesEnd - startReadPtr));

            if (categoricalColumns.find(j) != categoricalColumns.end()) {startReadPtr = endReadPtr + 1; continue;}

            uniqueChars.insert(startReadPtr, endReadPtr);
            startReadPtr = endReadPtr + 1;

            if ((uniqueChars.size() == 1) && (*uniqueChars.begin() == '"')) continue;

            if (!isdigit(*uniqueChars.rbegin())) categoricalColumns.insert(j);
        }

        if (categoricalColumns.size() == columnsCount) break;
    }

    //Criando diretório dos arquivos finais
    CreateDirectoryA("../output", NULL);

    //Arbetura dos arquivos das colunas categóricas
    vector<unique_ptr<fstream>> encoded_files; encoded_files.resize(columnsCount);
    for(auto&& categoricalColumn : categoricalColumns) {
        encoded_files[categoricalColumn] = make_unique<fstream>("../output/id_" + columns[categoricalColumn] + ".csv", ios::out);
        *encoded_files[categoricalColumn] << "id," + columns[categoricalColumn] << endl;
        columns[categoricalColumn] = "id" + columns[categoricalColumn];
    }

    //Abertura do arquivo final
    fstream finalFile("../output/encoded_dataset.csv", ios::out);
    for (size_t i = 0; i < columnsCount - 1; i++) finalFile << columns[i] << ",";
    finalFile << columns[columnsCount - 1] << endl;

    //Memória
    MEMORYSTATUSEX mem;
    mem.dwLength = sizeof(mem);
    GlobalMemoryStatusEx(&mem);
    size_t ram = mem.ullAvailPhys * 0.25;

    // Chunk
    size_t numChunk = ((size_t)fileSize.QuadPart + ram - 1) / ram;

    //Mapeamento dos chunks
    vector<pair<const unsigned char*, const unsigned char*>> chunksPtrs;
    startReadPtr = valuesBegin + 1; endReadPtr = valuesBegin + 1;
    
    for (size_t i = 1; i <= numChunk; i++) {
        const unsigned char* tentativeEnd = startReadPtr + min(ram, (size_t)(valuesEnd - startReadPtr));

        if (tentativeEnd >= valuesEnd) {
            chunksPtrs.emplace_back(startReadPtr, valuesEnd);
            break;
        }

        const unsigned char* endReadPtr = (const unsigned char*) memchr(tentativeEnd, '\n', valuesEnd - tentativeEnd);

        chunksPtrs.emplace_back(startReadPtr, endReadPtr);
        startReadPtr = endReadPtr + 1;
    }

    //Leitura paralelizada
    vector<unordered_map<string, size_t>> uniqueColumnsValues; uniqueColumnsValues.resize(columnsCount);

    for (size_t i = 0; i < chunksPtrs.size(); i++) {
        const unsigned char *localFirst, *localSecond;
        localFirst = chunksPtrs[i].first; localSecond = localFirst;
        cout << "iteracao" << endl;
        string value; char delimitator;

        size_t range = (size_t) chunksPtrs[i].second;
        while ((size_t) localSecond != range) {
            for (size_t j = 0; j < columnsCount; j++) {
            
                delimitator = (j != columnsCount - 1) ? ',' : '\n';

                localSecond = (const unsigned char*) memchr(localFirst, delimitator,  range - (size_t) localFirst);
                value.assign(localFirst, localSecond); localFirst = localSecond;

                if (categoricalColumns.find(j) == categoricalColumns.end()) {
                    finalFile << value << delimitator;
                    continue;
                }

                if (uniqueColumnsValues[j].find(value) == uniqueColumnsValues[j].end()) {
                    uniqueColumnsValues[j][value] = uniqueColumnsValues[j].size();
                }

                *encoded_files[j] << uniqueColumnsValues[j][value] << ',' << value << endl;
            }
        }
    }
}