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
#include <sstream>
#include <mutex>
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
    if (hFile == INVALID_HANDLE_VALUE) {
        cerr << "Erro ao abrir o arquivo de entrada!" << endl;
        return 1;
    }

    //Mapeamento do arquivo
    HANDLE hMap = CreateFileMappingA(
        hFile,
        NULL,
        PAGE_READONLY,
        0,
        0,
        NULL
    );
    if (hMap == NULL) {
        cerr << "Erro ao criar mapeamento de arquivo!" << endl;
        CloseHandle(hFile);
        return 1;
    }

    //Tamanho do arquivo
    LARGE_INTEGER fileSize;
    if (!GetFileSizeEx(hFile, &fileSize)) {
        cerr << "Erro ao obter tamanho do arquivo!" << endl;
        CloseHandle(hMap);
        CloseHandle(hFile);
        return 1;
    }

    //Conteúdo do arquivo
    const unsigned char* data = (const unsigned char*) MapViewOfFile(
        hMap,
        FILE_MAP_READ,
        0, 0, 0
    );
    if (!data) {
        cerr << "Erro ao mapear arquivo na memória!" << endl;
        CloseHandle(hMap);
        CloseHandle(hFile);
        return 1;
    }

    //Ponteiros para mapear os valores do arquivo e os ponteiros de escrita
    const unsigned char *valuesBegin, *startReadPtr, *endReadPtr, *valuesEnd;

    valuesEnd = data + fileSize.QuadPart;
    valuesBegin = (const unsigned char*) memchr(data, '\n', (size_t)(valuesEnd - data));
    if (!valuesBegin) {
        cerr << "Erro: não encontrou o fim do header (primeira linha) no arquivo!" << endl;
        UnmapViewOfFile(data);
        CloseHandle(hMap);
        CloseHandle(hFile);
        return 1;
    }

    //Leitura de headers
    string line((const char*)data, valuesBegin - data); 
    startReadPtr = valuesBegin + 1;
    cout << "Header lido: " << line << endl;

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
    if (!finalFile.is_open()) {
        cerr << "Erro ao abrir arquivo final de saída!" << endl;
        return 1;
    }
    for (size_t i = 0; i < columnsCount - 1; i++) finalFile << columns[i] << ",";
    finalFile << columns[columnsCount - 1];

    //Memória
    MEMORYSTATUSEX mem;
    mem.dwLength = sizeof(mem);
    GlobalMemoryStatusEx(&mem);
    // Usar 25% da RAM disponível
    size_t ram = mem.ullAvailPhys * 0.25;
    cout << "RAM disponível para chunks: " << ram / (1024 * 1024) << " MB" << endl;

    // Definir número máximo de threads para o OpenMP
    int max_threads = omp_get_max_threads();
    omp_set_num_threads(max_threads);
    cout << "Utilizando " << max_threads << " threads (CPUs) disponíveis." << endl;

    // Chunk: dividir em exatamente max_threads partes alinhadas ao início de linha
    vector<pair<const unsigned char*, const unsigned char*>> chunksPtrs;
    size_t total_bytes = valuesEnd - (valuesBegin + 1);
    size_t approx_chunk_size = total_bytes / max_threads;
    const unsigned char* chunk_start = valuesBegin + 1;
    for (int i = 0; i < max_threads; ++i) {
        const unsigned char* chunk_end;
        if (i == max_threads - 1) {
            chunk_end = valuesEnd;
        } else {
            const unsigned char* tentative_end = chunk_start + approx_chunk_size;
            if (tentative_end >= valuesEnd) {
                chunk_end = valuesEnd;
            } else {
                // Avançar até o próximo '\n' para alinhar ao início da linha
                chunk_end = (const unsigned char*) memchr(tentative_end, '\n', valuesEnd - tentative_end);
                if (!chunk_end) chunk_end = valuesEnd;
            }
        }
        chunksPtrs.emplace_back(chunk_start, chunk_end);
        chunk_start = chunk_end + 1;
        if (chunk_start >= valuesEnd) break;
    }
    cout << "Chunks mapeados: " << chunksPtrs.size() << "" << endl;

    //Leitura paralelizada e bufferização
    vector<unordered_map<string, size_t>> uniqueColumnsValues(columnsCount); // global para garantir consistência
    vector<std::mutex> col_mutex(columnsCount); // mutex para cada coluna categórica

    // Buffers locais para escrita
    vector<vector<string>> localEncodedBuffers(columnsCount); // para arquivos de colunas categóricas
    vector<string> localFinalBuffer(chunksPtrs.size()); // para arquivo final

    #pragma omp parallel for schedule(dynamic)
    for (int i = 0; i < (int)chunksPtrs.size(); i++) {
        std::stringstream finalBuffer;
        vector<std::stringstream> encodedBuffers(columnsCount);
        const unsigned char *chunk_start = chunksPtrs[i].first;
        const unsigned char *chunk_end = chunksPtrs[i].second;
        const unsigned char *localFirst = chunk_start;
        const unsigned char *localSecond = localFirst;
        size_t lineCount = 0;
        while (localFirst < chunk_end) {
            for (size_t j = 0; j < columnsCount; j++) {
                char delimitator = (j != columnsCount - 1) ? ',' : '\n';
                size_t remaining = chunk_end - localFirst;
                if (remaining == 0) break;
                localSecond = (const unsigned char*) memchr(localFirst, delimitator, remaining);
                if (!localSecond) {
                    // Linha malformada ou fim do chunk
                    localFirst = chunk_end;
                    break;
                }
                std::string value((const char*)localFirst, localSecond - localFirst);
                if (!value.empty() && value.back() == '\r') value.pop_back();
                localFirst = localSecond + 1;
                if (categoricalColumns.find(j) == categoricalColumns.end()) {
                    finalBuffer << value << delimitator;
                    continue;
                }
                size_t id;
                {
                    std::lock_guard<std::mutex> lock(col_mutex[j]);
                    auto it = uniqueColumnsValues[j].find(value);
                    if (it == uniqueColumnsValues[j].end()) {
                        id = uniqueColumnsValues[j].size();
                        uniqueColumnsValues[j][value] = id;
                    } else {
                        id = it->second;
                    }
                }
                encodedBuffers[j] << id << ',' << value << '\n';
                finalBuffer << id << delimitator;
            }
            lineCount++;
            if (lineCount % 100000 == 0) {
                #pragma omp critical
                std::cout << "Thread " << omp_get_thread_num() << " processou " << lineCount << " linhas do chunk " << i << std::endl;
            }
        }
        // Salva buffers locais
        localFinalBuffer[i] = finalBuffer.str();
        for (size_t j = 0; j < columnsCount; j++) {
            if (categoricalColumns.find(j) != categoricalColumns.end()) {
                #pragma omp critical
                localEncodedBuffers[j].push_back(encodedBuffers[j].str());
            }
        }
        #pragma omp critical
        std::cout << "Thread " << omp_get_thread_num() << " terminou chunk " << i << std::endl;
    }

    // Escrita sequencial dos buffers
    cout << "Iniciando escrita dos arquivos de saída..." << endl;
    for (size_t i = 0; i < localFinalBuffer.size(); i++) {
        finalFile << localFinalBuffer[i];
    }
    // Escreve apenas os pares únicos (id, valor) nos arquivos de dicionário
    for (size_t j = 0; j < columnsCount; j++) {
        if (categoricalColumns.find(j) != categoricalColumns.end() && encoded_files[j]) {
            // Inverter o map para ordenar por id
            vector<pair<size_t, string>> id_value_vec;
            for (const auto& kv : uniqueColumnsValues[j]) {
                id_value_vec.emplace_back(kv.second, kv.first);
            }
            sort(id_value_vec.begin(), id_value_vec.end());
            for (const auto& p : id_value_vec) {
                *encoded_files[j] << p.first << "," << p.second << "\n";
            }
            encoded_files[j]->close();
        }
    }
    finalFile.close();
    cout << "Arquivos de saída escritos e fechados." << endl;

    // Limpeza de recursos
    UnmapViewOfFile(data);
    CloseHandle(hMap);
    CloseHandle(hFile);
    cout << "Recursos liberados." << endl;
}