#include <iostream>
#include <string>
#include <fstream>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include<string>
#include <sstream>
#include <omp.h>

constexpr const char* ARQUIVO_DATASET = "dataset_00_sem_virg.csv";

using namespace std;
using Linha = vector<string>;

unordered_set<string> colunasAlvo = {
    "cdtup", "berco", "portoatracacao", "mes", "tipooperacao", 
    "tiponavegacaoatracacao", "terminal", "origem", "destino", 
    "cdmercadoria", "naturezacarga", "sentido"
};

void ler_cabecalho(ifstream& arquivo, vector<string>& nomeColunas, vector<int>& indicesAlvo, unordered_map<string, int>& nomeParaIndice);
vector<Linha> ler_dados(ifstream& arquivo);
void gerar_mapas_codificacao(
    const vector<Linha>& dados,
    const vector<string>& nomeColunas,
    const vector<int>& indicesAlvo,
    unordered_map<string, unordered_map<string, int>>& mapas,
    unordered_map<string, int>& contadores
);
void escrever_arquivos_individuais(
    const vector<int>& indicesAlvo,
    const vector<string>& nomeColunas,
    const unordered_map<string, unordered_map<string, int>>& mapas
);
void escrever_dataset_codificado(
    const vector<Linha>& dados,
    const vector<string>& nomeColunas,
    const unordered_map<string, unordered_map<string, int>>& mapas
);


int main() {
    ifstream arquivo(ARQUIVO_DATASET);
    if (!arquivo) {
        cerr << "Não foi possível abrir o arquivo " << ARQUIVO_DATASET << ". Verifique o local do arquivo." << endl;
        return 1;
    }

    vector<string> nomeColunas;
    vector<int> indicesAlvo;
    unordered_map<string, int> nomeParaIndice;
    
    /* leitura do arquivo */
    ler_cabecalho(arquivo, nomeColunas, indicesAlvo, nomeParaIndice);
    vector<Linha> dados = ler_dados(arquivo);
    arquivo.close();

    double inicio = omp_get_wtime();

    /* mapeamento das colunas */
    unordered_map<string, unordered_map<string, int>> mapas;
    unordered_map<string, int> contadores;

    /* criação dos arquivos de codificação */
    gerar_mapas_codificacao(dados, nomeColunas, indicesAlvo, mapas, contadores);
    escrever_arquivos_individuais(indicesAlvo, nomeColunas, mapas);
    escrever_dataset_codificado(dados, nomeColunas, mapas);

    double fim = omp_get_wtime();
    cout << (fim - inicio) << " segundos" << endl;

    return 0;
}

void ler_cabecalho(ifstream& arquivo, vector<string>& nomeColunas, vector<int>& indicesAlvo, unordered_map<string, int>& nomeParaIndice) {
    string cabecalho;
    getline(arquivo, cabecalho);

    stringstream streamCabecalho(cabecalho);
    string coluna;
    int idx = 0;

    while (getline(streamCabecalho, coluna, ',')) {
        nomeColunas.push_back(coluna);
        if (colunasAlvo.count(coluna)) {
            indicesAlvo.push_back(idx);
        }
        nomeParaIndice[coluna] = idx++;
    }
}

vector<Linha> ler_dados(ifstream& arquivo) {
    vector<Linha> dados;
    string linha;

    while (getline(arquivo, linha)) {
        stringstream streamLinha(linha);
        string valor;
        Linha linhaDados;

        while (getline(streamLinha, valor, ',')) {
            linhaDados.push_back(valor);
        }

        dados.push_back(linhaDados);
    }

    return dados;
}

void gerar_mapas_codificacao(
    const vector<Linha>& dados,
    const vector<string>& nomeColunas,
    const vector<int>& indicesAlvo,
    unordered_map<string, unordered_map<string, int>>& mapas,
    unordered_map<string, int>& contadores
) {
    #pragma omp parallel for
    for (int i = 0; i < indicesAlvo.size(); i++) 
    {
        int col = indicesAlvo[i];
        string nomeCol = nomeColunas[col];
        unordered_map<string, int> mapaLocal;
        int prox_id = 0;

        for (const auto &linha : dados)
        {
            string valor = linha[col];
            if (!mapaLocal.count(valor))
            {
                mapaLocal[valor] = prox_id++;
            }
        }

        #pragma omp critical
        {
            mapas[nomeCol] = mapaLocal;
        }
        contadores[nomeCol] = prox_id;
    }
}

void escrever_arquivos_individuais(
    const vector<int>& indicesAlvo,
    const vector<string>& nomeColunas,
    const unordered_map<string, unordered_map<string, int>>& mapas
) {
    #pragma omp parallel for
    for (int i = 0; i < indicesAlvo.size(); i++)
    {
        int col = indicesAlvo[i];
        string nomeCol = nomeColunas[col];
        ofstream arquivoId("ID_" + nomeCol + ".csv");
        arquivoId << "COD,CONTEUDO\n";

        for (const auto &par : mapas.at(nomeCol))
        {
            arquivoId << par.second << "," << par.first << "\n";
        }
        arquivoId.close();
    }
}

void escrever_dataset_codificado(
    const vector<Linha>& dados,
    const vector<string>& nomeColunas,
    const unordered_map<string, unordered_map<string, int>>& mapas
) 
{
    vector<string> linhasProntas(dados.size());

    #pragma omp parallel for
    for (int i = 0; i < dados.size(); i++)
    {
        stringstream ss;
        for (size_t j = 0; j < dados[i].size(); j++)
        {
            const string &valor = dados[i][j];
            if (mapas.count(nomeColunas[j]))
            {
                ss << mapas.at(nomeColunas[j]).at(valor);
            }
            else
            {
                ss << valor;
            }
            if (j < dados[i].size() - 1)
                ss << ",";
        }
        linhasProntas[i] = ss.str();
    }
    ofstream arquivoCodificado("dataset_codificado.csv");

    for (size_t i = 0; i < nomeColunas.size(); i++) 
    {
        arquivoCodificado << nomeColunas[i];
        if (i < nomeColunas.size() - 1) {
            arquivoCodificado << ",";
        }
    }
    arquivoCodificado << "\n";

    for (const auto &linha : linhasProntas)
    {
        arquivoCodificado << linha << "\n";
    }

    arquivoCodificado.close();
}
