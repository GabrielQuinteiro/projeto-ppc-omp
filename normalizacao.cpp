#include <iostream>
#include <string>
#include <fstream>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include<string>
#include <sstream>
#include <omp.h>

using namespace std;
using Linha = vector<string>;

constexpr const char* ARQUIVO_DATASET = "dataset_00_sem_virg.csv";
constexpr const char* ARQUIVO_SAIDA   = "dataset_codificado.csv";
constexpr int TAMANHO_CHUNK = 500000;

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
    const vector<vector<string>>& chunk,
    const vector<string>& nomeColunas,
    const unordered_map<string, unordered_map<string, int>>& mapas
);


int main() {
    vector<string> nomeColunas;
    vector<int> indicesAlvo;
    unordered_map<string, int> nomeParaIndice;
    
    /* mapas globais de valor->ID e contadores */
    unordered_map<string, unordered_map<string, int>> mapas;
    unordered_map<string, int> contadores;
    
    /* PASSO 1: leitura do arquivo */
    ifstream arquivo_entrada(ARQUIVO_DATASET);
    ofstream arquivo_saida(ARQUIVO_SAIDA);
    if (!arquivo_entrada || !arquivo_saida) {
        cerr << "Não foi possível abrir o arquivo " << ARQUIVO_DATASET << "ou" << ARQUIVO_SAIDA << ". Verifique o local do arquivo." << endl;
        return 1;
    }
    ler_cabecalho(arquivo_entrada, nomeColunas, indicesAlvo, nomeParaIndice);
    for (size_t i = 0; i < nomeColunas.size(); ++i) {
        arquivo_saida << nomeColunas[i] << (i + 1 < nomeColunas.size() ? ',' : '\n');
    }

    double inicio = omp_get_wtime();

    /* PASSO 2: construir os mapas */
    while(true) {
        auto chunk = ler_dados(arquivo_entrada);
        if (chunk.empty()) break;
        gerar_mapas_codificacao(chunk, nomeColunas, indicesAlvo, mapas, contadores);
        escrever_dataset_codificado(chunk, nomeColunas, mapas);
    } 
    arquivo_entrada.close();

    /* criação dos arquivos de codificação (indv, geral) */
    escrever_arquivos_individuais(indicesAlvo, nomeColunas, mapas);
    //escrever_dataset_codificado(nomeColunas, mapas);

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
    const vector<Linha>& chunk,
    const vector<string>& nomeColunas,
    const vector<int>& indicesAlvo,
    unordered_map<string, unordered_map<string, int>>& mapasGlobais,
    unordered_map<string, int>& contadoresGlobais
) {
    /* pra cada colunaAlvo, é paralelizado a construção de um mapa local */
    #pragma omp parallel for schedule(dynamic, 1)
    for (size_t i = 0; i < indicesAlvo.size(); i++) 
    {
        int col = indicesAlvo[i];
        const string& nomeCol = nomeColunas[col];

        unordered_map<string, int> mapaLocal; /* mapa local por thread */
        int prox_id = 1;

        for (const auto &linha : chunk)
        {
            const string& valor = linha[col];
            if (!mapaLocal.count(valor))
            {
                mapaLocal[valor] = prox_id++;
            }
        }

        #pragma omp critical
        {
            auto& mapaGlobal = mapasGlobais[nomeCol];
            auto& contadorGlobal = contadoresGlobais[nomeCol];
            for (auto& par : mapaLocal) 
            {
                if (!mapaGlobal.count(par.first)) {
                    mapaGlobal[par.first] = contadorGlobal++;
                }
            }
        }
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
    const vector<vector<string>>& chunk,
    const vector<string>& nomeColunas,
    const unordered_map<string, unordered_map<string, int>>& mapas
) 
{
    vector<string> linhasProntas(chunk.size());

    #pragma omp parallel for schedule(dynamic, TAMANHO_CHUNK)
    for (int i = 0; i < chunk.size(); i++)
    {
        stringstream ss;
        for (size_t j = 0; j < chunk[i].size(); j++)
        {
            const string &valor = chunk[i][j];
            if (mapas.count(nomeColunas[j]))
            {
                ss << mapas.at(nomeColunas[j]).at(valor);
            }
            else
            {
                ss << valor;
            }
            if (j < chunk[i].size() - 1)
                ss << ",";
        }
        linhasProntas[i] = ss.str();
    }
    ofstream arquivoCodificado(ARQUIVO_SAIDA);

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
