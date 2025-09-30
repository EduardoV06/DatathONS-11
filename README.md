# SINergia

## Download dos Dados Climáticos - INMET

Os dados históricos de clima podem ser obtidos diretamente no portal do INMET:  
[https://portal.inmet.gov.br/dadoshistoricos](https://portal.inmet.gov.br/dadoshistoricos)  

Após o download, os arquivos devem ser organizados na pasta `Climate` do projeto para facilitar o processamento e integração com os dados de energia.

## Download dos Dados de Energia - ONS

Os datasets oficiais do Operador Nacional do Sistema Elétrico (ONS) estão disponíveis em:  
[https://dados.ons.org.br/](https://dados.ons.org.br/)  

Sendo usados para esse projeto os seguintes datasets:

- [BALANÇO DE ENERGIA NOS SUBSISTEMAS](https://dados.ons.org.br/dataset/balanco-energia-subsistema)  
- [DADOS DE RESTRIÇÃO DE OPERAÇÃO POR CONSTRAINED-OFF DE USINAS EÓLICAS - DETALHAMENTO POR USINAS](https://dados.ons.org.br/dataset/restricao_coff_eolica_detail)  
- [CURVA DE CARGA HORÁRIA](https://dados.ons.org.br/dataset/curva-carga)  
- [GERAÇÃO POR USINA EM BASE HORÁRIA](https://dados.ons.org.br/dataset/geracao-usina-2)
- [CARGA DE ENERGIA VERIFICADA](https://dados.ons.org.br/dataset/carga-energia-verificada)
- [SIN Maps] 

Recomenda-se baixar os dados relevantes ao projeto e salvá-los em formato Parquet para otimizar a leitura e manipulação no pipeline.
## Pipeline da Aplicação (chat_app.py)

A aplicação `chat_app.py` implementa um pipeline integrado para análise e interpretação de séries temporais de energia e clima, com as seguintes etapas principais:

1. **Pré-processamento e agregação temporal**  
   - Carregamento dos dados de energia e clima, alinhamento e resample para frequência horária ou personalizada.  
   - Tratamento de valores faltantes e criação de features temporais para capturar tendências e sazonalidade.

2. **Cálculo de correlações e métricas estatísticas**  
   - Avaliação de correlações entre variáveis da ONS e climaticas principalmente usando o método de Pearson entretanto sendo possível usar Spearman e Mutual Information.  

3. **Detecção de anomalias**  
   - Utilização do algoritmo IsolationForest para identificar padrões atípicos nas séries temporais, auxiliando na detecção de eventos incomuns ou falhas.  
   - A detecção é realizada considerando as características temporais e multivariadas dos dados.

4. **Visualização interativa**  
   - Geração de gráficos dinâmicos para inspeção visual das séries, correlações, anomalias e outras métricas relevantes.  
   - Ferramentas integradas para facilitar a exploração dos dados e resultados.

5. **Interpretação com Large Language Model (LLM)**  
   - Uso d modelo de liguagem para fornecer explicações interpretáveis sobre os insights extraídos, incluindo causas potenciais para anomalias e relações entre variáveis.  
   - Integração que permite uma análise assistida por inteligência artificial para melhor compreensão dos dados.
