# DatathONS-11


## Dados do Portal ONS

- [BALANÇO DE ENERGIA NOS SUBSISTEMAS](https://dados.ons.org.br/dataset/balanco-energia-subsistema)  
- [DADOS DE RESTRIÇÃO DE OPERAÇÃO POR CONSTRAINED-OFF DE USINAS EÓLICAS - DETALHAMENTO POR USINAS](https://dados.ons.org.br/dataset/restricao_coff_eolica_detail)  
- [CURVA DE CARGA HORÁRIA](https://dados.ons.org.br/dataset/curva-carga)  
- [GERAÇÃO POR USINA EM BASE HORÁRIA](https://dados.ons.org.br/dataset/geracao-usina-2)  
- [CARGA DE ENERGIA VERIFICADA](https://dados.ons.org.br/dataset/carga-energia-verificada)  

## Dados Externos

- [Dados Históricos de Clima - INMET](https://portal.inmet.gov.br/dadoshistoricos)  

## Pipeline de Extração Automática de Insights

O objetivo do pipeline é **identificar relações relevantes entre variáveis de energia e variáveis climáticas**, incluindo:

1. **Pré-processamento e agregação temporal**  
   - Os datasets de energia (`df_e`) e clima (`df_c`) são **resampleados para uma frequência horária ou customizada**, garantindo alinhamento temporal.  
   - Função utilizada: `resample_and_merge(df_e, df_c, time_col='timestamp', freq='H')`.

2. **Criação de features temporais**  
   - Geramos **médias móveis e desvios padrão** de diferentes janelas (`1h`, `3h`, `24h`) para suavizar ruídos e capturar tendências locais.  
   - Função: `rolling_features(df, cols, windows=[1,3,24])`.

3. **Teste de estacionaridade**  
   - Cada série temporal é avaliada com **ADF test** (`adfuller`) para verificar se é estacionária, fundamental para correlação e Granger causality.

4. **Cálculo de métricas de correlação e dependência**  
   - **Pearson/Spearman correlation**: quantifica correlação linear e monotônica entre cada par de variáveis, incluindo diferentes lags.  
   - **Mutual Information**: mede dependência não-linear entre variáveis.  
   - Função: `compute_pair_metrics(df, x_col, y_col, max_lag=24)`.

5. **Teste de causalidade temporal (Granger)**  
   - Avalia se a série de energia é **previsora** de uma variável climática ou vice-versa.  
   - Função: `granger_pairs(df, x_col, y_col, maxlag=6)`.

6. **Estabilidade da correlação**  
   - A correlação é medida em **janelas móveis semanais** para detectar consistência de sinal (+/-) ao longo do tempo.

7. **Seleção e scoring de insights**  
   - Para cada par `x` (energia) e `y` (clima), é selecionado o **lag com maior correlação absoluta**.  
   - Um score agregado combina:  
     - magnitude da correlação (`corr`)  
     - significância estatística (`pvalue`)  
     - dependência não-linear (`mutual info`)  
     - estabilidade ao longo do tempo (`stability`)  

8. **Top insights**  
   - O pipeline retorna automaticamente os **top K pares** de variáveis com maior score, incluindo:  
     - `x`, `y`, `lag`, `corr`, `pvalue`, `mi`, `stability`, `granger_p`, `granger_lag`, `n` (observações usadas) e `score` final.

9. **Visualização (opcional)**  
   - Painéis interativos usando **Plotly** com:  
     - séries temporais normalizadas  
     - histogramas / contagem de categorias  
     - scatter plots com regressão  
     - heatmaps de correlação por lag  
     - boxplots sazonais  
     - métricas de insight  
     - estabilidade por janelas  
     - scatter múltiplos e heatmap de correlação entre top variáveis  
