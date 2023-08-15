# spatio-temporal-analysis-techniques
Comparação de Técnicas de Agrupamento de Dados Espaço Temporais Agregados por Área
> Dados do SUS: Leishmaniose Visceral Humana

## 1. Objetivo do Trabalho
O objetivo do trabalho é avaliar o desempenho de alguns dos principais algoritmos de agrupamento de dados espaço-temporais, citados pela literatura, aplicando-os a dados epidemiológicos agregados por área.
A análise será realizada utilizando dados históricos transacionais, modelos estatísticos e algoritmos de aprendizagem de máquina, que explorarão padrões de agrupamento de casos de um determinada doença considerando o espaço e temposimultaneamente.
Desta forma, será possível avaliar se outros algoritmos de aprendizagem de máquina são boas alternativas para este tipo de dado, se possuem desempenho semelhante à técnica estatística tradicionalmente utilizada e custo computacional menor.

## 2. Contextualização do Problema
A compreensão de como doenças avançam no espaço e tempo são de grande relevância para toda a sociedade, principalmente para as Vigilâncias Epidemiológicas que podem construir planos de ação com base em análises dessanatureza.
Normalmente dados epidemiológicos apresentam dependência espacial, que é quando a hipótese nula de aleatoriedade é rejeitada e não se pode afirmar que os casos ocorreram aleatoriamente no espaço de estudo. Desta forma, para a detecção destes agrupamentos e melhor compreensão do fenômeno é necessário analisar as ocorrências considerandoas duas dimensões.
A Estatística de Varredura Espaço-Temporal, implementada no software SaTScan, é uma técnica que varre todo o espaço e tempo, simultaneamente, em busca de agregados mais prováveis, utilizando para esta avaliação testes de hipóteses. É uma técnica amplamente utilizada, mas que pode apresentar tempo de execução longo, de acordo com o tamanho da base.
Portanto, a detecção de técnicas de agrupamento espaço-temporal que trabalhe bem com dados não pontuais (agregados por área) e que apresentem menor custo computacional em relação à técnica estatística citada, pode indicar alternativas para análise de bases desta natureza que sejam grandes, viabilizando a análise.