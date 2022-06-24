# Análise Fundamentalista

Este é um projeto cujo objetivo principal é o desenvolvimento de uma aplicação capaz de fornecer o acesso a informação de empresas listadas na BMFBovespa para a realização de análises fundamentalistas.

Os códigos têm a finalidade de realizar o provisionamento da infraestrutura necessária para armazenamento e execução das tarefas. Em um primeiro momento, as tarefas de extração de dados e pré-processamento serão realizadas localmente, no entanto, em um segundo momento, os códigos serão adaptados para funcionar 100% em cloud. A imagem a seguir apresenta o fluxo da aplicação de dados:

## Status do Projeto

![Pipeline de Dados](https://github.com/andre-ds/fundamentalist_analysis/blob/master/jira.jpg)


## Estrutura dos Dados

![Pipeline de Dados](https://github.com/andre-ds/fundamentalist_analysis/blob/master/application-flow.png)

### **Raw Data Ingestion** 

A primeira etapa consiste na extração dos dados de empresas disponíveis na CVM e são armazenados na camada **Raw** do datalake na AWS.

### **Pre-processing**

Na segunda etapa, os dados são pré-processados e armazenados na camada **pre_processed** do datalake.


## Estrutura da Aplicação

### Utils

- extraction_cvm
- unzippded_files
- saving_raw_data
- load_bucket


### PreProcessing

- pre_process_itr_dre



## Dicionário de Dados Brutos CVM

**Documentos: Formulário de Demonstrações Financeiras Padronizadas (DFP)**

O Formulário de Demonstrações Financeiras Padronizadas (DFP) é formado por um conjunto de documentos encaminhados periodicamente devido a normativa 480/09 da CVM.

**Formulário de Informações Trimestrais (ITR)**

O ITR é semlhante ao DFP, exeto pelo fato de conter informações contáveis trimestrais.

#### Documentos
* Balanço Patrimonial Ativo (BPA)
* Balanço Patrimonial Passivo (BPP)
* Demonstração de Fluxo de Caixa - Método Direto (DFC-MD)
* Demonstração de Fluxo de Caixa - Método Indireto (DFC-MI)
* Demonstração das Mutações do Patrimônio Líquido (DMPL)
* Demonstração de Resultado Abrangente (DRA)
* Demonstração de Resultado (DRE)
    * dfp_cia_aberta_2011.zip
        * dfp_cia_aberta_DRE_con_2022.csv - Consolidada
        * dfp_cia_aberta_DRE_ind_2022.csv - Individual

* Demonstração de Valor Adicionado (DVA)

*Obs*:
Consolidado: É referente aos dados do grupo econômico pela qual a empresa faz parte.
Individual: Diz respeito a empresa que é a controladora de um grupo individual


#### Links

* Dados Disponíves: http://dados.cvm.gov.br/

* Dicionário dos Dados: http://dados.cvm.gov.br/dataset/cia_aberta-doc-itr/resource/062b8f02-ca6b-424a-bf65-180ff2b69af2