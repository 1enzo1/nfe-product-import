# NF-e Product Importer

Ferramenta completa para conciliar itens de Notas Fiscais eletrônicas (NF-e) com o catálogo mestre da loja e gerar CSVs no
formato aceito pelo Shopify. O projeto oferece trás formas de uso: linha de comando, API (FastAPI) e painel de conciliaÃ§Ã£o em
Streamlit.

## Funcionalidades

* **Parser de NF-e 4.0**: leitura dos arquivos XML, extraindo itens com SKU, descrição, GTIN, NCM, CFOP, unidades e valores.
* **Leitura da ficha técnica**: ingestão do Excel mestre (`example_docs/MART-Ficha-tecnica-*.xlsx`) com normalização das
  colunas principais.
* **Motor de matching**: casa automaticamente por SKU, GTIN ou similaridade textual. MantÃ©m um cache de sinônimos para
  conciliações futuras.
* **Geração de CSV**: exporta um arquivo no padrÃ£o Shopify com colunas configuráveis e preenche metafields com NCM, CFOP,
  unidade, etc. Cria também um CSV de pendências.
* **API FastAPI**: upload de NF-e, disparo de processamento, listagem de execuções e download de arquivos.
* **Painel Streamlit**: tela para conciliar manualmente pendências, buscar itens no catálogo e registrar equivalências.
* **Agendador**: opÃ§Ã£o de â€œwatched folderâ€ que roda diariamente ou em intervalos configurados.
* **Integração opcional com Google Drive**: permite mapear SKUs para links públicos de imagens.

## Requisitos

```
python3.11
pip install -r requirements.txt
```

## ConfiguraÃ§Ã£o

Edite o arquivo `config.yaml` ou crie um novo baseado em `config/config.yml.example`. Principais parâmetros:

```yaml
paths:
  nfe_input_folder: "data/"          # Pasta monitorada / uploads
  master_data_file: "example_docs/MART-Ficha-tecnica-Biblioteca-Virtual-08-08-2025.xlsx"
  output_folder: "output/"           # CSVs gerados
  log_folder: "logs/"               # Metadados de execução
  synonym_cache_file: "data/synonyms.json"

pricing:
  strategy: "markup_fixo"
  markup_factor: 2.2

csv_output:
  filename_prefix: "importacao_produtos_"
  columns: ["Handle", "Title", "Vendor", ...]

metafields:
  namespace: "custom"
  keys:
    unidade: "unidade"
    ncm: "ncm"
    cest: "cest"
    cfop: "cfop"
```

## Uso

### Linha de comando

```bash
python -m nfe_importer.main process --config config.yaml
```

OpÃ§Ãµes disponÃ­veis:

* `process`: processa arquivos na pasta de entrada ou lista fornecida (`process -- files a.xml b.xml`).
* `watch`: inicia o agendador definido no `config.yaml`.
* `api`: sobe o servidor FastAPI (`uvicorn`) com endpoints para upload/processamento.
* `ui`: executa o painel Streamlit para conciliação manual.

### API

```
POST /upload/nfe         # upload de arquivos (multipart)
POST /process            # dispara processamento (opcionalmente informando arquivos especÃ­ficos)
GET  /runs               # lista execuÃ§Ãµes
GET  /exports/{run_id}   # baixa o CSV gerado
GET  /pendings/{run_id}  # baixa pendÃªncias
POST /reconcile          # registra equivalÃªncia manual
POST /catalog/reload     # recarrega a ficha tÃ©cnica
```

### Dashboard (Streamlit)

```bash
python -m nfe_importer.main ui --config config.yaml
```

O painel permite carregar NF-e, disparar processamentos e conciliar itens com sugestões do catálogo. As escolhas são
persistidas no cache de sinônimos (`synonyms.json`).

## Testes

```bash
pytest
```

Os testes utilizam os arquivos de exemplo presentes em `example_docs/` para validar o parser, o motor de matching e a geração do
CSV.


### Selecionando versoes no dashboard

A UI agora lista automaticamente as configuracoes encontradas em pipelines/**/config.yaml.
Cada versao (V1, V2, Enhanced, Super) aparece com o caminho da configuracao ao lado. Basta
abrir o painel com:

`ash
python -m nfe_importer.main ui --config config.yaml
`

Na barra lateral selecione a versao desejada, carregue os XML e clique em "Processar agora".
O arquivo de saida e gravado na pasta informada pelo YAML da versao (output/<versao>/).
O log tambem guarda mode="ui:<versao>" para rastreabilidade.

### Configs por versao

Os YAMLs de referencia ficam em:

- pipelines/v1/config.yaml
- pipelines/v2/config.yaml
- pipelines/enhanced/config.yaml
- pipelines/super/config.yaml

Todos compartilham o mesmo cabeçalho Shopify e mantem os defaults:
Variant Fulfillment Service=manual, Variant Inventory Policy=deny,
Variant Inventory Tracker=shopify, Variant Requires Shipping=TRUE e
Variant Taxable=TRUE.

### Smoke tests rapidos

Para validar as quatro versoes com os XMLs de exemplo, execute:

`ash
.\.venv\Scripts\python scripts/run_smoke_tests.py
`

O script gera um CSV por versao e cria 
eports/scoreboard.csv com o resultado
(checks de header, variantes unicas, politicas Shopify e regra g/kg).

