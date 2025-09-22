# NF-e Product Importer

Ferramenta completa para conciliar itens de Notas Fiscais eletrÃ´nicas (NF-e) com o catÃ¡logo mestre da loja e gerar CSVs no
formato aceito pelo Shopify. O projeto oferece trÃªs formas de uso: linha de comando, API (FastAPI) e painel de conciliaÃ§Ã£o em
Streamlit.

## Funcionalidades

* **Parser de NF-e 4.0**: leitura dos arquivos XML, extraindo itens com SKU, descriÃ§Ã£o, GTIN, NCM, CFOP, unidades e valores.
* **Leitura da ficha tÃ©cnica**: ingestÃ£o do Excel mestre (`example_docs/MART-Ficha-tecnica-*.xlsx`) com normalizaÃ§Ã£o das
  colunas principais.
* **Motor de matching**: casa automaticamente por SKU, GTIN ou similaridade textual. MantÃ©m um cache de sinÃ´nimos para
  conciliaÃ§Ãµes futuras.
* **GeraÃ§Ã£o de CSV**: exporta um arquivo no padrÃ£o Shopify com colunas configurÃ¡veis e preenche metafields com NCM, CFOP,
  unidade, etc. Cria tambÃ©m um CSV de pendÃªncias.
* **API FastAPI**: upload de NF-e, disparo de processamento, listagem de execuÃ§Ãµes e download de arquivos.
* **Painel Streamlit**: tela para conciliar manualmente pendÃªncias, buscar itens no catÃ¡logo e registrar equivalÃªncias.
* **Agendador**: opÃ§Ã£o de â€œwatched folderâ€ que roda diariamente ou em intervalos configurados.
* **IntegraÃ§Ã£o opcional com Google Drive**: permite mapear SKUs para links pÃºblicos de imagens.

## Requisitos

```
python3.11
pip install -r requirements.txt
```

## ConfiguraÃ§Ã£o

Edite o arquivo `config.yaml` ou crie um novo baseado em `config/config.yml.example`. Principais parÃ¢metros:

```yaml
paths:
  nfe_input_folder: "data/"          # Pasta monitorada / uploads
  master_data_file: "example_docs/MART-Ficha-tecnica-Biblioteca-Virtual-08-08-2025.xlsx"
  output_folder: "output/"           # CSVs gerados
  log_folder: "logs/"               # Metadados de execuÃ§Ã£o
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
* `ui`: executa o painel Streamlit para conciliaÃ§Ã£o manual.

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

O painel permite carregar NF-e, disparar processamentos e conciliar itens com sugestÃµes do catÃ¡logo. As escolhas sÃ£o
persistidas no cache de sinÃ´nimos (`synonyms.json`).

## Testes

```bash
pytest
```

Os testes utilizam os arquivos de exemplo presentes em `example_docs/` para validar o parser, o motor de matching e a geraÃ§Ã£o do
CSV.


