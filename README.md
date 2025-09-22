# NF-e Product Importer

Ferramenta completa para conciliar itens de Notas Fiscais eletrônicas (NF-e) com o catálogo mestre da loja e gerar CSVs no
formato aceito pelo Shopify. O projeto oferece três formas de uso: linha de comando, API (FastAPI) e painel de conciliação em
Streamlit.

## Funcionalidades

* **Parser de NF-e 4.0**: leitura dos arquivos XML, extraindo itens com SKU, descrição, GTIN, NCM, CFOP, unidades e valores.
* **Leitura da ficha técnica**: ingestão do Excel mestre (`example_docs/MART-Ficha-tecnica-*.xlsx`) com normalização das
  colunas principais.
* **Motor de matching**: casa automaticamente por SKU, GTIN ou similaridade textual. Mantém um cache de sinônimos para
  conciliações futuras.
* **Geração de CSV**: exporta um arquivo no padrão Shopify com colunas configuráveis e preenche metafields com NCM, CFOP,
  unidade, etc. Cria também um CSV de pendências.
* **API FastAPI**: upload de NF-e, disparo de processamento, listagem de execuções e download de arquivos.
* **Painel Streamlit**: tela para conciliar manualmente pendências, buscar itens no catálogo e registrar equivalências.
* **Agendador**: opção de “watched folder” que roda diariamente ou em intervalos configurados.
* **Integração opcional com Google Drive**: permite mapear SKUs para links públicos de imagens.

## Requisitos

```
python3.11
pip install -r requirements.txt
```

## Configuração

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

Opções disponíveis:

* `process`: processa arquivos na pasta de entrada ou lista fornecida (`process -- files a.xml b.xml`).
* `watch`: inicia o agendador definido no `config.yaml`.
* `api`: sobe o servidor FastAPI (`uvicorn`) com endpoints para upload/processamento.
* `ui`: executa o painel Streamlit para conciliação manual.

### API

```
POST /upload/nfe         # upload de arquivos (multipart)
POST /process            # dispara processamento (opcionalmente informando arquivos específicos)
GET  /runs               # lista execuções
GET  /exports/{run_id}   # baixa o CSV gerado
GET  /pendings/{run_id}  # baixa pendências
POST /reconcile          # registra equivalência manual
POST /catalog/reload     # recarrega a ficha técnica
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


## Arena (A/B/C/D)

A arena permite comparar variações do pipeline lado a lado (A, B, C e D) com dados de exemplo.

- Pré‑requisitos
  - Python 3.11 (recomendado)
  - Virtualenv (opcional, mas recomendado)

- Instalação (ambiente isolado)
  - Windows PowerShell:
    - `python -m venv venv`
    - `venv\Scripts\Activate.ps1`
  - macOS/Linux:
    - `python3 -m venv .venv`
    - `source .venv/bin/activate`
  - Dependências:
    - `pip install -r requirements.txt`

- Dados de teste
  - Já existem exemplos em `example_docs/` e foram copiados para `arena/data/`:
    - 2 XMLs de NF‑e, 1 CSV e 1 XLSX (catálogo mestre)

- Executar a arena
  - `python arena/evaluate.py`
  - Saídas:
    - `arena/reports/scoreboard.csv` (tabela com métricas por variante)
    - `arena/reports/summary.md` (relatório legível para PR/Code Review)
    - `arena/reports/summary.html` (mesmo conteúdo em HTML com snippets)
    - `arena/reports/scoreboard.html` (scoreboard em HTML)
    - `arena/variants/*/out/{result.csv, pendings.csv, metrics.json, run_id.txt, result_high_conf.csv, result_review.csv}`

- O que muda por variante
  - Os runners em `arena/variants/A…D/runner.py` ajustam o `matcher_threshold`:
    - A=0.95 (mais estrita), B=0.92, C=0.88, D=0.90 (mais flexível)
  - O cabeçalho do CSV é validado contra `config.yaml` + gerador atual; diferenças fazem o runner falhar (exit≠0) e são reportadas no summary.
  - Métricas de confiabilidade (buckets por confiança: high≥0.95, mid 0.90–0.95, low<0.90) são geradas em `metrics.json` e no `summary`.

- Como interpretar
  - `itens_total`: linhas geradas no CSV final (não inclui pendências)
  - `itens_ok` e `% completo`: completude de colunas essenciais (Barcode, NCM, Title)
  - `matched`/`unmatched`: itens conciliados vs. pendentes a partir das NF‑e

- Dica: para ver rapidamente, abra `arena/reports/scoreboard.html` e `arena/reports/summary.html` no navegador.

### Fluxo de branch/PR da arena

1. Criar a branch: `git checkout -b arena`
2. Comitar alterações de arena: entrypoint, runners, evaluate, etc.
3. Rodar a arena e revisar `arena/reports/summary.md` e `.html`
4. Enviar para o remoto: `git push -u origin arena`
5. Abrir PR e discutir qual variante seguir no `main`
