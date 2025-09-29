# Como preencher o CSV para importação no Shopify

Este guia explica de forma didática como montar corretamente o arquivo CSV para importar produtos no Shopify. Ele foi adaptado a partir do template oficial, exemplos de NF-e (XML), da ficha técnica fornecida pela MART e dos ajustes feitos no projeto de automação.

---

## 1. Introdução

O Shopify permite importar produtos em massa através de um arquivo CSV. Para que a importação funcione sem erros, o CSV precisa seguir exatamente o formato esperado (colunas, ordem e valores válidos).

Este documento serve como **manual de referência** para:

* Entender o que significa cada coluna do CSV.
* Saber quais campos são obrigatórios e quais são opcionais.
* Evitar erros comuns na importação.
* Preencher com exemplos reais de produtos.

---

## 2. Estrutura do Arquivo CSV

O CSV precisa ter o cabeçalho (linha 1) com os nomes das colunas, seguidos das linhas de produtos. Cada produto pode ter uma ou mais linhas (no caso de variantes).

### 2.1. Campos principais

| Coluna           | Obrigatório? | Descrição                                                     | Exemplo                                          | Observações                                                                                                     |
| ---------------- | ------------- | --------------------------------------------------------------- | ------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------- |
| Handle           | ✅            | Identificador único do produto (sem espaços, em minúsculas). | sabonete-natural-ervas                           | Usado para agrupar variantes. Se repetir em mais de uma linha, o Shopify entende como variantes do mesmo produto. |
| Title            | ✅            | Nome do produto.                                                | Sabonete Natural de Ervas                        | Nome exibido na loja.                                                                                             |
| Body (HTML)      | Opcional      | Descrição em HTML.                                            | `<p>Sabonete feito com óleos vegetais...</p>` | Importante evitar duplicar informações já em metafields (ex: composição).                                    |
| Vendor           | ✅            | Fabricante/fornecedor.                                          | MART                                             | Normalmente preenchido pelo campo de fornecedor.                                                                  |
| Product Category | ✅            | Categoria do Shopify.                                           | Home & Garden > Bathroom Accessories             | Necessário para SEO e coleções automáticas.                                                                   |
| Type             | Opcional      | Tipo do produto.                                                | Cosmético                                       | Pode ficar vazio se já estiver categorizado.                                                                     |
| Tags             | Opcional      | Palavras-chave separadas por vírgula.                          | "sabão, ervas, artesanal"                       | Evitar**tags estranhas**(códigos curtos como `1T24`). Usar apenas termos úteis.                         |
| Published        | ✅            | Define se o produto será publicado.                            | TRUE                                             | Use TRUE/FALSE.                                                                                                   |

### 2.2. Campos de variantes (Variant *)

| Coluna                      | Obrigatório? | Descrição                         | Exemplo       | Observações                                                                                |
| --------------------------- | ------------- | ----------------------------------- | ------------- | -------------------------------------------------------------------------------------------- |
| Option1 Name                | ✅            | Nome da primeira opção.           | Title         | Para produtos simples, use "Title".                                                          |
| Option1 Value               | ✅            | Valor da opção.                   | Default Title | Para produtos simples, use "Default Title".                                                  |
| SKU                         | ✅            | Código único do produto.          | SAB-1234      | Vem do XML (cProd) ou catálogo.                                                             |
| Barcode (EAN)               | Opcional      | Código de barras/EAN.              | 7891234567890 | Se não houver, deixar em branco.                                                            |
| Variant Price               | ✅            | Preço de venda.                    | 29.90         | Sempre com ponto decimal e 2 casas.                                                          |
| Variant Compare At Price    | Opcional      | Preço de comparação.             | 39.90         | Usado para promoções.                                                                      |
| Variant Inventory Qty       | ✅            | Quantidade em estoque.              | 15            | Número inteiro.                                                                             |
| Variant Inventory Policy    | ✅            | Política de estoque.               | deny          | Valores aceitos:`deny`(não vende sem estoque) ou `continue`(permite venda sem estoque). |
| Variant Fulfillment Service | ✅            | Serviço de fulfillment.            | manual        | Para nossa automação: sempre "manual".                                                     |
| Variant Requires Shipping   | ✅            | Define se precisa de frete.         | TRUE          | Use TRUE/FALSE.                                                                              |
| Variant Taxable             | ✅            | Define se o produto é tributável. | TRUE          | Use TRUE/FALSE.                                                                              |
| Variant Weight              | ✅            | Peso do produto.                    | 0.3           | Número decimal.                                                                             |
| Variant Weight Unit         | ✅            | Unidade de peso.                    | kg ou g       | Regra: se ≥1 → kg; se <1 → convertido para gramas (g).                                    |

### 2.3. Metafields personalizados

O template inclui colunas de  **metafields** . Eles armazenam informações adicionais úteis na loja.

| Coluna                                         | Obrigatório? | Descrição             | Exemplo                | Observações                          |
| ---------------------------------------------- | ------------- | ----------------------- | ---------------------- | -------------------------------------- |
| product.metafields.custom.ncm                  | ✅            | Código NCM do produto. | 3307.90.00             | Vem do catálogo ou XML.               |
| product.metafields.custom.unidade              | Opcional      | Unidade de venda.       | UN                     | Normalizar para unidade padrão.       |
| product.metafields.custom.composicao           | Opcional      | Composição/material.  | Óleos vegetais, ervas | Evitar duplicação no Body HTML.      |
| product.metafields.custom.dimensoes_do_produto | Opcional      | Dimensões (LxAxP).     | 10x5x3 cm              | Preencher se disponível no catálogo. |
| product.metafields.custom.catalogo             | Opcional      | Catálogo de origem.    | MART-2025              | Usado para referência interna.        |

---

## 3. Exemplo de preenchimento (linha única)

```csv
Handle,Title,Body (HTML),Vendor,Product Category,Type,Tags,Published,Option1 Name,Option1 Value,SKU,Barcode,Variant Price,Variant Compare At Price,Variant Inventory Qty,Variant Inventory Policy,Variant Fulfillment Service,Variant Requires Shipping,Variant Taxable,Variant Weight,Variant Weight Unit,product.metafields.custom.ncm,product.metafields.custom.unidade,product.metafields.custom.composicao,product.metafields.custom.dimensoes_do_produto
sabonete-natural-ervas,Sabonete Natural de Ervas,"<p>Sabonete artesanal feito com ervas selecionadas.</p>",MART,"Home & Garden > Bathroom Accessories",,"sabão,ervas,artesanal",TRUE,Title,Default Title,SAB-1234,7891234567890,29.90,,15,deny,manual,TRUE,TRUE,0.3,kg,3307.90.00,UN,"Óleos vegetais, ervas","10x5x3 cm"
```

---

## 4. Passo a passo para gerar e importar

1. **Gerar CSV** → através da automação ou manualmente no Excel.
2. **Validar cabeçalho** → conferir se está igual ao template.
3. **Checar valores** :

* Preço com ponto decimal (29.90, não 29,90).
* Estoque inteiro (10, 25, 0).
* Peso ≥1 → kg, <1 → g.
* Tags limpas (sem códigos curtos).

1. **Salvar como CSV UTF-8 (delimitado por vírgulas)** .
2. **Importar no Shopify** em: Admin → Produtos → Importar.
3. **Conferir no painel** : categorias, coleções, estoque, peso e descrição.

---

## 5. Dicas práticas

* Sempre usar **UTF-8** no CSV (evita erro de acentuação).
* Não remover colunas do template (mesmo se não usar, deixe em branco).
* Se a importação falhar, o Shopify mostra quais campos deram erro.
* Use planilha limpa (sem fórmulas escondidas, linhas extras, ou cabeçalhos repetidos).

---

## 6. Checklist antes de importar

* [ ] Cabeçalho confere com o template.
* [ ] Todos os produtos têm Handle único.
* [ ] SKU único e não vazio.
* [ ] Inventory Policy = deny (ou conforme regra).
* [ ] Fulfillment Service = manual.
* [ ] Peso correto (kg/g).
* [ ] Metafields preenchidos se disponíveis.
* [ ] Tags revisadas (sem códigos curtos ou irrelevantes).
