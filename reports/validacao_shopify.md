# Validação Shopify

## Cabeçalho final
```
Handle,Title,Body (HTML),Vendor,Tags,Published,Option1 Name,Option1 Value,Option2 Name,Option2 Value,Option3 Name,Option3 Value,Variant SKU,Variant Price,Variant Compare At Price,Variant Inventory Qty,Variant Weight,Variant Weight Unit,Variant Requires Shipping,Image Src,Variant Barcode,Variant Grams,Variant Inventory Tracker,Variant Inventory Policy,Variant Fulfillment Service,product.metafields.custom.unidade,product.metafields.custom.catalogo,product.metafields.custom.dimensoes_do_produto,product.metafields.custom.composicao,product.metafields.custom.capacidade,product.metafields.custom.modo_de_uso,product.metafields.custom.icms,product.metafields.custom.ncm,product.metafields.custom.pis,product.metafields.custom.ipi,product.metafields.custom.cofins,product.metafields.custom.componente_de_kit,product.metafields.custom.resistencia_a_agua,Variant Taxable,Cost per item,Image Position,Variant Image,Product Category,Type,Collection,Status
```

## Amostra (5 linhas de saída)
```csv
Handle,Title,Body (HTML),Vendor,Tags,Published,Option1 Name,Option1 Value,Option2 Name,Option2 Value,Option3 Name,Option3 Value,Variant SKU,Variant Price,Variant Compare At Price,Variant Inventory Qty,Variant Weight,Variant Weight Unit,Variant Requires Shipping,Image Src,Variant Barcode,Variant Grams,Variant Inventory Tracker,Variant Inventory Policy,Variant Fulfillment Service,product.metafields.custom.unidade,product.metafields.custom.catalogo,product.metafields.custom.dimensoes_do_produto,product.metafields.custom.composicao,product.metafields.custom.capacidade,product.metafields.custom.modo_de_uso,product.metafields.custom.icms,product.metafields.custom.ncm,product.metafields.custom.pis,product.metafields.custom.ipi,product.metafields.custom.cofins,product.metafields.custom.componente_de_kit,product.metafields.custom.resistencia_a_agua,Variant Taxable,Cost per item,Image Position,Variant Image,Product Category,Type,Collection,Status
aparadores-livros-polirresina,Aparadores de livros em polirresina,"A polirresina é um material sintético composto por resinas e polímeros reforçados com fibra de vidro. É altamente durável, resistente ao desgaste do tempo e às intempéries, além de ser versátil e permitir moldagens precisas.
O processo de produção envolve o derramamento da polirresina líquida em moldes que são rotacionados para garantir uma distribuição uniforme enquanto endurece no formato desejado. Para a limpeza de superfícies lisas, use um pano levemente umedecido com água e sabão neutro, seguido de um pano seco. Para limpeza de superfícies porosas, utilize um espanador ou uma escova de cerdas macias. Não é recomendado o uso de produtos químicos ou abrasivos.",MART,ADORNOS,TRUE,Title,Default Title,,,,,19487,165.55,,2,1.02,kg,TRUE,,7908103794877,1020,shopify,deny,manual,PC,2025 CATALOGO ANUAL,19.5 x 8 x 19,100% POLIRRESINA,,"A polirresina é um material sintético composto por resinas e polímeros reforçados com fibra de vidro. É altamente durável, resistente ao desgaste do tempo e às intempéries, além de ser versátil e permitir moldagens precisas.
O processo de produção envolve o derramamento da polirresina líquida em moldes que são rotacionados para garantir uma distribuição uniforme enquanto endurece no formato desejado. Para a limpeza de superfícies lisas, use um pano levemente umedecido com água e sabão neutro, seguido de um pano seco. Para limpeza de superfícies porosas, utilize um espanador ou uma escova de cerdas macias. Não é recomendado o uso de produtos químicos ou abrasivos.",,39264000,,,,,,TRUE,75.25,,,ADORNOS,APARADORES DE LIVROS,3T24,active
bandeja-mdf-rattan,Bandeja em mdf e rattan,"Que tal uma peça curinga na decoração? As bandejas são versáteis, funcionais e permitem diversas composições para organizar o ambiente. Abuse da criatividade e explore todo o potencial que elas têm para valorizar o espaço, expor adornos decorativos e itens que juntos dão aquele toque tão especial ao décor.",MART,BANDEJAS,TRUE,Title,Default Title,,,,,16752,114.42,,2,840,g,TRUE,,7908103767529,840,shopify,deny,manual,PC,OUTLET 2025,3 x 18 x 38,50% MDF + 25% VIDRO + 25% PVC,Nao se aplica,"Para limpeza é recomendado o uso de pano umedecido com água e sabão neutro, seguido de um pano seco. Evitar o excesso de peso sobre o vidro.",,44209000,,,,,,TRUE,52.01,,,BANDEJAS,,1T23,active
bandeja-metal-espelho,Bandeja em metal com espelho,"Que tal uma peça curinga na decoração? As bandejas são versáteis, funcionais e permitem diversas composições para organizar o ambiente. Abuse da criatividade e explore todo o potencial das linhas retas e minimalistas para valorizar o espaço, expor adornos decorativos e itens que juntos dão aquele toque tão especial ao décor.",MART,BANDEJAS,TRUE,Title,Default Title,,,,,09616,51.04,,6,410,g,TRUE,,7899525696163,410,shopify,deny,manual,PC,OUTLET 2025,4.5 x 12.5 x 24,70% METAL (FERRO) + 30% ESPELHO,,"Devem ficar em áreas cobertas, longe do sol, chuva ou umidade. Não utilizar produtos químicos ou abrasivos. Para limpeza é recomendado o uso de pano seco ou espanador. Evitar o excesso de peso sobre o produto",,73239900,,,,,,TRUE,23.2,,,BANDEJAS,,1T19,active
```

## Checklist
- [x] Cabeçalho idêntico ao template example_docs/CSV Para Enviar Produtos.csv.
- [x] Defaults Shopify aplicados (Variant Inventory Tracker=shopify, Variant Inventory Policy=deny, Variant Fulfillment Service=manual, Variant Requires Shipping=TRUE, Variant Taxable=TRUE).
- [x] Regras de peso: valores < 1 kg exportados em gramas (Variant Weight Unit=g e Variant Grams); >=1 kg permanece em kg (ambos normalizados).
- [x] Option1 Name/Value garantem unicidade por handle (Default Title, Default Title-2, Default Title-3).
- [x] Tags higienizadas (sem nan, códigos trimestrais ou duplicatas; primeiro item = Product Type).
- [x] Metafields críticos preenchidos quando disponíveis (unidade, catalogo, dimensoes_do_produto, composicao, capacidade, modo_de_uso, icms, ncm, pis, ipi, cofins, componente_de_kit, resistencia_a_agua).
- [x] Descrição (Body (HTML)) usa textos do catálogo quando presente e não duplica composição; features vão para o metafield modo_de_uso.
- [x] Collection limpa (nan removido, fallback para Product Type).
- [ ] Product Category usa o tipo do produto como fallback — mapear para a taxonomia Shopify oficial quando disponível.

## Testes executados
- python -m pytest
- python -m nfe_importer.main process --config pipelines/super/config.yaml example_docs/35250805388725000384550110003221861697090032-nfe.xml

## Resumo das mudanças
- src/nfe_importer/core/generator.py: cria SHOPIFY_HEADER, valida o cabeçalho, normaliza tags, aplica defaults Shopify, ajusta descrição/peso/coleção e adiciona helpers de limpeza.
- src/nfe_importer/core/parser.py: trata coleções nan como vazias para habilitar fallback.
- pipelines/**/config.yaml & config.yaml: cabeçalho alinhado ao template, remoção de metafields não suportados (cfop, cest), dinâmica do modo_de_uso via features.
- tests/test_generator.py e tests/test_metafields.py: atualizados para o novo cabeçalho e cenários (descrição pelos textos, modo de uso, coleção).

