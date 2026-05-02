# Project Context

## Goal

POC: pipeline generica ingestao dados, N produtos, baixa customizacao.

- Python padrao.
- Execucao local via [Ministack](https://github.com/ministackorg/ministack).
- Incluir `docker-compose`.
- Usar `IngestionPipeline.drawio` como referencia arquitetura.
- Cada evento no EventBus inicia Step Functions para processar arquivo.
- Variacao por produto fica em config DynamoDB + JSON de-para layout.

## Architecture

- EventBus: recebe evento novo arquivo.
- Step Functions: orquestra ingestao + harmonizacao.
- Glue: processa arquivo grande, converte `.parquet`, harmoniza layout.
- S3: guarda arquivos em `/raw`, `/processed`, `/curated`.
- DynamoDB: configs produto + de-para.
- SQS: recebe evento arquivo pronto.
- ECS: processa batch, escala sob demanda, reduz custo idle.
- SNS: publica eventos para filas destino corretas.

## S3 Folders

- `/raw`: arquivo recebido, ainda nao processado.
- `/processed`: arquivo ja lido, validado, convertido `.parquet`, harmonizado dominio.
- `/curated`: arquivo final, enriquecido, pronto consumo downstream.
- Regra: mover arquivo quando etapa termina.
- Fluxo pasta: `/raw -> /processed -> /curated`.
- Evento so dispara quando arquivo pronto para proxima etapa.

## Data Processing

- Volume alto: >1M registros/dia.
- Arquivos podem ser grandes.
- Evitar Lambda para carga grande.
- Glue/ECS preferidos para batch pesado.
- Codigo processamento generico.
- Cada produto tem JSON de-para: layout produto -> layout dominio.
- Arquivo dominio salva em `/processed`.
- Arquivo enriquecido final move para `/curated`.

## Processing Control

- Sem controle central processamento hoje.
- Produto origem = fonte verdade.
- Se produto reenviar mesmo `ano/mes/dia`, processar de novo.
- Idempotencia fica nos consumidores finais das filas SQS.
- POC nao cria deduplicacao extra sem pedido explicito.

## Functional Flow

1. Produto envia evento ao EventBus.
2. Step Functions valida evento.
3. Evento valido -> Glue le arquivo no S3 lake produto.
4. Dados dia -> `/raw`.
5. Step Functions harmonizacao busca `.parquet`.
6. De-para produto transforma layout produto -> dominio.
7. Fim harmonizacao: mover `/raw -> /processed`.
8. Enriquecimento batch usa tabelas mesh Glue.
9. Fim enriquecimento: mover `/processed -> /curated`.
10. Novo arquivo em `/curated` gera evento S3.
11. Evento `/curated` vai para SQS enriquecimento.
12. ECS consome SQS, le arquivo, publica eventos no SNS.
13. SNS roteia eventos para filas destino.

## Quality

- Simples, claro, POC-first.
- Generico por padrao; customizacao por config/de-para.
- Poucas camadas; sem over-engineering.
- Componentes coesos, leitura rapida.

## Success

- POC roda local via Ministack.
- Infra sobe via `docker-compose`.
- Evento entrada dispara fluxo completo.
- Arquivo percorre `/raw`, `/processed`, `/curated`, publicacao.
- Arquivo move pasta correta ao fim de cada etapa.
- Novo produto exige baixa customizacao.
