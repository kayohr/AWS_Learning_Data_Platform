# Guia Passo a Passo — O que cada etapa faz e por que importa

> Leia este guia antes de rodar qualquer coisa. Ele explica cada passo
> do pipeline na ordem em que acontece, o que cada script faz por dentro,
> e o que quebraria se você pulasse alguma etapa.

---

## Visão rápida da ordem

```
PASSO 1  →  PASSO 2  →  PASSO 3  →  PASSO 4  →  PASSO 5  →  PASSO 6
Gerar        ETL          ETL          Banco        Carregar     Qualidade
dados        Clientes     Vendas       de dados     no DW        dos dados
(raw/)       (processed/) (processed/) (tabelas)    (star schema)(checks)
```

Cada passo depende do anterior. Se você pular o Passo 2, o Passo 3 não
encontra clientes para fazer o JOIN e as vendas ficam sem informação do cliente.

---

## PASSO 1 — Gerar os dados de exemplo

**Comando:**
```bash
python3 scripts/generate_sample_data.py --clientes 200 --vendas 1000
```

**O que faz por dentro:**

O script cria dados fictícios brasileiros do zero, sem precisar de nenhuma
fonte externa. Ele gera:

- 200 clientes com CPF com dígitos verificadores corretos, nome, email,
  telefone com DDD, cidade, UF, CEP por faixa da UF, segmento (B2B/B2C)
  e limite de crédito
- 1.000 vendas vinculadas aos clientes gerados, com pedido, produto,
  valor em R$, forma de pagamento (PIX, cartão, boleto), canal (e-commerce,
  loja física, app), status e número de parcelas

Os arquivos são salvos assim:

```
s3/raw/clientes/ano=2026/mes=04/dia=12/clientes_20260412_165657.json
s3/raw/vendas/ano=2026/mes=04/vendas_20260412_165657.csv
```

**Por que essa estrutura de pasta com ano/mes/dia?**

Isso se chama **particionamento por data**. Na AWS real, o Glue e o Athena
conseguem ler só o mês que você quer sem escanear todos os arquivos.
Imagine 3 anos de dados: sem partição você lê tudo, com partição você
lê só os arquivos do mês consultado. É muito mais rápido e mais barato.

**Por que JSON para clientes e CSV para vendas?**

Simula a realidade: sistemas CRM e APIs geralmente exportam JSON.
Sistemas ERP e planilhas geralmente exportam CSV. O pipeline precisa
saber lidar com os dois formatos, como acontece em qualquer empresa.

**O que acontece se pular este passo?**

Os passos 2 e 3 não encontram nenhum arquivo para processar e encerram
sem fazer nada.

---

## PASSO 2 — ETL de Clientes (raw → processed)

**Comando:**
```bash
python3 glue/jobs/clientes_raw_to_processed.py
```

**O que faz por dentro — as 10 transformações:**

O script lê o JSON bruto de `s3/raw/clientes/` e aplica estas etapas
na ordem:

**1. Remove linhas completamente vazias**
Linhas onde todos os campos são nulos não têm utilidade.

**2. Limpa e valida o CPF**
Remove formatação (pontos e traços), verifica se tem 11 dígitos e
rejeita CPFs com todos os dígitos iguais (como 111.111.111-11).
Registros com CPF inválido são descartados — CPF é a chave de negócio
do cliente, sem ele não dá para identificar quem é.

**3. Normaliza os nomes**
Converte para Title Case respeitando preposições:
`JOÃO DA SILVA` vira `João da Silva`
`maria santos  ` vira `Maria Santos`

**4. Valida emails**
Verifica se tem o formato mínimo com @. Emails inválidos viram nulos
(não descarta o cliente, só apaga o email quebrado).

**5. Normaliza telefone**
Converte qualquer formato para padrão `(XX) XXXXX-XXXX`.

**6. Valida a UF**
Verifica se é uma das 27 siglas válidas do Brasil.

**7. Padroniza tipos de dados**
Converte datas para o tipo correto, números para numérico.
Sem isso, um campo "data_cadastro" pode chegar como texto e
o banco vai rejeitar na hora de inserir.

**8. Remove duplicatas por CPF**
Se o mesmo cliente apareceu duas vezes (arquivo reprocessado, falha
de ingestão), mantém só o registro mais recente.

**9. Adiciona colunas de auditoria**
Marca `_processed_at` (quando foi processado) e `_job_name`
(qual job gerou o arquivo). Essencial para debugar problemas depois.

**10. Salva como Parquet particionado por UF**
```
s3/processed/clientes/uf=SP/clientes_20260412.parquet
s3/processed/clientes/uf=RJ/clientes_20260412.parquet
```

**O que é Parquet e por que usar?**

Parquet é um formato de arquivo colunar. Em vez de guardar linha por linha
como CSV, guarda coluna por coluna. Isso significa que uma query que só
precisa de `nome` e `uf` lê apenas essas duas colunas no disco, ignorando
todas as outras. Em tabelas com 100 colunas isso faz uma diferença enorme.
Além disso, o Parquet comprime automaticamente e já carrega o schema,
sem precisar dizer os tipos na mão.

**Por que particionar por UF (estado)?**

Se o time de analytics precisar analisar só clientes de SP, o Glue
lê apenas `uf=SP/` e ignora os outros 26 estados. Sem partição,
leria tudo.

**O que acontece se pular este passo?**

O Passo 3 (ETL de Vendas) tenta fazer um JOIN com os clientes processados.
Se não existir nenhum Parquet em `processed/clientes/`, as vendas serão
processadas sem o nome, cidade e segmento do cliente — informação que
vai faltar no Data Warehouse.

---

## PASSO 3 — ETL de Vendas (raw → processed)

**Comando:**
```bash
python3 glue/jobs/vendas_raw_to_processed.py
```

**O que faz por dentro:**

Lê os CSVs de `s3/raw/vendas/` e aplica transformações mais complexas
que as de clientes, porque vendas têm cálculos de negócio.

**1. Limpa valores monetários**
Converte strings como `R$ 1.250,99` para o float `1250.99`.
Sistemas brasileiros usam vírgula como decimal e ponto como milhar —
o oposto do padrão internacional. Sem essa conversão, o Python
leria `1.250,99` como texto e não conseguiria somar nada.

**2. Remove vendas com valor inválido**
Vendas com valor zero ou negativo são removidas — não existe venda
de R$ 0,00 em negócios reais.

**3. Normaliza datas**
Converte `data_venda` e `data_entrega` para o tipo datetime correto.

**4. Cria campos derivados (campos calculados)**

- `categoria_valor`: classifica a venda por faixa
  - até R$ 100 → PEQUENA
  - R$ 100 a R$ 500 → MEDIA
  - R$ 500 a R$ 2.000 → GRANDE
  - acima de R$ 2.000 → PREMIUM

- `margem_pct`: calcula a margem de lucro
  `(valor_total - custo) / valor_total * 100`

- `ano_venda`, `mes_venda`, `dia_semana`: extraídos da data para
  facilitar análises de sazonalidade no BI

**5. JOIN com clientes**
Este é o passo mais importante deste job. Ele cruza as vendas com
a tabela de clientes processada no Passo 2:

```
venda (cpf_cliente=12345678901) + cliente (cpf=12345678901)
  → venda enriquecida com: nome, cidade, uf, segmento_cliente
```

Assim, o registro de venda passa a ter o nome do cliente, estado
e segmento junto — sem precisar fazer JOIN na hora da consulta.
Isso deixa o BI muito mais rápido.

**6. Salva como Parquet particionado por ano e mês**
```
s3/processed/vendas/ano=2026/mes=04/vendas_20260412.parquet
s3/processed/vendas/ano=2025/mes=11/vendas_20260412.parquet
```

Cada grupo de mês vira um arquivo separado. Queries de período
(ex: "vendas do 4º trimestre") só leem os arquivos daqueles meses.

**O que acontece se pular este passo?**

O Data Warehouse não terá dados de vendas para analisar. Toda a
camada de relatórios fica sem informação.

---

## PASSO 4 — Inicializar o banco de dados

**Pré-requisito:** Docker rodando com PostgreSQL
```bash
docker-compose up -d postgres
python3 redshift/init_db.py
```

**O que faz por dentro:**

Executa os 4 arquivos SQL em ordem no PostgreSQL (que simula o Redshift):

**`001_create_schemas.sql` — Cria os 3 schemas**
```
staging  →  área temporária onde os dados chegam primeiro
dw       →  Data Warehouse com o modelo Star Schema final
dm       →  Data Marts, views prontas para o BI consumir
```

A separação em schemas é importante porque cada camada tem um propósito
diferente e diferentes permissões de acesso. O time de BI só acessa `dm`.
O Airflow acessa `staging` e `dw`. Nenhum acesso direto ao `raw`.

**`002_staging_tables.sql` — Tabelas de staging**

Réplicas quase idênticas dos arquivos Parquet. Os dados chegam aqui
primeiro, sem transformação, como uma "área de chegada" do aeroporto.
Isso permite validar antes de mover para o DW.

```sql
staging.stg_clientes  -- espelho do processed/clientes/
staging.stg_vendas    -- espelho do processed/vendas/
```

**`003_dw_tables.sql` — Star Schema (o coração do DW)**

Aqui está o modelo dimensional:

```
              dim_tempo
              (calendário)
                   │
dim_produto ── fato_vendas ── dim_clientes
(produtos)    (métricas)      (clientes)
                   │
              dim_canal
              (canais de venda)
```

- `fato_vendas`: guarda os números (valor, quantidade, custo, margem)
  e as chaves para as dimensões
- `dim_clientes`: guarda todos os atributos do cliente com suporte a
  histórico (SCD Tipo 2 — se o cliente mudar de cidade, o registro
  antigo é preservado com data de início e fim de vigência)
- `dim_tempo`: uma linha para cada dia do calendário, com atributos
  como dia da semana, semana do ano, trimestre, flag de feriado.
  Isso permite queries como "vendas em dias úteis do Q3" sem cálculos

**`004_data_marts.sql` — Views para BI**

Views pré-agregadas que o time de BI consome diretamente:
- `dm.vm_vendas_diarias` — total de vendas por dia
- `dm.vm_top_produtos` — produtos mais vendidos
- `dm.vm_vendas_por_uf` — mapa de calor por estado
- `dm.vm_desempenho_canais` — comparativo entre canais

**Por que ter staging + dw em vez de ir direto para o dw?**

Porque se um dado chegar errado, você corrige no staging e
reprocessa sem apagar o DW. O staging é descartável, o DW é o
ativo da empresa.

**O que acontece se pular este passo?**

O Passo 5 não consegue inserir nenhum dado porque as tabelas
não existem.

---

## PASSO 5 — Carregar dados no Data Warehouse

**Pré-requisito:** Passos 2, 3 e 4 completos
```bash
python3 redshift/init_db.py  # já inclui carga se configurado
```
Ou via stored procedures diretamente no banco:
```sql
CALL staging.sp_load_staging();
CALL staging.sp_transform_dw();
```

**O que faz por dentro — 2 stored procedures:**

**`sp_load_staging.sql` — Copia processed/ para staging**

Lê os arquivos Parquet de `s3/processed/` e insere nas tabelas
de staging. Na AWS real, isso seria um comando `COPY` do Redshift:
```sql
COPY staging.stg_clientes
FROM 's3://bucket/processed/clientes/'
IAM_ROLE 'arn:aws:iam::123:role/redshift-role'
FORMAT AS PARQUET;
```

Aqui, como usamos PostgreSQL local, lemos os Parquets com Python
e fazemos INSERT via psycopg2.

**`sp_transform_dw.sql` — staging → Star Schema**

Aplica as regras de negócio para transformar os dados planos do
staging no modelo dimensional do DW:

1. Atualiza `dim_clientes` com UPSERT (insert se não existe,
   update se mudou algo, mantém histórico pelo SCD Tipo 2)
2. Popula `dim_tempo` com todos os dias do período das vendas
3. Calcula `regiao` a partir da UF (SP, RJ, MG → Sudeste)
4. Calcula `faixa_etaria` a partir da data de nascimento
5. Insere na `fato_vendas` com as chaves surrogate das dimensões

**Por que usar surrogate keys (sk_cliente, sk_produto)?**

As dimensões usam chaves internas do DW (sk_cliente = 1, 2, 3...)
em vez das chaves de negócio (CPF). Isso porque:
- CPF pode mudar ou ser corrigido na fonte sem quebrar o histórico
- JOINs com inteiro são muito mais rápidos que com VARCHAR
- Suporta múltiplas fontes apontando para a mesma dimensão

**O que acontece se pular este passo?**

As views do `dm` ficam vazias e o BI não mostra nenhum dado.

---

## PASSO 6 — Verificar qualidade dos dados

**Comando:**
```bash
python3 data_quality/checks/dq_clientes.py
python3 data_quality/checks/dq_vendas.py
```

**O que faz por dentro:**

Roda um conjunto de verificações automáticas nos dados que estão
no banco ou nos arquivos processados:

**Checks de clientes:**
- CPF não pode ser nulo em nenhuma linha
- CPF deve ter exatamente 11 dígitos
- Email deve conter @ quando não for nulo
- Limite de crédito deve ser maior que zero
- UF deve ser uma das 27 siglas válidas

**Checks de vendas:**
- Valor total deve ser maior que zero
- Data de entrega deve ser posterior à data da venda
- Status deve ser um dos valores esperados:
  CONCLUIDA, CANCELADA, DEVOLVIDA ou PENDENTE
- Produto ID não pode ser nulo
- Quantidade deve ser maior que zero

**Se algum check falhar:**
- O script registra o erro com a contagem de registros afetados
- Gera um relatório em `data_quality/relatorios/`
- Retorna código de saída 1 (o Airflow interpreta isso como falha
  e não avança para o próximo passo do DAG)

**Por que validar depois de já ter transformado?**

Porque a transformação não garante que os dados fazem sentido para
o negócio — só garante que o formato está correto. A qualidade de
dados verifica as **regras de negócio**: um valor de venda de R$ 0,01
é tecnicamente válido (número positivo) mas provavelmente é um erro.

**O que acontece se pular este passo?**

Dados errados chegam no DW e nos relatórios de BI sem ninguém saber.
A empresa toma decisões baseadas em números incorretos. Este é o
passo que mais empresas ignoram — e o que mais causa problemas.

---

## PASSO 7 — Orquestrar tudo com Airflow (quando Docker disponível)

**Pré-requisito:** Docker com integração WSL ativada
```bash
docker-compose up -d airflow-webserver airflow-scheduler
# Acesse: http://localhost:8080 (admin / admin123)
```

**O que faz por dentro:**

O Airflow executa os passos 1 a 6 automaticamente, no horário certo,
na ordem certa, com retry automático em caso de falha.

**DAGs configuradas:**

```
dag_ingestion_clientes   →  roda às 02:00 todo dia
dag_ingestion_vendas     →  roda a cada hora
dag_etl_full_pipeline    →  roda às 04:00 (depois das ingestões)
dag_archiving            →  roda todo domingo às 03:00
```

**O DAG do pipeline completo (`dag_etl_full_pipeline`) executa assim:**

```
[início]
    │
    ▼
validar_schema_clientes ──► (se falhar) → notificar_falha → [fim com erro]
    │
    ▼
validar_schema_vendas
    │
    ▼
glue_job_clientes        ← roda ETL clientes (Passo 2)
    │
    ▼
glue_job_vendas          ← roda ETL vendas (Passo 3)
    │                       (precisa do passo anterior para o JOIN)
    ▼
load_staging             ← carrega processed/ no banco (Passo 5a)
    │
    ▼
transform_dw             ← staging → star schema (Passo 5b)
    │
    ▼
data_quality_checks      ← verifica qualidade (Passo 6)
    │
    ▼
notificar_sucesso        ← envia confirmação para o time
    │
    ▼
[fim com sucesso]
```

Se `glue_job_clientes` falhar, o Airflow tenta novamente 1 vez após
10 minutos (configurado em `retries: 1, retry_delay: timedelta(minutes=10)`).
Se falhar de novo, para o pipeline e envia alerta por email.

**Por que o Airflow é importante?**

Sem orquestração, você precisaria rodar os scripts manualmente na ordem
certa, no horário certo, todos os dias. Se um script falhar às 3h da manhã
você só vai saber quando o relatório estiver vazio às 9h. O Airflow cuida
disso: roda no horário, detecta falha, tenta de novo, avisa o time.

---

## Resumo — por que cada passo importa

| Passo | Se pular, o que quebra |
|---|---|
| 1. Gerar dados | Não há nada para processar — tudo fica vazio |
| 2. ETL Clientes | Vendas ficam sem nome/cidade/segmento do cliente |
| 3. ETL Vendas | DW não tem dados de vendas para analisar |
| 4. Inicializar banco | Não há tabelas — a carga falha com erro |
| 5. Carregar no DW | Views do BI ficam vazias |
| 6. Qualidade | Dados errados chegam nos relatórios sem aviso |
| 7. Airflow | Tudo precisa ser rodado manualmente, todo dia |

---

## Comandos para rodar agora (sem Docker)

Tudo que funciona hoje com Python puro, na ordem correta:

```bash
# Na raiz do projeto
cd /home/kayo/Data_Platform_ETL_ELT_Streaming

# Passo 1 — Gera os dados
python3 scripts/generate_sample_data.py --clientes 200 --vendas 1000

# Passo 2 — ETL clientes (precisa: pandas, pyarrow)
python3 glue/jobs/clientes_raw_to_processed.py

# Passo 3 — ETL vendas (precisa do Passo 2 para o JOIN)
python3 glue/jobs/vendas_raw_to_processed.py

# Passo 4+5 — Banco e carga (precisa: Docker com PostgreSQL)
docker-compose up -d postgres
python3 redshift/init_db.py

# Passo 6 — Qualidade
python3 data_quality/checks/dq_clientes.py
python3 data_quality/checks/dq_vendas.py
```

**Instalar dependências dos ETL jobs:**
```bash
pip3 install pandas pyarrow
```

---

*Guia gerado em 2026-04-12 | Projeto: Data Platform ETL/ELT/Streaming*
