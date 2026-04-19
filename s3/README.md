# S3 - Simulação do Amazon S3 (Data Lake)

## O que é o Amazon S3?

**Amazon S3 (Simple Storage Service)** é o serviço de armazenamento de objetos da AWS.
É como um "HD na nuvem", mas projetado para guardar qualquer tipo de arquivo em qualquer escala.

### Conceitos Fundamentais do S3

#### Bucket
Um bucket é como uma "pasta raiz" no S3. Cada bucket tem um nome único GLOBAL (em toda a AWS).
```
# AWS Real:
s3://minha-empresa-data-lake/

# Simulação local:
./s3/  (esta pasta)
```

#### Objetos
No S3, arquivos são chamados de "objetos". Cada objeto tem:
- **Key** (chave): o "caminho" do arquivo (ex: `raw/clientes/2024-01-15/clientes.json`)
- **Body**: o conteúdo do arquivo
- **Metadata**: informações extras (criação, tamanho, tipo)

#### Prefix (Prefixo)
O S3 não tem pastas de verdade! O que parece uma pasta é na verdade um "prefixo" na key.
```
# Parece uma pasta, mas é só um prefixo na key:
s3://bucket/raw/clientes/2024-01-15/arquivo.json
             ↑ prefixo    ↑ prefixo  ↑ key completa
```

#### Storage Classes (Classes de Armazenamento)
O S3 tem diferentes "classes" com diferentes custos e tempos de acesso:

| Classe          | Uso                          | Custo/GB/mês | Acesso    |
|-----------------|------------------------------|--------------|-----------|
| Standard        | Dados acessados frequentemente | $0.023      | Imediato  |
| Standard-IA     | Acesso infrequente           | $0.0125      | Imediato  |
| Glacier         | Arquivos históricos          | $0.004       | 3-5 horas |
| Glacier Deep    | Dados raramente acessados    | $0.00099     | 12 horas  |

### Lifecycle Policies (Políticas de Ciclo de Vida)
Regras automáticas para mover/deletar objetos baseado em tempo:
```
raw/ → após 30 dias → Standard-IA → após 90 dias → Glacier → após 365 dias → Delete
```

## Estrutura do Data Lake

### Arquitetura de Zonas (Medalhão)
```
s3/
├── raw/         (Bronze) → Dados brutos, como chegam da fonte
│   ├── clientes/          Dados do CRM (JSON)
│   ├── vendas/            Dados do sistema de vendas (CSV)
│   └── eventos/           Eventos do website (JSON streaming)
│
├── processed/   (Silver) → Dados limpos, validados, em Parquet
│   ├── clientes/          Clientes deduplificados, CPF validado
│   └── vendas/            Vendas com joinadas com clientes
│
└── archived/    (Gold comprimido) → Dados históricos (simula Glacier)
```

### Por que Parquet na zona processed?
O formato **Parquet** é colunar e comprimido:
- Arquivo CSV de 1GB → Parquet equivalente: ~100MB
- Queries 10-100x mais rápidas para analytics
- AWS Glue e Redshift Spectrum lêem Parquet nativamente

## Simulação Local vs AWS Real

| Aspecto         | AWS S3 Real                  | Simulação Local          |
|-----------------|------------------------------|--------------------------|
| Armazenamento   | Escala para PB               | Limitado pelo HD         |
| Replicação      | 3 zonas de disponibilidade   | Não tem                  |
| Durabilidade    | 99.999999999% (11 noves)     | Depende do HD            |
| Lifecycle       | Automático pela AWS          | Script Python (manual)   |
| Custo           | $0.023/GB/mês                | Grátis                   |
| API             | boto3: s3.upload_file()      | Funções em s3_utils.py   |

## Padrão de Nomenclatura de Arquivos

Sempre use particionamento por data nos prefixos:
```
raw/clientes/ano=2024/mes=01/dia=15/clientes_20240115_143022.json
             ↑ partição     ↑ partição        ↑ timestamp no nome
```
Isso permite ao Glue/Athena fazer "partition pruning" (ler só o necessário).

## Scripts Úteis

```python
# Importar utilitários do S3 local
from s3.scripts.s3_utils import S3Local

s3 = S3Local()

# Listar arquivos
arquivos = s3.list_objects('raw/clientes/')

# "Upload" de arquivo
s3.put_object('raw/clientes/2024-01-15/clientes.json', dados)

# "Download" de arquivo
conteudo = s3.get_object('raw/clientes/2024-01-15/clientes.json')
```
