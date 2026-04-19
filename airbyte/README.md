# Airbyte - Ingestão de Dados de Múltiplas Fontes

## O que é o Airbyte?

**Airbyte** é uma plataforma open-source de integração de dados.
Conecta 300+ fontes de dados ao seu Data Lake automaticamente.

### Airbyte vs AWS Equivalentes

| Airbyte              | AWS Equivalente       | Diferença principal                    |
|----------------------|-----------------------|----------------------------------------|
| Source Connector     | AWS DMS               | Airbyte tem mais conectores prontos    |
| Full Refresh         | DMS Full Load         | Recopia tudo do zero                   |
| Incremental          | DMS CDC               | Só novos/alterados registros           |
| Normalization        | Glue (básico)         | Airbyte normaliza automaticamente      |
| Catalog              | Glue Data Catalog     | Define schema das streams              |

## Modos de Sincronização

### Full Refresh (Recarga Completa)
```
Fonte:     [A, B, C, D, E]
Destino:   [A, B, C, D, E]  ← tudo é copiado

Na próxima sync:
Fonte:     [A, B, C, D, E, F]
Destino:   [A, B, C, D, E, F]  ← tudo é copiado de novo
```

Quando usar: tabelas pequenas que mudam muito (ex: produtos, categorias)

### Incremental (Cursor-based)
```
Fonte:     [A, B, C, D, E]
1ª sync → Destino: [A, B, C, D, E], cursor: last_id=5

Fonte:     [A, B, C, D, E, F, G]
2ª sync → Destino: [A, B, C, D, E, F, G], cursor: last_id=7
         (só copiou F e G, que são novos!)
```

Quando usar: tabelas com campo de incremento (id, updated_at)

### CDC (Change Data Capture)
```
Monitora o binlog do banco de dados fonte
Captura INSERT, UPDATE, DELETE em tempo real
```

Quando usar: dados que mudam frequentemente (pedidos, status)

## Estrutura

```
airbyte/
├── sources/      → Onde os dados vêm (CRM, API, CSV)
├── destinations/ → Onde os dados vão (S3 raw/)
└── connections/  → Liga source → destination com sync mode
```
