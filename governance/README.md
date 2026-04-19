# Governance - Governança e Segurança de Dados

## O que é Governança de Dados?

Governança de dados são as **políticas, processos e controles** que garantem que os dados
sejam usados de forma correta, segura e conforme as regulamentações.

## IAM - Identity and Access Management

No AWS, o IAM define **quem pode fazer o quê**:

```
╔══════════════════════════════════════════╗
║  Princípio do Mínimo Privilégio          ║
║  Dar apenas as permissões NECESSÁRIAS    ║
╚══════════════════════════════════════════╝

Exemplo correto:
    glue-etl-role → pode LER de s3/raw/ e ESCREVER em s3/processed/
    glue-etl-role → NÃO pode escrever em s3/raw/ (zone protection)
    glue-etl-role → NÃO pode deletar nada no S3

Exemplo incorreto (antipadrão):
    glue-etl-role → tem acesso TOTAL ao S3 (muito permissivo!)
```

## LGPD - Lei Geral de Proteção de Dados

A LGPD (Lei 13.709/2018) regula o tratamento de dados pessoais no Brasil.
Equivalente ao GDPR europeu.

Obrigações para plataformas de dados:
- **Minimização**: Coletar apenas dados necessários
- **Limitação de finalidade**: Usar dados só para o propósito declarado
- **Retenção**: Definir e cumprir prazos de retenção
- **Acesso**: Responder requisições de titulares (SAR - Subject Access Request)
- **Anonimização**: Anonimizar dados pessoais quando não necessários

## Dados Sensíveis neste Projeto

| Campo         | Categoria        | Proteção                              |
|---------------|------------------|---------------------------------------|
| CPF           | Dado pessoal     | Hash ou mascaramento em logs          |
| Nome          | Dado pessoal     | Acesso controlado por IAM role        |
| Email         | Dado pessoal     | Criptografado em repouso (S3 SSE)     |
| Telefone      | Dado pessoal     | Mascarado: (11) 9****-****            |
| Valor compras | Financeiro       | Acesso restrito ao time de finanças   |

## Arquivos de Simulação

- `iam/roles.json` - Simula IAM roles (quais serviços têm quais permissões)
- `iam/policies.json` - Simula IAM policies (definição das permissões)
- `catalog/data_dictionary.md` - Dicionário de dados completo
