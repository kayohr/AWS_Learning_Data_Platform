"""
generate_sample_data.py - Gera dados de exemplo realistas brasileiros

Gera:
  - Clientes com CPF, nomes e cidades brasileiras
  - Vendas com valores em R$, produtos e canais reais
  - Eventos de website

USO:
    python scripts/generate_sample_data.py
    python scripts/generate_sample_data.py --clientes 500 --vendas 2000
"""

import os
import sys
import json
import csv
import random
import hashlib
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta, date
from typing import List, Dict

projeto_root = Path(__file__).parent.parent
sys.path.insert(0, str(projeto_root))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [GENERATE] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('GenerateSampleData')

# Dados brasileiros reais para simulação
NOMES_MASCULINOS = [
    'João', 'Pedro', 'Carlos', 'Paulo', 'Marcos', 'Lucas', 'Rafael',
    'André', 'Felipe', 'Bruno', 'Diego', 'Rodrigo', 'Eduardo', 'Thiago',
    'Guilherme', 'Leonardo', 'Matheus', 'Gabriel', 'Gustavo', 'Ricardo'
]
NOMES_FEMININOS = [
    'Maria', 'Ana', 'Juliana', 'Patricia', 'Sandra', 'Fernanda', 'Amanda',
    'Camila', 'Larissa', 'Bianca', 'Vanessa', 'Leticia', 'Mariana', 'Carla',
    'Priscila', 'Renata', 'Claudia', 'Tatiana', 'Aline', 'Natalia'
]
SOBRENOMES = [
    'Silva', 'Santos', 'Oliveira', 'Souza', 'Rodrigues', 'Ferreira',
    'Alves', 'Pereira', 'Lima', 'Gomes', 'Costa', 'Ribeiro', 'Martins',
    'Carvalho', 'Almeida', 'Lopes', 'Soares', 'Fernandes', 'Vieira', 'Barbosa',
    'Rocha', 'Dias', 'Nascimento', 'Andrade', 'Moreira', 'Nunes', 'Marques',
    'Machado', 'Mendes', 'Freitas', 'Cardoso', 'Ramos', 'Gonçalves', 'Luz',
    'Castro', 'Araújo', 'Pinto', 'Teixeira', 'Correia', 'Miranda'
]
CIDADES_POR_UF = {
    'SP': ['São Paulo', 'Campinas', 'Santos', 'Ribeirão Preto', 'São José dos Campos',
            'Sorocaba', 'Mogi das Cruzes', 'Osasco', 'Guarulhos', 'Santo André'],
    'RJ': ['Rio de Janeiro', 'Niterói', 'Nova Iguaçu', 'Duque de Caxias',
            'São Gonçalo', 'Belford Roxo', 'Campos dos Goytacazes', 'Volta Redonda'],
    'MG': ['Belo Horizonte', 'Uberlândia', 'Contagem', 'Juiz de Fora',
            'Betim', 'Montes Claros', 'Uberaba', 'Governador Valadares'],
    'RS': ['Porto Alegre', 'Caxias do Sul', 'Pelotas', 'Canoas', 'Santa Maria',
            'Gravataí', 'Viamão', 'Novo Hamburgo'],
    'PR': ['Curitiba', 'Londrina', 'Maringá', 'Ponta Grossa', 'Cascavel',
            'São José dos Pinhais', 'Foz do Iguaçu'],
    'SC': ['Florianópolis', 'Joinville', 'Blumenau', 'Chapecó', 'Itajaí'],
    'BA': ['Salvador', 'Feira de Santana', 'Vitória da Conquista', 'Camaçari'],
    'PE': ['Recife', 'Caruaru', 'Petrolina', 'Olinda', 'CARUARU'],
    'CE': ['Fortaleza', 'Caucaia', 'Juazeiro do Norte', 'Maracanaú'],
    'GO': ['Goiânia', 'Aparecida de Goiânia', 'Anápolis', 'Rio Verde'],
    'DF': ['Brasília', 'Taguatinga', 'Ceilândia', 'Samambaia'],
    'AM': ['Manaus', 'Parintins', 'Itacoatiara'],
    'PA': ['Belém', 'Ananindeua', 'Santarém', 'Marabá'],
    'ES': ['Vitória', 'Vila Velha', 'Cariacica', 'Serra'],
    'MT': ['Cuiabá', 'Várzea Grande', 'Rondonópolis'],
    'MS': ['Campo Grande', 'Dourados', 'Corumbá'],
    'RN': ['Natal', 'Mossoró', 'Parnamirim'],
    'PB': ['João Pessoa', 'Campina Grande', 'Santa Rita'],
    'AL': ['Maceió', 'Arapiraca', 'Palmeira dos Índios'],
    'MA': ['São Luís', 'Imperatriz', 'Timon'],
    'PI': ['Teresina', 'Parnaíba', 'Picos'],
    'SE': ['Aracaju', 'Nossa Senhora do Socorro'],
    'RO': ['Porto Velho', 'Ji-Paraná'],
    'AC': ['Rio Branco', 'Cruzeiro do Sul'],
    'AP': ['Macapá', 'Santana'],
    'RR': ['Boa Vista', 'Rorainópolis'],
    'TO': ['Palmas', 'Araguaína']
}

PROVEDORES_EMAIL = ['gmail.com', 'hotmail.com', 'yahoo.com.br', 'outlook.com',
                     'uol.com.br', 'terra.com.br', 'bol.com.br']

PRODUTOS = [
    {'id': 'ELET-001', 'nome': 'Smartphone Samsung Galaxy A54', 'cat': 'Eletrônicos', 'min': 1200, 'max': 2200},
    {'id': 'ELET-002', 'nome': 'Notebook Dell Inspiron 15', 'cat': 'Eletrônicos', 'min': 2800, 'max': 5500},
    {'id': 'ELET-003', 'nome': 'Smart TV 55" LG OLED', 'cat': 'Eletrônicos', 'min': 3500, 'max': 7000},
    {'id': 'ELET-004', 'nome': 'Fone JBL Tune 760NC', 'cat': 'Eletrônicos', 'min': 350, 'max': 600},
    {'id': 'ELET-005', 'nome': 'iPad Air 5ª Geração', 'cat': 'Eletrônicos', 'min': 4500, 'max': 7500},
    {'id': 'CALC-001', 'nome': 'Tênis Nike Air Max 270', 'cat': 'Calçados', 'min': 450, 'max': 900},
    {'id': 'CALC-002', 'nome': 'Bota Couro Timberland', 'cat': 'Calçados', 'min': 600, 'max': 1200},
    {'id': 'VEST-001', 'nome': 'Camiseta Polo Masculina', 'cat': 'Vestuário', 'min': 80, 'max': 250},
    {'id': 'VEST-002', 'nome': 'Vestido Social Feminino', 'cat': 'Vestuário', 'min': 150, 'max': 450},
    {'id': 'CASA-001', 'nome': 'Cafeteira Nespresso Essenza', 'cat': 'Casa', 'min': 400, 'max': 700},
    {'id': 'CASA-002', 'nome': 'Aspirador Robô iRobot', 'cat': 'Casa', 'min': 1500, 'max': 3500},
    {'id': 'ESPO-001', 'nome': 'Bicicleta Caloi 21 Marchas', 'cat': 'Esportes', 'min': 800, 'max': 2000},
    {'id': 'LIVR-001', 'nome': 'Livro - O Poder do Hábito', 'cat': 'Livros', 'min': 35, 'max': 60},
    {'id': 'BELE-001', 'nome': 'Perfume Natura Essencial', 'cat': 'Beleza', 'min': 120, 'max': 280},
    {'id': 'ALIM-001', 'nome': 'Kit Chocolate Belga Importado', 'cat': 'Alimentação', 'min': 80, 'max': 200},
]

FORMAS_PAGAMENTO = ['CARTAO_CREDITO'] * 4 + ['PIX'] * 3 + ['BOLETO'] * 2 + ['CARTAO_DEBITO']
CANAIS = ['ECOMMERCE'] * 5 + ['LOJA_FISICA'] * 3 + ['APP_MOBILE'] * 2 + ['TELEVENDAS']
STATUS = ['CONCLUIDA'] * 85 + ['CANCELADA'] * 8 + ['DEVOLVIDA'] * 5 + ['PENDENTE'] * 2


def gerar_cpf() -> str:
    """Gera um CPF fictício com dígitos verificadores corretos."""
    def calcular_digito(cpf_parcial: List[int], multiplicadores: List[int]) -> int:
        soma = sum(d * m for d, m in zip(cpf_parcial, multiplicadores))
        resto = soma % 11
        return 0 if resto < 2 else 11 - resto

    digitos = [random.randint(0, 9) for _ in range(9)]
    digitos.append(calcular_digito(digitos, range(10, 1, -1)))
    digitos.append(calcular_digito(digitos, range(11, 1, -1)))
    return ''.join(map(str, digitos))


def gerar_cep(uf: str) -> str:
    """Gera um CEP fictício coerente com a UF."""
    faixas_cep = {
        'SP': (1000000, 19999999), 'RJ': (20000000, 28999999),
        'MG': (30000000, 39999999), 'ES': (29000000, 29999999),
        'BA': (40000000, 48999999), 'SE': (49000000, 49999999),
        'PE': (50000000, 56999999), 'AL': (57000000, 57999999),
        'PB': (58000000, 58999999), 'RN': (59000000, 59999999),
        'CE': (60000000, 63999999), 'PI': (64000000, 64999999),
        'MA': (65000000, 65999999), 'PA': (66000000, 68899999),
        'AP': (68900000, 68999999), 'AM': (69000000, 69299999),
        'RR': (69300000, 69399999), 'AC': (69900000, 69999999),
        'RO': (76800000, 76999999), 'MT': (78000000, 78999999),
        'MS': (79000000, 79999999), 'GO': (72800000, 76799999),
        'DF': (70000000, 72799999), 'TO': (77000000, 77999999),
        'RS': (90000000, 99999999), 'SC': (88000000, 89999999),
        'PR': (80000000, 87999999)
    }
    faixa = faixas_cep.get(uf, (10000000, 99999999))
    return str(random.randint(*faixa)).zfill(8)


def gerar_clientes(quantidade: int) -> List[Dict]:
    """Gera lista de clientes com dados brasileiros realistas."""
    logger.info(f"Gerando {quantidade} clientes...")
    clientes = []
    cpfs_usados = set()

    ufs = list(CIDADES_POR_UF.keys())
    # Distribui mais clientes em SP e RJ (mais populosos)
    pesos_uf = [0.20 if uf == 'SP' else 0.10 if uf == 'RJ' else
                0.07 if uf in ('MG', 'RS', 'PR') else 0.03
                for uf in ufs]
    total_peso = sum(pesos_uf)
    pesos_uf = [p / total_peso for p in pesos_uf]

    for i in range(quantidade):
        # CPF único
        cpf = gerar_cpf()
        while cpf in cpfs_usados:
            cpf = gerar_cpf()
        cpfs_usados.add(cpf)

        # Nome aleatório
        masculino = random.random() > 0.5
        if masculino:
            nome = f"{random.choice(NOMES_MASCULINOS)} {random.choice(SOBRENOMES)}"
        else:
            nome = f"{random.choice(NOMES_FEMININOS)} {random.choice(SOBRENOMES)}"

        # Localização
        uf = random.choices(ufs, weights=pesos_uf)[0]
        cidade = random.choice(CIDADES_POR_UF[uf])

        # Data de nascimento (entre 18 e 80 anos)
        anos_atras = random.randint(18 * 365, 80 * 365)
        data_nasc = (date.today() - timedelta(days=anos_atras)).strftime('%Y-%m-%d')

        # Data de cadastro (nos últimos 5 anos)
        dias_cadastro = random.randint(0, 5 * 365)
        data_cadastro = (datetime.now() - timedelta(days=dias_cadastro)).isoformat()

        # Email único baseado no nome
        nome_email = nome.lower().replace(' ', '.').replace('ã', 'a').replace('é', 'e') \
                         .replace('ê', 'e').replace('ú', 'u').replace('ó', 'o') \
                         .replace('ç', 'c').replace('à', 'a').replace('â', 'a')
        email = f"{nome_email}{random.randint(1, 999)}@{random.choice(PROVEDORES_EMAIL)}"

        # Telefone celular
        ddd = random.choice(['11', '12', '13', '14', '15', '16', '17', '18', '19',  # SP
                              '21', '22', '24',  # RJ
                              '31', '32', '33', '34', '35',  # MG
                              '41', '42', '43', '44', '45', '46',  # PR
                              '51', '53', '54', '55',  # RS
                              '61',  # DF
                              '71', '73', '74', '75', '77',  # BA
                              '81', '87',  # PE
                              '85', '88',  # CE
                              '91', '93', '94'])  # PA
        telefone = f"({ddd}) 9{random.randint(1000, 9999)}-{random.randint(1000, 9999)}"

        # Segmento
        segmento = 'B2B' if random.random() < 0.15 else 'B2C'

        # Limite de crédito
        limite = round(random.choice([500, 1000, 2000, 3000, 5000, 8000, 10000, 15000, 20000]), 2)

        clientes.append({
            'id': i + 1,
            'cpf': cpf,
            'nome': nome,
            'email': email,
            'telefone': telefone,
            'data_nascimento': data_nasc,
            'data_cadastro': data_cadastro,
            'cidade': cidade,
            'uf': uf,
            'cep': gerar_cep(uf),
            'segmento_cliente': segmento,
            'limite_credito': limite,
            'ativo': random.random() > 0.05  # 95% ativos
        })

    logger.info(f"  {quantidade} clientes gerados com sucesso!")
    return clientes


def gerar_vendas(clientes: List[Dict], quantidade: int) -> List[Dict]:
    """Gera vendas realistas baseadas nos clientes existentes."""
    logger.info(f"Gerando {quantidade} vendas...")
    vendas = []

    # Data de início: 1 ano atrás
    data_inicio = datetime.now() - timedelta(days=365)

    for i in range(quantidade):
        cliente = random.choice(clientes)
        produto = random.choice(PRODUTOS)

        # Data da venda (distribuída no último ano, mais recente = mais vendas)
        peso = random.betavariate(2, 1)  # Beta distribution: mais vendas recentes
        dias_atras = int((1 - peso) * 365)
        data_venda = data_inicio + timedelta(days=dias_atras,
                                              hours=random.randint(8, 22),
                                              minutes=random.randint(0, 59))

        qtd = random.choices([1, 1, 1, 2, 2, 3], weights=[50, 25, 15, 7, 2, 1])[0]
        valor_unit = round(random.uniform(produto['min'], produto['max']), 2)

        # Desconto (30% das vendas têm desconto)
        if random.random() < 0.3:
            pct_desconto = random.choice([0.05, 0.10, 0.15, 0.20])
            desconto = round(valor_unit * qtd * pct_desconto, 2)
        else:
            desconto = 0.0

        valor_total = round(valor_unit * qtd - desconto, 2)
        custo = round(valor_total * random.uniform(0.40, 0.75), 2)

        canal = random.choices(CANAIS)[0]
        status = random.choices(STATUS)[0]
        forma_pgto = random.choices(FORMAS_PAGAMENTO)[0]
        parcelas = 1
        if forma_pgto == 'CARTAO_CREDITO' and valor_total > 200:
            parcelas = random.choice([1, 2, 3, 6, 12])

        # Data de entrega (3-10 dias úteis depois)
        data_entrega = data_venda + timedelta(days=random.randint(3, 10))

        vendas.append({
            'id_venda': i + 1,
            'id_pedido': f"PED-{data_venda.strftime('%Y%m%d')}-{i + 1:06d}",
            'cpf_cliente': cliente['cpf'],
            'data_venda': data_venda.isoformat(),
            'data_entrega': data_entrega.isoformat() if status not in ('CANCELADA', 'PENDENTE') else '',
            'produto_id': produto['id'],
            'produto_nome': produto['nome'],
            'categoria_produto': produto['cat'],
            'quantidade': qtd,
            'valor_unitario': valor_unit,
            'desconto': desconto,
            'valor_total': valor_total,
            'custo': custo,
            'canal_venda': canal,
            'status_venda': status,
            'forma_pagamento': forma_pgto,
            'parcelas': parcelas,
            'vendedor_id': f"VND{random.randint(1, 50):03d}",
            'loja_id': f"LOJA{random.randint(1, 20):02d}" if canal == 'LOJA_FISICA' else ''
        })

    logger.info(f"  {quantidade} vendas geradas!")
    return vendas


def salvar_clientes(clientes: List[Dict], pasta: Path):
    """Salva clientes como JSON no S3 raw/clientes/."""
    pasta.mkdir(parents=True, exist_ok=True)
    hoje = datetime.now()
    pasta_particionada = pasta / f"ano={hoje.year}" / f"mes={hoje.month:02d}" / f"dia={hoje.day:02d}"
    pasta_particionada.mkdir(parents=True, exist_ok=True)

    arquivo = pasta_particionada / f"clientes_{hoje.strftime('%Y%m%d_%H%M%S')}.json"
    with open(arquivo, 'w', encoding='utf-8') as f:
        json.dump({'clientes': clientes, 'total': len(clientes),
                   'gerado_em': datetime.now().isoformat()}, f,
                  ensure_ascii=False, indent=2, default=str)

    logger.info(f"Clientes salvos em: {arquivo}")
    return str(arquivo)


def salvar_vendas(vendas: List[Dict], pasta: Path):
    """Salva vendas como CSV no S3 raw/vendas/."""
    pasta.mkdir(parents=True, exist_ok=True)
    hoje = datetime.now()
    pasta_particionada = pasta / f"ano={hoje.year}" / f"mes={hoje.month:02d}"
    pasta_particionada.mkdir(parents=True, exist_ok=True)

    arquivo = pasta_particionada / f"vendas_{hoje.strftime('%Y%m%d_%H%M%S')}.csv"

    if vendas:
        fieldnames = list(vendas[0].keys())
        with open(arquivo, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(vendas)

    logger.info(f"Vendas salvas em: {arquivo}")
    return str(arquivo)


def main():
    parser = argparse.ArgumentParser(
        description='Gera dados de exemplo realistas brasileiros'
    )
    parser.add_argument('--clientes', type=int,
                        default=int(os.getenv('SAMPLE_DATA_CLIENTES', 500)))
    parser.add_argument('--vendas', type=int,
                        default=int(os.getenv('SAMPLE_DATA_VENDAS', 2000)))
    parser.add_argument('--eventos', type=int,
                        default=int(os.getenv('SAMPLE_DATA_EVENTOS', 1000)))
    args = parser.parse_args()

    inicio = datetime.now()
    logger.info("=" * 60)
    logger.info("GERANDO DADOS DE EXEMPLO")
    logger.info(f"  Clientes: {args.clientes}")
    logger.info(f"  Vendas  : {args.vendas}")
    logger.info(f"  Eventos : {args.eventos}")
    logger.info("=" * 60)

    # Gera e salva clientes
    clientes = gerar_clientes(args.clientes)
    salvar_clientes(clientes, projeto_root / 's3' / 'raw' / 'clientes')

    # Gera e salva vendas
    vendas = gerar_vendas(clientes, args.vendas)
    salvar_vendas(vendas, projeto_root / 's3' / 'raw' / 'vendas')

    # Gera eventos de click
    if args.eventos > 0:
        logger.info(f"Gerando {args.eventos} eventos de click...")
        from streaming.producers.evento_click_producer import gerar_evento_click, salvar_em_arquivo
        for i in range(args.eventos):
            evento = gerar_evento_click()
            salvar_em_arquivo(evento, str(projeto_root / 's3' / 'raw' / 'eventos'))
        logger.info(f"  {args.eventos} eventos salvos!")

    duracao = (datetime.now() - inicio).total_seconds()
    logger.info("=" * 60)
    logger.info("DADOS GERADOS COM SUCESSO!")
    logger.info(f"  Clientes: {args.clientes} em s3/raw/clientes/")
    logger.info(f"  Vendas  : {args.vendas} em s3/raw/vendas/")
    logger.info(f"  Eventos : {args.eventos} em s3/raw/eventos/")
    logger.info(f"  Duração : {duracao:.1f}s")
    logger.info("=" * 60)
    logger.info("Próximo passo: python redshift/init_db.py")


if __name__ == '__main__':
    main()

    # ==========================================================================
    # VISUALIZAÇÃO — descomente o bloco abaixo para ver os dados gerados
    # Basta remover o  '''  de abertura e fechamento
    # ==========================================================================
    '''
    import json, glob
    from tabulate import tabulate

    print("\n" + "=" * 60)
    print("PRÉVIA DOS DADOS GERADOS")
    print("=" * 60)

    # -- Clientes (JSON) -------------------------------------------------------
    arquivos_cli = sorted(glob.glob("s3/raw/clientes/**/*.json", recursive=True))
    if arquivos_cli:
        with open(arquivos_cli[-1], encoding="utf-8") as f:
            dados = json.load(f)
        clientes = dados.get("clientes", [])
        print(f"\n>>> s3/raw/clientes/  ({len(clientes)} registros)\n")
        colunas = ["id", "cpf", "nome", "email", "cidade", "uf",
                   "segmento_cliente", "limite_credito"]
        linhas = [[c.get(col, "") for col in colunas] for c in clientes[:10]]
        print(tabulate(linhas, headers=colunas, tablefmt="rounded_outline"))
        print(f"  ... e mais {max(0, len(clientes)-10)} linhas")

    # -- Vendas (CSV) ----------------------------------------------------------
    import csv
    arquivos_ven = sorted(glob.glob("s3/raw/vendas/**/*.csv", recursive=True))
    if arquivos_ven:
        with open(arquivos_ven[-1], encoding="utf-8") as f:
            reader = csv.DictReader(f)
            vendas = list(reader)
        print(f"\n>>> s3/raw/vendas/  ({len(vendas)} registros)\n")
        colunas = ["id_pedido", "produto_nome", "valor_total",
                   "canal_venda", "forma_pagamento", "status_venda"]
        linhas = [[v.get(col, "") for col in colunas] for v in vendas[:10]]
        print(tabulate(linhas, headers=colunas, tablefmt="rounded_outline"))
        print(f"  ... e mais {max(0, len(vendas)-10)} linhas")

        # Resumo financeiro
        valores = [float(v["valor_total"]) for v in vendas if v.get("valor_total")]
        print(f"\n  Receita total : R$ {sum(valores):,.2f}")
        print(f"  Ticket médio  : R$ {sum(valores)/len(valores):,.2f}")
        print(f"  Maior venda   : R$ {max(valores):,.2f}")
    '''
