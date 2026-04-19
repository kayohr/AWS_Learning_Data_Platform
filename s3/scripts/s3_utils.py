"""
s3_utils.py - Utilitários para simular o Amazon S3 localmente

CONCEITO AWS:
    O Amazon S3 é acessado via boto3 (SDK Python):
        import boto3
        s3 = boto3.client('s3')
        s3.upload_file('arquivo.json', 'meu-bucket', 'raw/clientes/arquivo.json')

    Aqui simulamos essa mesma API usando o sistema de arquivos local.
    O objetivo é aprender os conceitos antes de usar a AWS real.

EQUIVALÊNCIAS:
    s3.put_object()    → boto3: s3.put_object(Bucket=..., Key=..., Body=...)
    s3.get_object()    → boto3: s3.get_object(Bucket=..., Key=...)
    s3.list_objects()  → boto3: s3.list_objects_v2(Bucket=..., Prefix=...)
    s3.delete_object() → boto3: s3.delete_object(Bucket=..., Key=...)
"""

import os
import json
import shutil
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union

# Configuração de logging - no AWS seria o CloudWatch Logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('S3Local')


class S3Local:
    """
    Simula o cliente boto3 do Amazon S3 usando o sistema de arquivos local.

    No AWS real, você usaria:
        import boto3
        self.s3_client = boto3.client(
            's3',
            region_name='us-east-1',
            aws_access_key_id='SUA_ACCESS_KEY',
            aws_secret_access_key='SUA_SECRET_KEY'
        )

    Aqui, o "bucket" é o diretório base (s3/).
    A "key" é o caminho relativo dentro desse diretório.
    """

    def __init__(self, base_path: str = None):
        """
        Inicializa o cliente S3 local.

        Args:
            base_path: Caminho para a pasta que simula o bucket S3.
                      Padrão: pasta 's3/' relativa ao projeto.
        """
        if base_path is None:
            # Encontra a pasta s3/ relativa ao diretório do projeto
            projeto_root = Path(__file__).parent.parent.parent
            base_path = str(projeto_root / 's3')

        self.base_path = Path(base_path)

        # Garantir que as zonas do Data Lake existem
        for zona in ['raw', 'processed', 'archived']:
            (self.base_path / zona).mkdir(parents=True, exist_ok=True)

        logger.info(f"S3Local inicializado. Base path: {self.base_path}")

    def _key_to_path(self, key: str) -> Path:
        """
        Converte uma 'key' do S3 para um caminho local.

        No S3 real:
            key = 'raw/clientes/2024-01-15/clientes.json'
            → s3://meu-bucket/raw/clientes/2024-01-15/clientes.json

        Local:
            key = 'raw/clientes/2024-01-15/clientes.json'
            → /home/kayo/s3/raw/clientes/2024-01-15/clientes.json
        """
        # Remove barra inicial se houver
        key_clean = key.lstrip('/')
        return self.base_path / key_clean

    def put_object(self, key: str, body: Union[str, bytes, dict],
                   metadata: Dict = None) -> Dict:
        """
        Salva um objeto no S3 local.

        AWS equivalente:
            s3_client.put_object(
                Bucket='meu-bucket',
                Key='raw/clientes/arquivo.json',
                Body=json.dumps(dados),
                Metadata={'fonte': 'crm', 'versao': '1.0'}
            )

        Args:
            key: Caminho do objeto (ex: 'raw/clientes/2024-01-15/arquivo.json')
            body: Conteúdo do arquivo (string, bytes ou dict)
            metadata: Metadados opcionais (como tags no S3)

        Returns:
            Dict com informações do objeto criado (como response do boto3)
        """
        file_path = self._key_to_path(key)

        # Cria diretórios pai se não existirem (no S3 real, não há pastas)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # Converte dict para JSON automaticamente
        if isinstance(body, dict):
            body = json.dumps(body, ensure_ascii=False, indent=2)

        # Converte string para bytes se necessário
        if isinstance(body, str):
            body_bytes = body.encode('utf-8')
        else:
            body_bytes = body

        # Salva o arquivo
        with open(file_path, 'wb') as f:
            f.write(body_bytes)

        # Calcula MD5 (no S3 real, isso é o ETag)
        etag = hashlib.md5(body_bytes).hexdigest()

        # Salva metadados em arquivo separado (simula metadata do S3)
        if metadata:
            meta_path = file_path.with_suffix(file_path.suffix + '.meta')
            meta_data = {
                'key': key,
                'size': len(body_bytes),
                'etag': etag,
                'last_modified': datetime.now().isoformat(),
                'metadata': metadata
            }
            with open(meta_path, 'w') as f:
                json.dump(meta_data, f, ensure_ascii=False, indent=2)

        logger.info(f"PUT s3://{key} ({len(body_bytes)} bytes)")

        # Retorna resposta similar ao boto3
        return {
            'ETag': f'"{etag}"',
            'VersionId': None,
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'Key': key,
                'Size': len(body_bytes)
            }
        }

    def get_object(self, key: str) -> Optional[Dict]:
        """
        Lê um objeto do S3 local.

        AWS equivalente:
            response = s3_client.get_object(
                Bucket='meu-bucket',
                Key='raw/clientes/arquivo.json'
            )
            conteudo = response['Body'].read().decode('utf-8')

        Args:
            key: Caminho do objeto

        Returns:
            Dict com 'Body' (bytes) e 'ContentLength', ou None se não existir
        """
        file_path = self._key_to_path(key)

        if not file_path.exists():
            logger.warning(f"GET s3://{key} - OBJETO NÃO ENCONTRADO")
            return None

        with open(file_path, 'rb') as f:
            body = f.read()

        # Lê metadados se existirem
        meta_path = file_path.with_suffix(file_path.suffix + '.meta')
        metadata = {}
        if meta_path.exists():
            with open(meta_path, 'r') as f:
                metadata = json.load(f).get('metadata', {})

        logger.info(f"GET s3://{key} ({len(body)} bytes)")

        # Retorna resposta similar ao boto3
        # No boto3 real: response['Body'].read()
        return {
            'Body': body,
            'ContentLength': len(body),
            'LastModified': datetime.fromtimestamp(file_path.stat().st_mtime),
            'Metadata': metadata,
            'ContentType': 'application/json' if key.endswith('.json') else 'application/octet-stream'
        }

    def list_objects(self, prefix: str = '', max_keys: int = 1000) -> List[Dict]:
        """
        Lista objetos no S3 local com um determinado prefixo.

        AWS equivalente:
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket='meu-bucket',
                Prefix='raw/clientes/'
            )
            for page in pages:
                for obj in page['Contents']:
                    print(obj['Key'], obj['Size'])

        Args:
            prefix: Filtrar por prefixo (ex: 'raw/clientes/')
            max_keys: Máximo de resultados (S3 limita a 1000 por request)

        Returns:
            Lista de dicts com informações dos objetos
        """
        prefix_path = self._key_to_path(prefix)

        objetos = []

        if not prefix_path.exists():
            return objetos

        # Percorre recursivamente todos os arquivos
        for file_path in sorted(prefix_path.rglob('*')):
            # Pula diretórios e arquivos de metadados
            if file_path.is_dir() or file_path.suffix == '.meta':
                continue

            # Converte caminho local para key do S3
            key = str(file_path.relative_to(self.base_path))
            key = key.replace(os.sep, '/')  # Windows compatibility

            stat = file_path.stat()

            objetos.append({
                'Key': key,
                'Size': stat.st_size,
                'LastModified': datetime.fromtimestamp(stat.st_mtime),
                'ETag': f'"{hashlib.md5(open(file_path, "rb").read()).hexdigest()}"',
                'StorageClass': 'STANDARD'
            })

            if len(objetos) >= max_keys:
                break

        logger.info(f"LIST s3://{prefix} - {len(objetos)} objetos encontrados")
        return objetos

    def delete_object(self, key: str) -> Dict:
        """
        Delete um objeto do S3 local.

        AWS equivalente:
            s3_client.delete_object(
                Bucket='meu-bucket',
                Key='raw/clientes/arquivo-antigo.json'
            )

        Args:
            key: Chave do objeto a deletar

        Returns:
            Dict com status da operação
        """
        file_path = self._key_to_path(key)

        if not file_path.exists():
            logger.warning(f"DELETE s3://{key} - objeto não encontrado")
            return {'Deleted': False, 'Key': key}

        file_path.unlink()

        # Remove arquivo de metadados se existir
        meta_path = file_path.with_suffix(file_path.suffix + '.meta')
        if meta_path.exists():
            meta_path.unlink()

        logger.info(f"DELETE s3://{key}")
        return {'Deleted': True, 'Key': key}

    def copy_object(self, source_key: str, dest_key: str) -> Dict:
        """
        Copia um objeto de um lugar para outro no S3.

        AWS equivalente:
            s3_client.copy_object(
                Bucket='meu-bucket',
                CopySource={'Bucket': 'meu-bucket', 'Key': 'raw/arquivo.json'},
                Key='processed/arquivo.json'
            )

        Usado principalmente para:
            - Mover dados entre zonas (raw → processed → archived)
            - Versionamento manual de arquivos
        """
        source_path = self._key_to_path(source_key)

        if not source_path.exists():
            raise FileNotFoundError(f"Objeto de origem não encontrado: {source_key}")

        dest_path = self._key_to_path(dest_key)
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        shutil.copy2(source_path, dest_path)

        logger.info(f"COPY s3://{source_key} → s3://{dest_key}")
        return {
            'CopyObjectResult': {
                'ETag': f'"{hashlib.md5(open(dest_path, "rb").read()).hexdigest()}"',
                'LastModified': datetime.now().isoformat()
            }
        }

    def move_object(self, source_key: str, dest_key: str) -> Dict:
        """
        Move um objeto (copia e deleta o original).

        No S3 real não existe operação atômica de "move".
        É sempre: COPY + DELETE.
        """
        resultado = self.copy_object(source_key, dest_key)
        self.delete_object(source_key)
        logger.info(f"MOVE s3://{source_key} → s3://{dest_key}")
        return resultado

    def get_object_size(self, key: str) -> Optional[int]:
        """Retorna o tamanho em bytes de um objeto."""
        file_path = self._key_to_path(key)
        if file_path.exists():
            return file_path.stat().st_size
        return None

    def object_exists(self, key: str) -> bool:
        """
        Verifica se um objeto existe no S3.

        AWS equivalente:
            try:
                s3_client.head_object(Bucket='meu-bucket', Key=key)
                return True
            except ClientError:
                return False
        """
        return self._key_to_path(key).exists()

    def get_bucket_stats(self) -> Dict:
        """
        Retorna estatísticas do "bucket" (pasta s3/).
        Útil para monitoramento - no AWS seria via CloudWatch.
        """
        stats = {
            'total_objects': 0,
            'total_size_bytes': 0,
            'zones': {}
        }

        for zona in ['raw', 'processed', 'archived']:
            zona_path = self.base_path / zona
            if not zona_path.exists():
                continue

            zona_count = 0
            zona_size = 0

            for f in zona_path.rglob('*'):
                if f.is_file() and not f.suffix == '.meta':
                    zona_count += 1
                    zona_size += f.stat().st_size

            stats['zones'][zona] = {
                'objects': zona_count,
                'size_bytes': zona_size,
                'size_mb': round(zona_size / 1024 / 1024, 2)
            }
            stats['total_objects'] += zona_count
            stats['total_size_bytes'] += zona_size

        stats['total_size_mb'] = round(stats['total_size_bytes'] / 1024 / 1024, 2)
        return stats


# =============================================================================
# FUNÇÕES DE CONVENIÊNCIA (API simplificada)
# =============================================================================

def upload_to_raw(filename: str, content: Union[str, bytes, dict],
                  source: str = 'unknown', date: str = None) -> str:
    """
    Faz upload de um arquivo para a zona raw/ do Data Lake.

    No AWS real:
        - Arquivo vai para s3://bucket/raw/{source}/ano={ano}/mes={mes}/dia={dia}/
        - Lambda pode ser triggerada automaticamente (S3 Event Notification)
        - Glue Crawler detecta e atualiza o catálogo automaticamente

    Args:
        filename: Nome do arquivo
        content: Conteúdo a salvar
        source: Fonte dos dados (ex: 'clientes', 'vendas', 'eventos')
        date: Data no formato YYYY-MM-DD (padrão: hoje)

    Returns:
        Key do objeto criado
    """
    if date is None:
        date = datetime.now().strftime('%Y-%m-%d')

    ano, mes, dia = date.split('-')

    # Padrão de particionamento Hive (compatível com Glue e Athena)
    key = f"raw/{source}/ano={ano}/mes={mes}/dia={dia}/{filename}"

    s3 = S3Local()
    s3.put_object(key, content, metadata={'fonte': source, 'data_upload': date})

    return key


# =============================================================================
# TESTE RÁPIDO (rodar diretamente: python s3_utils.py)
# =============================================================================
if __name__ == '__main__':
    print("=== Testando S3Local ===\n")

    s3 = S3Local()

    # Teste 1: Salvar um arquivo JSON
    print("1. Salvando arquivo de teste...")
    dados_teste = {
        'id': 1,
        'nome': 'João da Silva',
        'cpf': '123.456.789-00',
        'cidade': 'São Paulo'
    }
    s3.put_object('raw/clientes/teste/cliente_teste.json', dados_teste)

    # Teste 2: Ler o arquivo
    print("2. Lendo arquivo de teste...")
    resultado = s3.get_object('raw/clientes/teste/cliente_teste.json')
    if resultado:
        print(f"   Conteúdo: {resultado['Body'].decode('utf-8')[:100]}...")

    # Teste 3: Listar objetos
    print("3. Listando objetos em raw/clientes/...")
    objetos = s3.list_objects('raw/clientes/')
    for obj in objetos:
        print(f"   - {obj['Key']} ({obj['Size']} bytes)")

    # Teste 4: Estatísticas do bucket
    print("\n4. Estatísticas do Data Lake:")
    stats = s3.get_bucket_stats()
    for zona, info in stats['zones'].items():
        print(f"   - {zona}/: {info['objects']} objetos, {info['size_mb']} MB")

    print("\n=== Testes concluídos! ===")
