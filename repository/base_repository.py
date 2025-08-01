from typing import Type, Any, Optional, List, Dict, Union, Iterator, TypeVar
from pynamodb.exceptions import DoesNotExist
from pynamodb.pagination import ResultIterator
from typing import Type
from pynamodb.models import Model
from pynamodb.expressions.update import SetAction
from pynamodb.expressions.operand import Path
from pynamodb.models import Model as PynamoModel
from pynamodb.expressions.condition import Condition

T = TypeVar("T", bound=PynamoModel)

class DynamoRepository:
    """Repositório genérico para operações DynamoDB com PynamoDB."""

    @staticmethod
    def get(model: Type[Model], hash_key: Any, range_key: Optional[Any] = None) -> Optional[Model]:
        if not hash_key:
            # Pode lançar ValueError, ou retornar None
            return None
        try:
            return model.get(hash_key, range_key) if range_key else model.get(hash_key)
        except DoesNotExist:
            return None

    @staticmethod
    def exists(model: Type[Model], hash_key: Any, range_key: Optional[Any] = None) -> bool:
        """Verifica se item existe na tabela."""
        return DynamoRepository.get(model, hash_key, range_key) is not None

    @staticmethod
    def insert(model_instance: Model) -> Model:
        """
        Inserts a new item into the table.
        """
        model_instance.save()
        return model_instance

    @staticmethod
    def update(
            model_instance: Model,
            hash_key_name: str,
            range_key_name: Optional[str] = None,
            consistent_read: bool = False
    ) -> Model:
        """
        Atualiza um item existente na tabela DynamoDB.

        :param model_instance: Instância do modelo PynamoDB preenchida.
        :param hash_key_name: Nome do atributo que é a chave hash.
        :param range_key_name: Nome do atributo que é a chave de range (opcional).
        :return: Instância atualizada do modelo.
        :raises DoesNotExist: Se o item não existir na tabela.
        """
        model_cls = type(model_instance)
        hash_key = getattr(model_instance, hash_key_name)
        range_key = getattr(model_instance, range_key_name) if range_key_name else None

        # Busca o item existente
        if range_key is not None:
            existing = model_cls.get(hash_key, range_key, consistent_read=consistent_read)
        else:
            existing = model_cls.get(hash_key, consistent_read=consistent_read)

        # Atualiza os atributos do item existente com os do model_instance
        for attr, value in model_instance.attribute_values.items():
            setattr(existing, attr, value)

        existing.save()
        return existing

    @staticmethod
    def upsert(
            model_instance: Model,
            hash_key_name: str,
            range_key_name: Optional[str] = None,
            consistent_read: bool = False
    ) -> Model:
        model_cls = type(model_instance)
        hash_key = getattr(model_instance, hash_key_name)
        range_key = getattr(model_instance, range_key_name) if range_key_name else None

        try:
            if range_key is not None:
                model_cls.get(hash_key, range_key, consistent_read=consistent_read)
            else:
                model_cls.get(hash_key, consistent_read=consistent_read)
            return DynamoRepository.update(model_instance, hash_key_name, range_key_name)
        except DoesNotExist:
            return DynamoRepository.insert(model_instance)

    @staticmethod
    def delete(model: Type[Model], hash_key: Any, range_key: Optional[Any] = None) -> None:
        """Remove item da tabela."""
        model(hash_key, range_key).delete() if range_key else model(hash_key).delete()

    @staticmethod
    def query(
            model_cls: Type[Model],
            hash_key_value: Optional[Any] = None,
            range_key_condition: Optional[Condition] = None,
            filter_condition: Optional[Condition] = None,
            limit: Optional[int] = None,
            scan_forward: bool = True,
            use_scan_if_missing_hash: bool = False,
            consistent_read: bool = False
    ) -> Iterator[Model]:
        """
        Consulta por:
        - Partition key apenas
        - Partition + Sort key
        - Apenas Sort key (usando scan se habilitado)

        :param model_cls: Classe do modelo.
        :param hash_key_value: Valor da partition key.
        :param range_key_condition: Condição sobre a sort key.
        :param filter_condition: Filtros adicionais (em colunas secundárias).
        :param limit: Número máximo de resultados.
        :param scan_forward: True para ordem crescente da sort key.
        :param use_scan_if_missing_hash: Permite scan caso hash key esteja ausente.
        """
        if hash_key_value is not None:
            # 1. Apenas Hash ou 2. Hash + Range
            return model_cls.query(
                hash_key_value,
                range_key_condition=range_key_condition,
                filter_condition=filter_condition,
                limit=limit,
                scan_index_forward=scan_forward,
                consistent_read=consistent_read
            )
        elif use_scan_if_missing_hash and range_key_condition is not None:
            # 3. Apenas Range Key via scan
            full_filter = (
                range_key_condition & filter_condition
                if filter_condition else range_key_condition
            )
            return model_cls.scan(
                filter_condition=full_filter,
                limit=limit,
                consistent_read=consistent_read
            )
        else:
            raise ValueError(
                "hash_key_value é obrigatório, exceto se use_scan_if_missing_hash=True e range_key_condition for fornecido."
            )

    @staticmethod
    def query_index(
            model_cls: Type[Model],
            index_name: str,
            hash_key_value: Optional[Any] = None,
            range_key_condition: Optional[Condition] = None,
            filter_condition: Optional[Condition] = None,
            limit: Optional[int] = None,
            scan_forward: bool = True,
            use_scan_if_missing_hash: bool = False,
            consistent_read: bool = False
    ) -> Iterator[Model]:
        """
        Consulta flexível usando índice (GSI ou LSI).

        :param model_cls: Classe do modelo.
        :param index_name: Nome do índice definido no modelo.
        :param hash_key_value: Valor da partition key do índice.
        :param range_key_condition: Condição da sort key do índice.
        :param filter_condition: Filtros adicionais.
        :param limit: Limite de resultados.
        :param scan_forward: Ordem crescente da sort key.
        :param use_scan_if_missing_hash: Permite scan se a hash key não for fornecida.
        """
        if hash_key_value is not None:
            # 1. Hash key apenas, ou 2. Hash + Range
            return model_cls.query(
                hash_key_value,
                index_name=index_name,
                range_key_condition=range_key_condition,
                filter_condition=filter_condition,
                limit=limit,
                scan_index_forward=scan_forward,
                consistent_read=consistent_read
            )
        elif use_scan_if_missing_hash and range_key_condition is not None:
            # 3. Apenas sort key do índice via scan
            full_filter = (
                range_key_condition & filter_condition
                if filter_condition else range_key_condition
            )
            return model_cls.scan(
                index_name=index_name,
                filter_condition=full_filter,
                limit=limit,
                consistent_read=consistent_read
            )
        else:
            raise ValueError(
                "hash_key_value é obrigatório, exceto se use_scan_if_missing_hash=True e range_key_condition for fornecido."
            )

    @staticmethod
    def scan(
        model: Type[Model],
        filter_condition: Optional[Condition] = None,
        limit: Optional[int] = None,
        consistent_read: bool = False
    ) -> Iterator[Model]:
        """Faz scan completo, com filtro opcional."""
        return model.scan(filter_condition=filter_condition, limit=limit, consistent_read=consistent_read)

    @staticmethod
    def scan_paginated(
        model: Type[Model],
        filter_condition: Optional[Condition] = None,
        page_size: int = 10,
        limit: Optional[int] = None,
        consistent_read: bool = False
    ) -> ResultIterator[Model]:
        """Faz scan paginado."""
        return model.scan(
            filter_condition=filter_condition,
            limit=limit,
            page_size=page_size,
            consistent_read=consistent_read
        )

    @staticmethod
    def batch_get(model: Type[Model], keys: List[Union[Any, tuple]], consistent_read: bool=True) -> Iterator[Model]:
        """Busca múltiplos itens por lote."""
        return model.batch_get(keys, consistent_read=consistent_read)

    @staticmethod
    def build_actions(updates: Dict[str, Any]) -> List[SetAction]:
        """Gera ações de atualização para o método update()."""
        return [SetAction(Path(attr), value) for attr, value in updates.items()]
