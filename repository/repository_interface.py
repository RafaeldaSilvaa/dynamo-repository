from abc import ABC, abstractmethod
from typing import Type, Any, Optional, List, Dict, Union, Iterator, TypeVar
from pynamodb.models import Model as PynamoModel
from pynamodb.expressions.condition import Condition
from pynamodb.expressions.update import SetAction
from pynamodb.pagination import ResultIterator

T = TypeVar("T", bound=PynamoModel)

class IDynamoRepository(ABC):
    """
    Interface genérica para repositórios de dados DynamoDB com PynamoDB.

    Define os métodos para CRUD, consultas e operações avançadas,
    garantindo assinatura consistente para implementações concretas.
    """

    @staticmethod
    @abstractmethod
    def get(model: Type[T], hash_key: Any, range_key: Optional[Any] = None) -> Optional[T]:
        """
        Obtém um item pelo hash_key e range_key opcional.
        Retorna None se não encontrado ou parâmetros inválidos.
        """
        return None

    @staticmethod
    @abstractmethod
    def exists(model: Type[T], hash_key: Any, range_key: Optional[Any] = None) -> bool:
        """
        Verifica se um item existe. Retorna False se não existir.
        """
        return False

    @staticmethod
    @abstractmethod
    def insert(model_instance: T) -> T:
        """
        Insere um novo item e retorna a instância inserida.
        """
        return model_instance

    @staticmethod
    @abstractmethod
    def update(
        model_instance: T,
        hash_key_name: str,
        range_key_name: Optional[str] = None,
        consistent_read: bool = False
    ) -> T:
        """
        Atualiza um item existente e retorna a instância atualizada.
        """
        return model_instance

    @staticmethod
    @abstractmethod
    def upsert(
        model_instance: T,
        hash_key_name: str,
        range_key_name: Optional[str] = None,
        consistent_read: bool = False
    ) -> T:
        """
        Insere ou atualiza um item (upsert) e retorna a instância resultante.
        """
        return model_instance

    @staticmethod
    @abstractmethod
    def delete(model: Type[T], hash_key: Any, range_key: Optional[Any] = None) -> None:
        """
        Remove um item pelo hash_key e range_key.
        """
        pass

    @staticmethod
    @abstractmethod
    def query(
        model_cls: Type[T],
        hash_key_value: Optional[Any] = None,
        range_key_condition: Optional[Condition] = None,
        filter_condition: Optional[Condition] = None,
        limit: Optional[int] = None,
        scan_forward: bool = True,
        use_scan_if_missing_hash: bool = False,
        consistent_read: bool = False
    ) -> Iterator[T]:
        """
        Consulta itens por hash_key e range_key ou scan via condição.
        Retorna iterador vazio se inválido.
        """
        return iter([])

    @staticmethod
    @abstractmethod
    def query_index(
        model_cls: Type[T],
        index_name: str,
        hash_key_value: Optional[Any] = None,
        range_key_condition: Optional[Condition] = None,
        filter_condition: Optional[Condition] = None,
        limit: Optional[int] = None,
        scan_forward: bool = True,
        use_scan_if_missing_hash: bool = False,
        consistent_read: bool = False
    ) -> Iterator[T]:
        """
        Consulta via índice secundário.
        Retorna iterador vazio se inválido.
        """
        return iter([])

    @staticmethod
    @abstractmethod
    def scan(
        model: Type[T],
        filter_condition: Optional[Condition] = None,
        limit: Optional[int] = None,
        consistent_read: bool = False
    ) -> Iterator[T]:
        """
        Escaneia itens com filtro opcional.
        Retorna iterador vazio se nenhum resultado.
        """
        return iter([])

    @staticmethod
    @abstractmethod
    def scan_paginated(
        model: Type[T],
        filter_condition: Optional[Condition] = None,
        page_size: int = 10,
        limit: Optional[int] = None,
        consistent_read: bool = False
    ) -> ResultIterator[T]:
        """
        Escaneia itens paginados.
        Pode retornar ResultIterator vazio ou padrão.
        """
        # Como não tem implementação concreta, pode lançar NotImplementedError,
        # mas conforme pedido, evitamos raise. Podemos retornar iter vazio via cast.
        from pynamodb.pagination import ResultIterator
        # Retornando iterador vazio genérico (cast forçar tipo)
        return iter([])  # type: ignore

    @staticmethod
    @abstractmethod
    def batch_get(
        model: Type[T],
        keys: List[Union[Any, tuple]],
        consistent_read: bool = True
    ) -> Iterator[T]:
        """
        Busca múltiplos itens por lote.
        Retorna iterador vazio se nada encontrado.
        """
        return iter([])

    @staticmethod
    @abstractmethod
    def build_actions(updates: Dict[str, Any]) -> List[SetAction]:
        """
        Gera ações para atualização parcial.
        Retorna lista vazia se nenhum update.
        """
        return []
