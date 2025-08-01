from typing import Type, Any, Optional, List, Dict, Union, Iterator, TypeVar
from pynamodb.exceptions import DoesNotExist
from pynamodb.pagination import ResultIterator
from pynamodb.models import Model as PynamoModel
from pynamodb.expressions.update import SetAction
from pynamodb.expressions.operand import Path
from pynamodb.expressions.condition import Condition

from repository.repository_interface import IDynamoRepository


class DynamoRepository(IDynamoRepository):
    """
    Repositório genérico para operações com DynamoDB utilizando a biblioteca PynamoDB.

    Esta classe oferece métodos estáticos para CRUD, consultas, escaneamentos e operações
    mais avançadas como upsert e batch_get, abstraindo detalhes do acesso direto ao DynamoDB.

    Utiliza generics para suportar qualquer modelo que estenda PynamoModel.

    Exemplo básico de uso:
        customer = CustomerModel(customer_id="C001", tenant_id="T1", ...)
        DynamoRepository.insert(customer)
        fetched = DynamoRepository.get(CustomerModel, "C001", "T1")
    """

    @staticmethod
    def get(model: Type[PynamoModel], hash_key: Any, range_key: Optional[Any] = None) -> Optional[PynamoModel]:
        """
        Obtém um item pelo hash key e opcionalmente range key.

        :param model: Classe do modelo PynamoDB.
        :param hash_key: Valor da chave hash (partition key).
        :param range_key: Valor da chave de range (sort key), se aplicável.
        :return: Instância do modelo se encontrada, None caso contrário.
        """
        if not hash_key:
            # Caso hash_key seja None ou vazio, retorna None diretamente.
            return None
        try:
            return model.get(hash_key, range_key) if range_key else model.get(hash_key)
        except DoesNotExist:
            return None

    @staticmethod
    def exists(model: Type[PynamoModel], hash_key: Any, range_key: Optional[Any] = None) -> bool:
        """
        Verifica se um item existe na tabela.

        :param model: Classe do modelo PynamoDB.
        :param hash_key: Valor da chave hash.
        :param range_key: Valor da chave range (opcional).
        :return: True se o item existir, False caso contrário.
        """
        return DynamoRepository.get(model, hash_key, range_key) is not None

    @staticmethod
    def insert(model_instance: PynamoModel) -> PynamoModel:
        """
        Insere um novo item na tabela.

        :param model_instance: Instância do modelo a ser inserida.
        :return: A própria instância inserida.
        """
        model_instance.save()
        return model_instance

    @staticmethod
    def update(
        model_instance: PynamoModel,
        hash_key_name: str,
        range_key_name: Optional[str] = None,
        consistent_read: bool = False
    ) -> PynamoModel:
        """
        Atualiza um item existente na tabela com os dados da instância fornecida.

        Evita sobrescrever chaves primárias e salva o item.

        :param model_instance: Instância com os dados atualizados.
        :param hash_key_name: Nome do campo de chave hash.
        :param range_key_name: Nome do campo de chave range, se existir.
        :param consistent_read: Se True, faz leitura consistente.
        :return: Instância atualizada salva no DynamoDB.
        """
        model_cls = type(model_instance)
        hash_key = getattr(model_instance, hash_key_name)
        range_key = getattr(model_instance, range_key_name) if range_key_name else None

        # Recupera o item existente
        if range_key is not None:
            existing = model_cls.get(hash_key, range_key, consistent_read=consistent_read)
        else:
            existing = model_cls.get(hash_key, consistent_read=consistent_read)

        # Atualiza os atributos, exceto as chaves primárias
        for attr, value in model_instance.attribute_values.items():
            attr_def = model_cls._get_attributes().get(attr)
            if attr_def and not attr_def.is_hash_key and not attr_def.is_range_key:
                setattr(existing, attr, value)

        existing.save()
        return existing

    @staticmethod
    def upsert(
        model_instance: PynamoModel,
        hash_key_name: str,
        range_key_name: Optional[str] = None,
        consistent_read: bool = False
    ) -> PynamoModel:
        """
        Insere ou atualiza um item (upsert).

        Tenta obter o item; se existir, atualiza, caso contrário insere novo.

        :param model_instance: Instância do modelo.
        :param hash_key_name: Nome da chave hash.
        :param range_key_name: Nome da chave range (opcional).
        :param consistent_read: Leitura consistente se True.
        :return: Instância inserida ou atualizada.
        """
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
    def delete(model: Type[PynamoModel], hash_key: Any, range_key: Optional[Any] = None) -> None:
        """
        Remove um item da tabela pelo hash e range key.

        :param model: Classe do modelo.
        :param hash_key: Valor da chave hash.
        :param range_key: Valor da chave range (opcional).
        """
        if range_key is not None:
            model(hash_key, range_key).delete()
        else:
            model(hash_key).delete()

    @staticmethod
    def query(
        model_cls: Type[PynamoModel],
        hash_key_value: Optional[Any] = None,
        range_key_condition: Optional[Condition] = None,
        filter_condition: Optional[Condition] = None,
        limit: Optional[int] = None,
        scan_forward: bool = True,
        use_scan_if_missing_hash: bool = False,
        consistent_read: bool = False
    ) -> Iterator[PynamoModel]:
        """
        Realiza query por partition key e opcionalmente sort key, ou scan com condição de range key.

        :param model_cls: Classe do modelo.
        :param hash_key_value: Valor da chave hash.
        :param range_key_condition: Condição sobre a sort key.
        :param filter_condition: Filtros adicionais.
        :param limit: Limite de itens retornados.
        :param scan_forward: Ordenação ascendente da sort key.
        :param use_scan_if_missing_hash: Se True, permite scan se hash_key_value não fornecido.
        :param consistent_read: Se True, leitura consistente.
        :raises ValueError: Se parâmetros inválidos para a consulta.
        :return: Iterador dos itens encontrados.
        """
        if hash_key_value is not None:
            return model_cls.query(
                hash_key_value,
                range_key_condition=range_key_condition,
                filter_condition=filter_condition,
                limit=limit,
                scan_index_forward=scan_forward,
                consistent_read=consistent_read
            )
        elif use_scan_if_missing_hash and range_key_condition is not None:
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
        model_cls: Type[PynamoModel],
        index_name: str,
        hash_key_value: Optional[Any] = None,
        range_key_condition: Optional[Condition] = None,
        filter_condition: Optional[Condition] = None,
        limit: Optional[int] = None,
        scan_forward: bool = True,
        use_scan_if_missing_hash: bool = False,
        consistent_read: bool = False
    ) -> Iterator[PynamoModel]:
        """
        Consulta usando índice secundário global ou local.

        :param model_cls: Classe do modelo.
        :param index_name: Nome do índice.
        :param hash_key_value: Valor da chave hash do índice.
        :param range_key_condition: Condição da chave sort do índice.
        :param filter_condition: Filtros adicionais.
        :param limit: Limite de resultados.
        :param scan_forward: Ordenação da chave sort.
        :param use_scan_if_missing_hash: Permite scan se hash_key_value não fornecido.
        :param consistent_read: Leitura consistente.
        :raises ValueError: Se parâmetros inválidos.
        :return: Iterador dos itens encontrados.
        """
        if hash_key_value is not None:
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
        model: Type[PynamoModel],
        filter_condition: Optional[Condition] = None,
        limit: Optional[int] = None,
        consistent_read: bool = False
    ) -> Iterator[PynamoModel]:
        """
        Escaneia toda a tabela, opcionalmente filtrando os resultados.

        :param model: Classe do modelo.
        :param filter_condition: Condição para filtrar resultados.
        :param limit: Limite de itens retornados.
        :param consistent_read: Se True, leitura consistente.
        :return: Iterador dos itens encontrados.
        """
        return model.scan(filter_condition=filter_condition, limit=limit, consistent_read=consistent_read)

    @staticmethod
    def scan_paginated(
        model: Type[PynamoModel],
        filter_condition: Optional[Condition] = None,
        page_size: int = 10,
        limit: Optional[int] = None,
        consistent_read: bool = False
    ) -> ResultIterator[PynamoModel]:
        """
        Escaneia a tabela paginando resultados para controlar memória e latência.

        :param model: Classe do modelo.
        :param filter_condition: Condição para filtrar resultados.
        :param page_size: Tamanho da página (itens por página).
        :param limit: Limite total de itens a retornar.
        :param consistent_read: Leitura consistente.
        :return: ResultIterator para iteração paginada.
        """
        return model.scan(
            filter_condition=filter_condition,
            limit=limit,
            page_size=page_size,
            consistent_read=consistent_read
        )

    @staticmethod
    def batch_get(
        model: Type[PynamoModel],
        keys: List[Union[Any, tuple]],
        consistent_read: bool = True
    ) -> Iterator[PynamoModel]:
        """
        Busca múltiplos itens em lote pelo conjunto de chaves.

        :param model: Classe do modelo.
        :param keys: Lista de chaves (tuplas hash e range, ou apenas hash).
        :param consistent_read: Leitura consistente.
        :return: Iterador com os itens encontrados.
        """
        return model.batch_get(keys, consistent_read=consistent_read)

    @staticmethod
    def build_actions(updates: Dict[str, Any]) -> List[SetAction]:
        """
        Gera uma lista de ações para atualização parcial via update.

        Cada ação corresponde a um atributo e seu novo valor, usando SetAction.

        :param updates: Dicionário atributo -> novo valor.
        :return: Lista de ações para passar em update.
        """
        return [SetAction(Path(attr), value) for attr, value in updates.items()]

    @staticmethod
    def flexible_query(
        model_cls: Type[PynamoModel],
        hash_key_value: Optional[Any] = None,
        range_key_condition: Optional[Condition] = None,
        filter_condition: Optional[Condition] = None,
        limit: Optional[int] = None,
        scan_forward: bool = True,
        consistent_read: bool = False,
        use_scan_if_missing_hash: bool = False,
        index_name: Optional[str] = None
    ) -> Iterator[PynamoModel]:
        """
        Consulta flexível que unifica query normal, query com índice e fallback para scan.

        :param model_cls: Classe do modelo Pynamo.
        :param hash_key_value: Valor da chave de partição.
        :param range_key_condition: Condição de sort key.
        :param filter_condition: Filtro adicional.
        :param limit: Limite de itens.
        :param scan_forward: Ordenação crescente da sort key.
        :param consistent_read: Se a leitura será consistente.
        :param use_scan_if_missing_hash: Permite scan se não houver hash_key.
        :param index_name: Nome do índice, se aplicável.
        :raises ValueError: Parâmetros inválidos.
        :return: Iterador com os resultados.
        """
        kwargs = {
            "range_key_condition": range_key_condition,
            "filter_condition": filter_condition,
            "limit": limit,
            "scan_index_forward": scan_forward,
            "consistent_read": consistent_read,
        }

        if hash_key_value is not None:
            if index_name:
                return model_cls.query(
                    hash_key_value,
                    index_name=index_name,
                    **kwargs
                )
            else:
                return model_cls.query(
                    hash_key_value,
                    **kwargs
                )
        elif use_scan_if_missing_hash and range_key_condition is not None:
            full_filter = range_key_condition & filter_condition if filter_condition else range_key_condition
            scan_kwargs = {
                "filter_condition": full_filter,
                "limit": limit,
                "consistent_read": consistent_read,
            }
            if index_name:
                scan_kwargs["index_name"] = index_name
            return model_cls.scan(**scan_kwargs)

        raise ValueError("hash_key_value é obrigatório, exceto se use_scan_if_missing_hash=True e range_key_condition for fornecido.")