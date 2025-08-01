from dataclasses import dataclass
from typing import Optional, List, Type
from pynamodb.models import Model
from pynamodb.attributes import Attribute

@dataclass
class IndexKeyMetadata:
    name: str
    hash_key: Optional[str]
    range_key: Optional[str]

@dataclass
class ModelKeyMetadata:
    hash_key: str
    range_key: Optional[str]
    gsis: List[IndexKeyMetadata]
    lsis: List[IndexKeyMetadata]

def extract_keys_metadata(
    model_cls: Type[Model],
    return_attr_name: bool = False
) -> ModelKeyMetadata:
    """
    Extrai metadados de chaves primárias e índices de um modelo PynamoDB.

    :param model_cls: Classe do modelo PynamoDB.
    :param return_attr_name: Se True, retorna o `attr_name` ao invés do nome do atributo Python.
    :return: ModelKeyMetadata com chaves primárias, GSIs e LSIs.
    """
    attributes = model_cls._get_attributes()

    def get_key(attr: Attribute) -> str:
        return attr.attr_name if return_attr_name else attr.attr_name or attr.attr_name

    # Chaves primárias
    hash_key = None
    range_key = None
    for name, attr in attributes.items():
        if attr.is_hash_key:
            hash_key = get_key(attr)
        elif attr.is_range_key:
            range_key = get_key(attr)

    if not hash_key:
        raise ValueError("Hash key não encontrada no modelo.")

    # GSIs
    gsis: List[IndexKeyMetadata] = []
    for index_name, index in model_cls._indexes.items():
        hash_key_name = None
        range_key_name = None

        for name, attr in index.attributes.items():
            if attr.is_hash_key:
                hash_key_name = get_key(attr)
            elif attr.is_range_key:
                range_key_name = get_key(attr)

        gsis.append(IndexKeyMetadata(
            name=index_name,
            hash_key=hash_key_name,
            range_key=range_key_name
        ))

    # LSIs
    lsis: List[IndexKeyMetadata] = []
    for index_name, index in model_cls._local_indexes.items():
        hash_key_name = None
        range_key_name = None

        for name, attr in index.attributes.items():
            if attr.is_hash_key:
                hash_key_name = get_key(attr)
            elif attr.is_range_key:
                range_key_name = get_key(attr)

        lsis.append(IndexKeyMetadata(
            name=index_name,
            hash_key=hash_key_name,
            range_key=range_key_name
        ))

    return ModelKeyMetadata(
        hash_key=hash_key,
        range_key=range_key,
        gsis=gsis,
        lsis=lsis
    )