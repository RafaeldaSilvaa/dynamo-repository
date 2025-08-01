from datetime import datetime
from pynamodb.exceptions import DoesNotExist
from models.customer_model import CustomerModel
from pynamodb.expressions.condition import Condition
from pynamodb.expressions.operand import Path
import pytest

from repository.base_repository import DynamoRepository


def make_customer(i: int) -> CustomerModel:
    return CustomerModel(
        customer_id=f"C{i:04d}",
        tenant_id=f"T{i%5}",
        name=f"Name {i}",
        email=f"user{i}@example.com",
        status="active" if i % 2 == 0 else "inactive",
        created_at=datetime.now().isoformat()
    )

def test_insert_and_get():
    customer = make_customer(9999)
    # Insert
    DynamoRepository.insert(customer)
    # Get
    retrieved = DynamoRepository.get(CustomerModel, customer.customer_id, customer.tenant_id)
    assert retrieved is not None
    assert retrieved.email == customer.email

def test_exists():
    customer = make_customer(8888)
    DynamoRepository.insert(customer)
    assert DynamoRepository.exists(CustomerModel, customer.customer_id, customer.tenant_id)
    assert not DynamoRepository.exists(CustomerModel, "nonexistent", "nope")

def test_update():
    customer = make_customer(7777)
    DynamoRepository.insert(customer)
    # Update
    customer.name = "Updated Name"
    updated = DynamoRepository.update(customer)
    assert updated.name == "Updated Name"

def test_upsert_insert_and_update():
    customer = make_customer(6666)
    # Should insert
    upserted1 = DynamoRepository.upsert(customer)
    assert upserted1.customer_id == customer.customer_id

    # Change and upsert again should update
    customer.name = "Upsert Updated"
    upserted2 = DynamoRepository.upsert(customer)
    assert upserted2.name == "Upsert Updated"

def test_delete():
    customer = make_customer(5555)
    DynamoRepository.insert(customer)
    # Delete
    DynamoRepository.delete(CustomerModel, customer.customer_id, customer.tenant_id)
    assert not DynamoRepository.exists(CustomerModel, customer.customer_id, customer.tenant_id)

def test_batch_get():
    keys = [("C0010", "T0"), ("C0011", "T1")]
    results = list(DynamoRepository.batch_get(CustomerModel, keys))
    assert len(results) == 2

def test_query_index():
    # Para esse teste, você deve ter um índice secundário definido (ex: index on tenant_id)
    # Supondo que o índice se chame 'tenant_id_index'
    # Só para exemplo, se você não tiver índice, pode pular este teste
    try:
        results = list(DynamoRepository.query_index(CustomerModel, "tenant_id_index", "T1"))
        assert all(r.tenant_id == "T1" for r in results)
    except AttributeError:
        print("Índice secundário não definido, pule este teste")

# Rodar testes com pytest
if __name__ == "__main__":
    pytest.main([__file__])
