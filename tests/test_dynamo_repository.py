import pytest
from datetime import datetime, timezone
from pynamodb import *
from moto import mock_aws
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, UTCDateTimeAttribute
from pynamodb.indexes import GlobalSecondaryIndex, AllProjection
from pynamodb.exceptions import DoesNotExist
from pynamodb.expressions.condition import Between, BeginsWith
from pynamodb.expressions.operand import Path, Value
from pynamodb.expressions.condition import Condition

from repository.base_repository import DynamoRepository

# --- Modelo de Teste ---
class TenantIdIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = "tenant_id_index"
        projection = AllProjection()
        read_capacity_units = 1
        write_capacity_units = 1

    tenant_id = UnicodeAttribute(hash_key=True)


class CreatedAtIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = "created_at_index"
        projection = AllProjection()
        read_capacity_units = 1
        write_capacity_units = 1

    tenant_id = UnicodeAttribute(hash_key=True)
    created_at = UTCDateTimeAttribute(range_key=True)


class CustomerModel(Model):
    class Meta:
        table_name = "customers"
        region = "us-east-1"
        #host = "http://localhost:4566"  # LocalStack or moto endpoint

    customer_id = UnicodeAttribute(hash_key=True)
    tenant_id = UnicodeAttribute(range_key=True)
    name = UnicodeAttribute()
    email = UnicodeAttribute()
    status = UnicodeAttribute()
    created_at = UTCDateTimeAttribute()

    tenant_id_index = TenantIdIndex()
    created_at_index = CreatedAtIndex()

@pytest.fixture(autouse=True)
def setup_dynamodb():
    with mock_aws():
        if not CustomerModel.exists():
            CustomerModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)
        yield


def create_customer(i: int) -> CustomerModel:
    return CustomerModel(
        customer_id=f"C{i:04d}",
        tenant_id=f"T{i % 3}",
        name=f"User {i}",
        email=f"user{i}@example.com",
        status="active" if i % 2 == 0 else "inactive",
        created_at=datetime.now(timezone.utc)
    )


def test_insert_and_get():
    customer = create_customer(1)
    DynamoRepository.insert(customer)

    result = DynamoRepository.get(model=CustomerModel, hash_key=customer.customer_id, range_key=customer.tenant_id)
    assert result is not None
    assert result.name == customer.name


def test_exists():
    customer = create_customer(2)
    DynamoRepository.insert(customer)
    assert DynamoRepository.exists(model=CustomerModel, hash_key=customer.customer_id, range_key=customer.tenant_id)


def test_update():
    customer = create_customer(3)
    DynamoRepository.insert(customer)
    customer.name = "Updated"
    updated = DynamoRepository.update(model_instance=customer, hash_key_name="customer_id", range_key_name="tenant_id", consistent_read=True)
    assert updated.name == "Updated"


def test_upsert_insert_and_update():
    customer = create_customer(4)
    upserted = DynamoRepository.upsert(model_instance=customer, hash_key_name="customer_id", range_key_name="tenant_id", consistent_read=True)
    assert upserted.customer_id == customer.customer_id

    customer.name = "New Name"
    upserted2 = DynamoRepository.upsert(model_instance=customer, hash_key_name="customer_id", range_key_name="tenant_id", consistent_read=True)
    assert upserted2.name == "New Name"


def test_delete():
    customer = create_customer(5)
    DynamoRepository.insert(customer)
    DynamoRepository.delete(model=CustomerModel, hash_key=customer.customer_id, range_key=customer.tenant_id)
    assert not DynamoRepository.exists(model=CustomerModel, hash_key=customer.customer_id, range_key=customer.tenant_id)


def test_query_by_hash_only():
    customer = create_customer(6)
    DynamoRepository.insert(customer)
    results = list(DynamoRepository.query(model_cls=CustomerModel, hash_key_value=customer.customer_id, consistent_read=True))
    assert any(r.tenant_id == customer.tenant_id for r in results)

def test_query_with_range_condition():
    customer = create_customer(7)
    DynamoRepository.insert(customer)
    condition = BeginsWith(Path("tenant_id"), Value("T"))
    results = list(
        DynamoRepository.query(
            model_cls=CustomerModel,
            hash_key_value=customer.customer_id,
            range_key_condition=condition
        )
    )
    assert any(r.customer_id == customer.customer_id for r in results)


def test_query_with_only_range_condition():
    customer = create_customer(8)
    DynamoRepository.insert(customer)
    cond = BeginsWith(Path("tenant_id"), Value("T"))
    results = list(DynamoRepository.query(
        CustomerModel,
        hash_key_value=None,
        range_key_condition=cond,
        use_scan_if_missing_hash=True
    ))
    assert any(r.customer_id == customer.customer_id for r in results)


def test_query_index_hash_only():
    customer = create_customer(9)
    DynamoRepository.insert(customer)
    results = list(DynamoRepository.query_index(
        CustomerModel,
        "tenant_id_index",
        hash_key_value=customer.tenant_id
    ))
    assert any(r.customer_id == customer.customer_id for r in results)


def test_query_index_with_range():
    customer = create_customer(10)
    DynamoRepository.insert(customer)
    cond = Between(Path("created_at"), Value("2020-01-01"), Value("2030-01-01"))
    results = list(DynamoRepository.query_index(
        CustomerModel,
        "created_at_index",
        hash_key_value=customer.tenant_id,
        range_key_condition=cond
    ))
    assert any(r.customer_id == customer.customer_id for r in results)


def test_query_index_with_only_range_scan():
    customer = create_customer(11)
    DynamoRepository.insert(customer)
    cond = BeginsWith(Path("created_at"), Value("20"))
    results = list(DynamoRepository.query_index(
        CustomerModel,
        "created_at_index",
        hash_key_value=None,
        range_key_condition=cond,
        use_scan_if_missing_hash=True
    ))
    assert any(r.customer_id == customer.customer_id for r in results)


def test_scan_all():
    customer = create_customer(12)
    DynamoRepository.insert(customer)
    results = list(DynamoRepository.scan(CustomerModel))
    assert any(r.customer_id == customer.customer_id for r in results)


def test_scan_with_filter():
    customer = create_customer(13)
    customer.status = "inactive"
    DynamoRepository.insert(customer)

    results = list(DynamoRepository.scan(CustomerModel, filter_condition=CustomerModel.status == "inactive"))
    assert any(r.customer_id == customer.customer_id for r in results)

def test_scan_paginated():
    for i in range(14, 20):
        DynamoRepository.insert(create_customer(i))
    results = list(DynamoRepository.scan_paginated(CustomerModel, page_size=2, limit=5))
    assert len(results) == 5


def test_batch_get():
    customers = [create_customer(i) for i in range(21, 24)]
    for c in customers:
        DynamoRepository.insert(c)
    keys = [(c.customer_id, c.tenant_id) for c in customers]
    results = list(DynamoRepository.batch_get(CustomerModel, keys))
    assert len(results) == 3


def test_query_invalid_without_hash_and_flag():
    with pytest.raises(ValueError):
        list(DynamoRepository.query(CustomerModel))


def test_query_index_invalid_without_hash_and_flag():
    with pytest.raises(ValueError):
        list(DynamoRepository.query_index(CustomerModel, "tenant_id_index"))


def test_build_actions():
    updates = {"name": "Test", "email": "t@example.com"}
    actions = DynamoRepository.build_actions(updates)
    assert len(actions) == 2

def test_query_with_begins_with():
    customer = create_customer(100)
    DynamoRepository.insert(customer)

    cond = BeginsWith(Path("tenant_id"), Value("T"))
    results = list(DynamoRepository.query(CustomerModel, customer.customer_id, range_key_condition=cond))

    assert len(results) >= 1
    assert any(r.tenant_id.startswith("T") for r in results)


def test_query_with_between():
    customer = create_customer(101)
    DynamoRepository.insert(customer)

    cond = Between(Path("tenant_id"), Value("T0"), Value("T9"))
    results = list(DynamoRepository.query(CustomerModel, customer.customer_id, range_key_condition=cond))

    assert any(r.tenant_id == customer.tenant_id for r in results)


def test_update_only_selected_fields():
    customer = create_customer(102)
    DynamoRepository.insert(customer)

    customer.name = "Partial Update"
    updated = DynamoRepository.update(
        customer,
        hash_key_name="customer_id",
        range_key_name="tenant_id"
    )

    assert updated.name == "Partial Update"
    assert updated.email == customer.email


def test_delete_and_cannot_get():
    customer = create_customer(103)
    DynamoRepository.insert(customer)

    DynamoRepository.delete(CustomerModel, customer.customer_id, customer.tenant_id)

    with pytest.raises(DoesNotExist):
        CustomerModel.get(customer.customer_id, customer.tenant_id)


def test_upsert_creates_new_when_not_exists():
    customer = create_customer(104)
    result = DynamoRepository.upsert(
        customer,
        hash_key_name="customer_id",
        range_key_name="tenant_id"
    )

    assert result.customer_id == customer.customer_id
    assert result.email == customer.email


def test_batch_get_with_missing_keys():
    c1 = create_customer(105)
    c2 = create_customer(106)
    DynamoRepository.insert(c1)
    DynamoRepository.insert(c2)

    keys = [(c1.customer_id, c1.tenant_id), ("NOTFOUND", "T0")]
    results = list(DynamoRepository.batch_get(CustomerModel, keys))

    assert len(results) == 1
    assert results[0].customer_id == c1.customer_id

def test_get_with_invalid_keys():
    result = DynamoRepository.get(CustomerModel, hash_key=None, range_key=None)
    assert result is None

def test_update_nonexistent_raises():
    customer = create_customer(200)
    with pytest.raises(DoesNotExist):
        DynamoRepository.update(customer, "customer_id", "tenant_id")

def test_delete_nonexistent_does_not_fail():
    # Deve simplesmente não lançar erro
    DynamoRepository.delete(CustomerModel, "NOEXIST", "NOEXIST")

def test_scan_with_complex_filter():
    cond = (CustomerModel.status == "active") & (CustomerModel.name.startswith("User"))
    results = list(DynamoRepository.scan(CustomerModel, filter_condition=cond))
    assert all(r.status == "active" for r in results)

def test_query_with_limit():
    for i in range(30, 40):
        DynamoRepository.insert(create_customer(i))
    results = list(DynamoRepository.query(CustomerModel, hash_key_value="C0030", limit=1))
    assert len(results) <= 1

def test_query_index_with_invalid_index():
    customer = create_customer(50)
    DynamoRepository.insert(customer)
    with pytest.raises(KeyError):
        list(DynamoRepository.query_index(CustomerModel, "invalid_index", hash_key_value=customer.tenant_id))

def test_batch_get_empty_keys():
    results = list(DynamoRepository.batch_get(CustomerModel, []))
    assert results == []

def test_build_actions_empty():
    actions = DynamoRepository.build_actions({})
    assert actions == []

def test_get_with_invalid_keys():
    # Passar None ou string vazia
    assert DynamoRepository.get(CustomerModel, hash_key=None, range_key=None) is None
    assert DynamoRepository.get(CustomerModel, hash_key="", range_key=None) is None