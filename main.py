from datetime import datetime
from pynamodb.expressions.operand import Path, Value
from pynamodb.expressions.condition import Between, BeginsWith, In, And

from data.seed_customers import seed_customers
from models.customer_model import CustomerModel
from repository.base_repository import DynamoRepository


def create_customer(i: int) -> CustomerModel:
    return CustomerModel(
        customer_id=f"C{i:04d}",
        tenant_id=f"T{i % 3}",
        name=f"User {i}",
        email=f"user{i}@example.com",
        status="active" if i % 2 == 0 else "inactive",
        created_at=datetime.now().isoformat()
    )


def examples():
    print("1. Insert:")
    customer = create_customer(1)
    DynamoRepository.insert(customer)

    print("\n2. Get:")
    got = DynamoRepository.get(CustomerModel, customer.customer_id, customer.tenant_id)
    print(" - Got:", got.name if got else "Not found")

    print("\n3. Exists:")
    print(" - Exists?", DynamoRepository.exists(CustomerModel, customer.customer_id, customer.tenant_id))

    print("\n4. Update:")
    customer.name = "Updated Name"
    updated = DynamoRepository.update(customer, hash_key_name="customer_id", range_key_name="tenant_id")
    print(" - Updated name:", updated.name)

    print("\n5. Upsert (update existing):")
    customer.email = "newemail@example.com"
    upserted = DynamoRepository.upsert(customer, hash_key_name="customer_id", range_key_name="tenant_id")
    print(" - Upserted email:", upserted.email)

    print("\n6. Upsert (insert new):")
    new_customer = create_customer(2)
    upserted_new = DynamoRepository.upsert(new_customer, hash_key_name="customer_id", range_key_name="tenant_id")
    print(" - Inserted new customer:", upserted_new.customer_id)

    print("\n7. Delete:")
    DynamoRepository.delete(CustomerModel, new_customer.customer_id, new_customer.tenant_id)
    print(" - Exists after delete?", DynamoRepository.exists(CustomerModel, new_customer.customer_id, new_customer.tenant_id))

    print("\n8. Query by hash only:")
    for item in DynamoRepository.query(CustomerModel, customer.customer_id):
        print(f" - {item.customer_id} | {item.tenant_id}")

    print("\n9. Query with range condition (tenant_id begins with 'T'):")
    cond = BeginsWith(Path("tenant_id"), Value("T"))
    for item in DynamoRepository.query(CustomerModel, customer.customer_id, range_key_condition=cond):
        print(f" - {item.customer_id} | {item.tenant_id}")

    print("\n10. Query index (tenant_id_index) by hash only:")
    customer = create_customer(6)
    DynamoRepository.insert(customer)
    results = list(
        DynamoRepository.query(model_cls=CustomerModel, hash_key_value=customer.customer_id, consistent_read=True))
    for result in results:
        print(f" - {result.customer_id} | {result.tenant_id}")

    print("\n11. Query index (created_at_index) with range condition:")
    try:
        customer = create_customer(8)
        DynamoRepository.insert(customer)
        cond = BeginsWith(Path("tenant_id"), Value("T"))
        results = list(DynamoRepository.query(
            CustomerModel,
            hash_key_value=None,
            range_key_condition=cond,
            use_scan_if_missing_hash=True
        ))
        for result in results:
            print(f" - {result.customer_id} | {result.tenant_id}")
    except Exception as e:
        print(f" - Index query failed: {e}")

    print("\n12. Scan with filter (status='inactive'):")
    customer = create_customer(13)
    customer.status = "inactive"
    DynamoRepository.insert(customer)

    results = list(DynamoRepository.scan(CustomerModel, filter_condition=CustomerModel.status == "inactive"))
    for result in results:
        print(f" - {result.customer_id} | {result.tenant_id} | {result.status}")

    print("\n13. Scan paginated (limit 5, page size 2):")
    for item in DynamoRepository.scan_paginated(CustomerModel, page_size=2, limit=5):
        print(f" - {item.customer_id} | {item.name}")

    print("\n14. Batch get:")
    keys = [(customer.customer_id, customer.tenant_id), ("C0002", "T1"), ("C0003", "T2")]
    for item in DynamoRepository.batch_get(CustomerModel, keys):
        print(f" - {item.customer_id} | {item.tenant_id}")

    print("\n15. Insert duplicate (expect error or overwrite):")
    try:
        DynamoRepository.insert(customer)
        print(" - Inserted duplicate without error")
    except Exception as e:
        print(f" - Expected error: {e}")

    print("\n16. Update with non-existent field (should ignore or error):")
    try:
        setattr(customer, "non_existent_field", "value")
        updated = DynamoRepository.update(customer, hash_key_name="customer_id", range_key_name="tenant_id")
        print(" - Updated with non-existent field")
    except Exception as e:
        print(f" - Expected error: {e}")

    print("\n17. Upsert with undeclared attribute:")
    customer.extra_field = "value"
    try:
        upserted = DynamoRepository.upsert(customer, hash_key_name="customer_id", range_key_name="tenant_id")
        print(" - Upserted with extra field (ignored)")
    except Exception as e:
        print(f" - Unexpected failure: {e}")

    print("\n19. Query with sort key between values:")
    range_cond = Between(Path("tenant_id"), Value("T0"), Value("T2"))
    for item in DynamoRepository.query(CustomerModel, customer.customer_id, range_key_condition=range_cond):
        print(f" - {item.customer_id} | {item.tenant_id}")

    print("\n20. Get with invalid keys (expect None):")
    print(" - None key:", DynamoRepository.get(CustomerModel, None, None))
    print(" - Empty string key:", DynamoRepository.get(CustomerModel, "", ""))

    print("\n21. Delete and confirm not found:")
    DynamoRepository.delete(CustomerModel, customer.customer_id, customer.tenant_id)
    after_delete = DynamoRepository.get(CustomerModel, customer.customer_id, customer.tenant_id)
    print(" - After delete:", "Not found" if after_delete is None else "Found")


if __name__ == "__main__":
    seed_customers()  # Popula tabela com dados fake, ajuste conforme sua implementação
    examples()
