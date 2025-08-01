from datetime import datetime
from typing import List
from faker import Faker
from models.customer_model import CustomerModel

fake = Faker()


def seed_customers(n: int = 10) -> None:
    """
    Cria a tabela se não existir e insere n clientes gerados com Faker.
    Retorna a lista de instâncias inseridas.
    """
    if not CustomerModel.exists():
        CustomerModel.create_table(
            read_capacity_units=1,
            write_capacity_units=1,
            wait=True
        )

        customers = []
        for i in range(n):
            customer = CustomerModel(
                customer_id=f"C{str(i+1).zfill(4)}",
                tenant_id=f"T{fake.random_int(min=1, max=20)}",
                name=fake.name(),
                email=fake.email(),
                status=fake.random_element(elements=["active", "inactive"]),
                created_at=datetime.now().isoformat()
            )
            customer.save()
            customers.append(customer)