# models/customer_model.py

from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute
from pynamodb.indexes import GlobalSecondaryIndex, AllProjection


class CustomerByEmailIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = "email-index"
        projection = AllProjection()
        read_capacity_units = 1
        write_capacity_units = 1

    email = UnicodeAttribute(hash_key=True)


class CustomerByStatusIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = "status-index"
        projection = AllProjection()
        read_capacity_units = 1
        write_capacity_units = 1

    status = UnicodeAttribute(hash_key=True)
    created_at = UnicodeAttribute(range_key=True)


class CustomerModel(Model):
    class Meta:
        table_name = "customers"
        region = "us-east-1"
        host = "http://localhost:4566"
        read_capacity_units = 1
        write_capacity_units = 1

    customer_id = UnicodeAttribute(hash_key=True)
    tenant_id = UnicodeAttribute(range_key=True)

    name = UnicodeAttribute()
    email = UnicodeAttribute()
    status = UnicodeAttribute()
    created_at = UnicodeAttribute()

    email_index = CustomerByEmailIndex()
    status_index = CustomerByStatusIndex()
