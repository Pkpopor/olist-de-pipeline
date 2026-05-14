from dataclasses import dataclass, field
from typing import List


@dataclass
class ColumnContract:
    name: str
    dtype: str   # 'str' | 'int' | 'float' | 'datetime'
    nullable: bool = True


@dataclass
class TableContract:
    source_file: str
    target_table: str
    primary_key: List[str]
    columns: List[ColumnContract]
    pii_fields: List[str] = field(default_factory=list)


TABLE_CONTRACTS: List[TableContract] = [
    TableContract(
        source_file="olist_orders_dataset.csv",
        target_table="orders",
        primary_key=["order_id"],
        columns=[
            ColumnContract("order_id",                      "str",      nullable=False),
            ColumnContract("customer_id",                   "str",      nullable=False),
            ColumnContract("order_status",                  "str"),
            ColumnContract("order_purchase_timestamp",      "datetime"),
            ColumnContract("order_approved_at",             "datetime"),
            ColumnContract("order_delivered_carrier_date",  "datetime"),
            ColumnContract("order_delivered_customer_date", "datetime"),
            ColumnContract("order_estimated_delivery_date", "datetime"),
        ],
    ),
    TableContract(
        source_file="olist_order_items_dataset.csv",
        target_table="order_items",
        primary_key=["order_id", "order_item_id"],
        columns=[
            ColumnContract("order_id",            "str",   nullable=False),
            ColumnContract("order_item_id",       "int",   nullable=False),
            ColumnContract("product_id",          "str"),
            ColumnContract("seller_id",           "str"),
            ColumnContract("shipping_limit_date", "datetime"),
            ColumnContract("price",               "float"),
            ColumnContract("freight_value",       "float"),
        ],
    ),
    TableContract(
        source_file="olist_customers_dataset.csv",
        target_table="customers",
        primary_key=["customer_id"],
        pii_fields=["customer_zip_code_prefix"],
        columns=[
            ColumnContract("customer_id",              "str", nullable=False),
            ColumnContract("customer_unique_id",       "str"),
            ColumnContract("customer_zip_code_prefix", "str"),
            ColumnContract("customer_city",            "str"),
            ColumnContract("customer_state",           "str"),
        ],
    ),
    TableContract(
        source_file="olist_products_dataset.csv",
        target_table="products",
        primary_key=["product_id"],
        columns=[
            ColumnContract("product_id",                  "str",   nullable=False),
            ColumnContract("product_category_name",       "str"),
            ColumnContract("product_name_lenght",         "int"),
            ColumnContract("product_description_lenght",  "int"),
            ColumnContract("product_photos_qty",          "int"),
            ColumnContract("product_weight_g",            "float"),
            ColumnContract("product_length_cm",           "float"),
            ColumnContract("product_height_cm",           "float"),
            ColumnContract("product_width_cm",            "float"),
        ],
    ),
    TableContract(
        source_file="olist_sellers_dataset.csv",
        target_table="sellers",
        primary_key=["seller_id"],
        pii_fields=["seller_zip_code_prefix"],
        columns=[
            ColumnContract("seller_id",              "str", nullable=False),
            ColumnContract("seller_zip_code_prefix", "str"),
            ColumnContract("seller_city",            "str"),
            ColumnContract("seller_state",           "str"),
        ],
    ),
    TableContract(
        source_file="olist_order_payments_dataset.csv",
        target_table="order_payments",
        primary_key=["order_id", "payment_sequential"],
        columns=[
            ColumnContract("order_id",             "str",   nullable=False),
            ColumnContract("payment_sequential",   "int",   nullable=False),
            ColumnContract("payment_type",         "str"),
            ColumnContract("payment_installments", "int"),
            ColumnContract("payment_value",        "float"),
        ],
    ),
    TableContract(
        source_file="olist_order_reviews_dataset.csv",
        target_table="order_reviews",
        primary_key=["review_id"],
        columns=[
            ColumnContract("review_id",               "str", nullable=False),
            ColumnContract("order_id",                "str", nullable=False),
            ColumnContract("review_score",            "int"),
            ColumnContract("review_comment_title",    "str"),
            ColumnContract("review_comment_message",  "str"),
            ColumnContract("review_creation_date",    "datetime"),
            ColumnContract("review_answer_timestamp", "datetime"),
        ],
    ),
    TableContract(
        source_file="olist_geolocation_dataset.csv",
        target_table="geolocation",
        primary_key=["geolocation_zip_code_prefix"],
        columns=[
            ColumnContract("geolocation_zip_code_prefix", "str",   nullable=False),
            ColumnContract("geolocation_lat",             "float"),
            ColumnContract("geolocation_lng",             "float"),
            ColumnContract("geolocation_city",            "str"),
            ColumnContract("geolocation_state",           "str"),
        ],
    ),
    TableContract(
        source_file="product_category_name_translation.csv",
        target_table="product_category_name_translation",
        primary_key=["product_category_name"],
        columns=[
            ColumnContract("product_category_name",         "str", nullable=False),
            ColumnContract("product_category_name_english", "str"),
        ],
    ),
]
