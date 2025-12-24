from feast import FeatureView, Field
from feast.types import Int64, Float32
from feast.infra.offline_stores.file_source import FileSource
from datetime import timedelta

from entities import user

transaction_features_source = FileSource(
    path="data/transaction_features",
    timestamp_field="event_timestamp",
)

transaction_features = FeatureView(
    name="transaction_features",
    entities=[user],
    ttl=timedelta(minutes=10),
    schema=[
        Field(name="txn_count_5min", dtype=Int64),
        Field(name="avg_amount_5min", dtype=Float32),
    ],
    online=True,
    source=transaction_features_source,
)