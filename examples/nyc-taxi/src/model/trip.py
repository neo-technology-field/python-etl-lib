from pydantic import BaseModel, Field
from datetime import datetime

class Trip(BaseModel):
    vendor_id: int = Field(alias="VendorID")
    pickup_datetime: datetime = Field(alias="tpep_pickup_datetime")
    dropoff_datetime: datetime = Field(alias="tpep_dropoff_datetime")
    passenger_count: int = Field(alias="passenger_count")
    trip_distance: float = Field(alias="trip_distance")
    pu_location: int = Field(alias="PULocationID")
    do_location: int = Field(alias="DOLocationID")
    total_amount: float = Field(alias="total_amount")
