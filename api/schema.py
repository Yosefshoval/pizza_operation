from pydantic import BaseModel, Field

class OrderRequest(BaseModel):
    order_id: str
    pizza_type: str
    size: str
    quantity: int
    is_delivered: bool = False
    special_instructions: str

