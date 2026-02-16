"""id_order
.pizza_type, size, quantity, is_delivery, special_instructions
"""

from pydantic import BaseModel, Field

class OrderRequest(BaseModel):
    order_id: str
    pizza_type: str
    size: float
    quantity: int
    is_delivered: bool = False
    special_instructions: str

