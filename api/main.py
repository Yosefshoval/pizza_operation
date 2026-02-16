from fastapi import FastAPI, UploadFile
from pydantic import ValidationError
from schema import OrderRequest
import json


app = FastAPI()

@app.post('/upload_file')
def post_orders(file: UploadFile):
    orders = json.load(file)
    for order in orders:
        try:
            OrderRequest(**order)
        except ValidationError as err:
            print(err)
            continue



@app.get('/order/{order_id}')
def get_order(order_id: str):
    pass


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app='main:app',
        host='0.0.0.0',
        port=8080,
        reload=True
    )
