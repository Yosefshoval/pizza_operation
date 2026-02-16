from fastapi import FastAPI, UploadFile, HTTPException
from pydantic import ValidationError
from schema import OrderRequest
from mongo_connection import DBConnection
from kafka_producer import publish_message, KafkaException
from redis_producer import *
import json

mongo_client = DBConnection()

app = FastAPI()


@app.post('/upload_file')
def post_orders(file: UploadFile):
    orders = json.load(file.file)
    ids = []
    for order in orders:
        try:
            OrderRequest(**order)

            # 1: save in mongodb
            order['status'] = 'PREPARING'
            inserted_id = mongo_client.save_order(order)
            ids.append(str(inserted_id))
            print(f'order {inserted_id} inserted to mongodb')

            # 2: publish to kafka
            publish_message(order)
            print(f'order {order["order_id"]} pushed to kafka')

        except ValidationError as err:
            print(err)
            continue
        except KafkaException as err:
            raise HTTPException(status_code=500, detail=str(err))

    return {
        'message' : 'Your orders saved in MongoDB, pushed to Kafka for further treatment.',
        "ids": ids
    }



@app.get('/order/{order_id}')
def retrieve_order(order_id: str):
    try:
        # try to retrieve the order from redis chach
        order = get_order(order_id=order_id)
        print(order)
        if not order:
            print('not order')
            order = mongo_client.get_order_by_id(order_id)
            order = order.to_list()
            print(order)
            cache_order(order[0], 60)
            return order[0]
        return json.loads(order)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app='main:app',
        host='0.0.0.0',
        port=8001,
        reload=True
    )
