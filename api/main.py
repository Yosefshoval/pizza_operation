from fastapi import FastAPI, UploadFile, HTTPException
from pydantic import ValidationError
from schema import OrderRequest
from mongo_connection import DBConnection
from kafka_producer import publish_message, KafkaException
from redis_producer import *
import json


ttl = 60
mongo_client = DBConnection()

app = FastAPI()
try:
    publish_message({
        'message': 'init topic message'
    })
except (KeyError, KafkaException) as e:
    print(e)


@app.post('/upload_file')
def post_orders(file: UploadFile):
    orders = json.load(file.file)
    ids = []
    exist_orders = 0
    for order in orders:
        try:
            OrderRequest(**order)

            # 1: save in mongodb
            order['status'] = 'PREPARING'
            inserted_id = mongo_client.save_order(order)
            if inserted_id is None:
                print('order already exist or something else was wrong')
                exist_orders += 1
                continue
                # return {'message': 'order already exist or something else was wrong'}

            ids.append(str(inserted_id))
            print(f'order {inserted_id} inserted to mongodb')

            # 2: publish to kafka
            print('before pushing: ', order)
            order['_id'] = inserted_id
            publish_message(order)
            print(f'order {order["order_id"]} pushed to kafka')

            # 3 cache in redis for 60 sec
            cache_order(order, ttl)

        except ValidationError as err:
            print(err)
            continue
        except KafkaException as err:
            raise HTTPException(status_code=500, detail=str(err))

    return {
        'message' : 'Your orders saved in MongoDB, pushed to Kafka for further treatment.',
        'orders that already exist in mongodb' : exist_orders,
        "ids": ids
    }



@app.get('/order/{order_id}')
def retrieve_order(order_id: str):
    try:
        order = get_order(order_id=order_id)
        if not order:
            print('not order')
            order = mongo_client.get_order_by_id(order_id)

            if not order:
                raise HTTPException(status_code=404, detail=f'Error: order {order_id} not found')

            cache_order(order, ttl)
            return {'source':'from mongodb', 'order': order}
        else:
            dict_order = json.loads(order)
            cache_order(dict_order, ttl)
        return {'source':'from redis', 'order': dict_order}

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app='main:app',
        host='0.0.0.0',
        port=8001,
        reload=True
    )
