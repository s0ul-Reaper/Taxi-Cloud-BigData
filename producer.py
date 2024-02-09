import asyncio
import os

from dotenv import load_dotenv
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

from pyspark.sql import SparkSession
from pyspark.sql.functions import  monotonically_increasing_id, col

load_dotenv()

EVENT_HUB_FULLY_QUALIFIED_NAMESPACE = os.getenv('EVENT_HUB_FULLY_QUALIFIED_NAMESPACE')
EVENT_HUB_NAME = os.getenv('EVENT_HUB_NAME')

CONNECTION_STRING = os.getenv('CONNECTION_STRING')

spark = SparkSession.builder.appName("StreamTaxi").getOrCreate()

df = spark.read.option('header', 'true').csv('./data/train.csv')
df = df.withColumn("row_id", monotonically_increasing_id())

async def run():
    r = 0 

    while True:
        await asyncio.sleep(5)
        producer = EventHubProducerClient.from_connection_string(conn_str = CONNECTION_STRING, eventhub_name = EVENT_HUB_NAME)

        async with producer:
            # Create a batch.
            event_data_batch = await producer.create_batch()
            json_string = df.filter((col("row_id") >= r) & (col("row_id") < r+1)).toJSON().collect()
            # Add events to the batch.
            event_data_batch.add(EventData(json_string))
            # Send the batch of events to the event hub.
            await producer.send_batch(event_data_batch)
            r +=1

loop = asyncio.get_event_loop()
try:
    asyncio.ensure_future(run())
    loop.run_forever()
except KeyboardInterrupt:
    pass
finally:
    print('ClosingLoop')
    loop.close()
