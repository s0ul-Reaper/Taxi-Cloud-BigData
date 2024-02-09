import asyncio
import os
from dotenv import load_dotenv

from azure.storage.blob import BlobServiceClient
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import (
    BlobCheckpointStore,
)

load_dotenv()
BLOB_STORAGE_CONNECTION_STRING = os.getenv('BLOB_STORAGE_CONNECTION_STRING')
BLOB_CONTAINER_NAME = os.getenv('BLOB_CONTAINER_NAME')

EVENT_HUB_CONNECTION_STR = os.getenv('EVENT_HUB_CONNECTION_STR')
EVENT_HUB_NAME = os.getenv('EVENT_HUB_NAME')

async def on_event(partition_context, event):
    # Print the event data.
    print(
        'Received the event: "{}"'.format(
        event.body_as_str(encoding="UTF-8"))
    )

    # Write the event data as a JSON file to Blob Storage
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)

    
    blobname = f"events/event{event.sequence_number}.json"

    blob_client = container_client.get_blob_client(blobname)
    blob_client.upload_blob(event.body_as_str(encoding="UTF-8"), overwrite=True)

    # Update the checkpoint so that the program doesn't read the events
    # that it has already read when you run it next time.
    await partition_context.update_checkpoint(event)

async def main():
    # Create an Azure blob checkpoint store to store the checkpoints.
    checkpoint_store = BlobCheckpointStore.from_connection_string(
        BLOB_STORAGE_CONNECTION_STRING, BLOB_CONTAINER_NAME
    )

    # Create a consumer client for the event hub.
    client = EventHubConsumerClient.from_connection_string(
        EVENT_HUB_CONNECTION_STR,
        consumer_group="$Default",
        eventhub_name=EVENT_HUB_NAME,
        checkpoint_store=checkpoint_store
    )
    async with client:
        # Call the receive method. Read from the beginning of the
        await client.receive(on_event=on_event, starting_position="-1")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    # Run the main method.
    loop.run_until_complete(main())