"""
Create and listen to Azure Service Bus.
"""

import time
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.servicebus.exceptions import (
    MessageAlreadySettled,
    OperationTimeoutError,
    ServiceBusConnectionError,
    ServiceBusError,
)
import json
import os
import dotenv


def get_json(unknown_object: str | bytes) -> dict | bool:
    try:
        json_object = json.loads(unknown_object)
        return json_object
    except ValueError:
        return False


# Load environment variables
dotenv.load_dotenv()

""""""
# Get Azure Bus connection string
# WARN: Will use passwordless method in production, However instead of forcing you to
# download azure cli binary and login to azure, get correct perms etc etc.
# For now Bypass all of that and use a connection string.
SERVICE_BUS_CON_STRING = os.environ.get("SERVICE_BUS_CONNECTION_STRING", "")
""""""
TOPIC_NAME = os.environ.get("TOPIC_NAME", "")
TOPIC_NAME_RESULT = os.environ.get("TOPIC_NAME_RESULT", "")
SUBSCRIPTION_NAME = os.environ.get("SUBSCRIPTION_NAME", "")

# Create Service Bus Client
client: ServiceBusClient


def connect() -> ServiceBusClient | None:
    """
    Connects and handles all errors.
    """
    try:
        client_to_use = ServiceBusClient.from_connection_string(
            SERVICE_BUS_CON_STRING
        )
        return client_to_use
    except ServiceBusConnectionError as err:
        print("Service Bus Connection Error: ", err)
        return None


# Add a subscription to the topic
def subscribe():
    """
    Subscribe to the topic and listen for messages.
    """
    try:
        with client.get_subscription_receiver(
            # TODO: Move Subscription Name to environment variable
            topic_name=TOPIC_NAME,
            subscription_name=SUBSCRIPTION_NAME,
        ) as receiver:
            # Open a connection to the bus as a sender
            with client.get_topic_sender(
                topic_name=TOPIC_NAME_RESULT
            ) as sender:
                # Need to structure better, this is just a proof of concept kept as small as possible
                for message in receiver:
                    # In production, this will be the place where we will call the model and do operations
                    # Should recieve JSON obj. for now just print it.
                    # Check if message is a JSON object
                    time.sleep(2)
                    # Consume the message to remove it from the queue.
                    receiver.complete_message(message)
                    # returning the message immediately back to the queue to be picked up by another (or the same) receiver.
                    # After doing operations, resend response back to bus.
                    # Send message to the bus.
                    sender.send_messages(
                        ServiceBusMessage("Recieved item:" + str(message))
                    )
    # Handle errors within the context manager scope of only bus operations.
    except OperationTimeoutError as err:
        # OperationTimeoutError: This indicates that the service
        # did not respond to an operation within the expected amount of time.
        # This may have been caused by a transient network issue or service problem.
        # The service may or may not have successfully completed the request;
        # the status is not known.
        # It is recommended to attempt to verify the current state and retry if necessary.
        print("Operation Timeout Error: ", err)
    except MessageAlreadySettled as err:
        # MessageAlreadySettled: This indicates failure to settle the message.
        # This could happen when trying to settle an already-settled message.
        print("Message Already Settled Error: ", err)
    except ServiceBusError as err:
        # All other Service Bus related errors.
        # It is the root error class of all the errors described above
        print("Service Bus Error: ", err)


if __name__ == "__main__":
    client = connect()
    if client:
        print("Connected to Service Bus " + str(TOPIC_NAME))
        subscribe()
