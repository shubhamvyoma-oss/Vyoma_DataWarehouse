import requests
import json

# This is the address where the webhook receiver program is running
# We need this to know where to send our test data
WEBHOOK_URL = "http://localhost:5000/webhook"

# This function sends a simple test message to the webhook
def send_simple_test():
    # We create a dictionary that looks like a real event
    # Webhooks usually send data in this JSON format
    test_data = {
        "id": "manual-test-001",
        "event_name": "manual.test",
        "data": {
            "message": "Hello! This is a manual test.",
            "value": 123
        }
    }
    
    # We print a message so we know the script is starting
    print("Sending test data to: " + WEBHOOK_URL)
    
    try:
        # We use the requests library to send a POST request
        # We pass our dictionary to the 'json' parameter
        response = requests.post(WEBHOOK_URL, json=test_data, timeout=5)
        
        # We check the status code the server gave back
        # 200 means success (OK)
        if response.status_code == 200:
            # We print that it worked
            print("SUCCESS: The server accepted our data.")
            # We show what the server said back to us
            print("Server said: " + response.text)
        else:
            # We print that the server rejected our data
            print("FAILURE: The server gave status code " + str(response.status_code))
            
    except Exception as error_message:
        # If the server is not running, the request will fail
        # We catch that error here and print a friendly message
        print("ERROR: I could not talk to the server.")
        # We print the specific error to help with fixing it
        print(error_message)

# This is the main part of the script
if __name__ == "__main__":
    # We call our function to send the test
    send_simple_test()
