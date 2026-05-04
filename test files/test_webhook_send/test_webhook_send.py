# ============================================================
# 14 — SEND A TEST WEBHOOK EVENT
# ============================================================
# What it does: Sends one fake webhook event to the running
#               webhook server to confirm it is working.
#
# Why we need it: Before going live, you should verify that
#                 the webhook server (script 06) is accepting
#                 events and saving them to the database.
#
# How to run:
#   1. First start the webhook server:
#      python 06_webhook_receiver/webhook_receiver.py
#   2. Then run this script in a separate terminal:
#      python 14_test_webhook_send/test_webhook_send.py
#
# What to check after:
#   - This script should print: Status: 200
#   - Run 12_check_db_counts and look for bronze.webhook_events > 0
# ============================================================

import hashlib
import hmac
import json
import time
import requests

# ── SETTINGS ─────────────────────────────────────────────────
# The URL of the webhook server running on this machine
WEBHOOK_URL = "http://localhost:5000/webhook"

# The secret key used to sign our test event
# (must match what the webhook server expects)
WEBHOOK_SECRET = "your_webhook_secret_here"

# The institution ID for Vyoma
INSTITUTION_ID = 483
# ─────────────────────────────────────────────────────────────

# The test event we will send.
# This is a fake 'user created' event with a test user ID.
# is_live_mode: False means this is a test event, not real data.
TEST_EVENT = {
    "id":              "manual-test-001",
    "event_name":      "user.user_created",
    "event_timestamp": int(time.time()),
    "is_live_mode":    False,
    "data": {
        "user_id":        99999999,
        "email":          "manual.test@vyoma.org",
        "name":           "Manual Test User",
        "user_name":      "manualtestuser",
        "user_role":      "student",
        "created_at":     int(time.time()),
        "institution_id": INSTITUTION_ID,
    },
}


def build_hmac_signature(secret_key, message_body):
    # HMAC is a way to prove this message came from someone who knows the secret.
    # We sign the message body using SHA-256 hashing.
    signature = hmac.new(
        secret_key.encode(),   # The secret key as bytes
        message_body.encode(), # The message body as bytes
        hashlib.sha256         # The hashing algorithm
    ).hexdigest()
    return signature


def send_test_event():
    # Convert the test event dictionary to a JSON string
    event_body = json.dumps(TEST_EVENT)

    # Build the HMAC signature so the server knows this is a legitimate event
    signature = build_hmac_signature(WEBHOOK_SECRET, event_body)

    # Build the HTTP headers for the request
    request_headers = {
        "Content-Type":        "application/json",
        "X-Webhook-Signature": signature,
    }

    print("Sending test event to: " + WEBHOOK_URL)
    print("Event ID: " + TEST_EVENT["id"])
    print("Event type: " + TEST_EVENT["event_name"])
    print("")

    # Send the POST request with a 10-second timeout
    try:
        response = requests.post(
            WEBHOOK_URL,
            data=event_body,
            headers=request_headers,
            timeout=10,
        )
        # Print the result
        print("Status : " + str(response.status_code))
        print("Body   : " + response.text)
        print("Signature sent: " + signature)
        print("")

        # Check if the server accepted it
        if response.status_code == 200:
            print("SUCCESS: The webhook server accepted the event.")
            print("Check bronze.webhook_events in the database to confirm it was saved.")
        else:
            print("WARNING: Server returned a non-200 status code.")
            print("The webhook server may not be running or may have an error.")

    except Exception as error:
        print("ERROR: Could not connect to the webhook server.")
        print("Error message: " + str(error))
        print("Make sure the webhook server is running:")
        print("  python 06_webhook_receiver/webhook_receiver.py")


def main():
    print("=" * 50)
    print("TEST WEBHOOK SEND")
    print("=" * 50)
    print("")
    send_test_event()


if __name__ == "__main__":
    main()
