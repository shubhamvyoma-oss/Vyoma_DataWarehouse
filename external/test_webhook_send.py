import hashlib
import hmac
import json
import time
import requests

# ── CONFIG ──────────────────────────────────────
DB_HOST           = "localhost"
DB_NAME           = "edmingle_analytics"
DB_USER           = "postgres"
DB_PASSWORD       = "Svyoma"
DB_PORT           = 5432
WEBHOOK_SECRET    = "your_webhook_secret_here"
EDMINGLE_API_KEY  = "859b19531f4b149a605679c5ea21eeb8"
ORG_ID            = 683
INSTITUTION_ID    = 483
API_BASE_URL      = "https://vyoma-api.edmingle.com/nuSource/api/v1"
# ─────────────────────────────────────────────────

WEBHOOK_URL = "http://localhost:5000/webhook"

EVENT = {
    'id':              'manual-test-001',
    'event_name':      'user.user_created',
    'event_timestamp': int(time.time()),
    'is_live_mode':    False,
    'data': {
        'user_id':        99999999,
        'email':          'manual.test@vyoma.org',
        'name':           'Manual Test',
        'user_name':      'manualtest',
        'user_role':      'student',
        'created_at':     int(time.time()),
        'institution_id': INSTITUTION_ID,
    },
}


def main():
    body = json.dumps(EVENT)
    sig  = hmac.new(WEBHOOK_SECRET.encode(), body.encode(), hashlib.sha256).hexdigest()
    headers = {
        'Content-Type':        'application/json',
        'X-Webhook-Signature': sig,
    }
    resp = requests.post(WEBHOOK_URL, data=body, headers=headers, timeout=10)
    print(f"Status : {resp.status_code}")
    print(f"Body   : {resp.text}")
    print(f"Sig    : {sig}")


if __name__ == '__main__':
    main()
