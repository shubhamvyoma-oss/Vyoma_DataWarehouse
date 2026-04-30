import sys
import os
import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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

import psycopg2
import psycopg2.extras

from ingestion.webhook_receiver import (
    _init_pool, get_db_connection, release_db_connection,
    EVENT_ROUTER, mark_routed_to_silver,
)

_init_pool()


def _parse_event_timestamp(payload):
    if 'event' in payload and 'id' not in payload:
        event_obj    = payload.get('event', {})
        event_ts_str = event_obj.get('event_ts', '')
        try:
            return int(datetime.datetime.fromisoformat(event_ts_str).timestamp())
        except (ValueError, TypeError):
            return None
    return payload.get('event_timestamp')


def _extract_data(payload):
    if 'event' in payload and 'id' not in payload:
        return payload.get('payload', {})
    return payload.get('data', {})


def main():
    conn_read = get_db_connection()
    cur = conn_read.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT event_id, event_type, raw_payload
        FROM   bronze.webhook_events
        WHERE  event_type != 'url.validate'
        ORDER  BY id
    """)
    rows = cur.fetchall()
    cur.close()
    release_db_connection(conn_read)

    ok      = 0
    skipped = 0
    errors  = 0

    for row in rows:
        event_id   = row['event_id']
        event_type = row['event_type']
        payload    = row['raw_payload']

        router_fn = EVENT_ROUTER.get(event_type)
        if not router_fn:
            skipped += 1
            continue

        data            = _extract_data(payload)
        event_timestamp = _parse_event_timestamp(payload)

        conn = get_db_connection()
        try:
            router_fn(conn, event_id, event_type, data, event_timestamp)
            mark_routed_to_silver(conn, event_id)
            conn.commit()
            ok += 1
        except Exception as e:
            conn.rollback()
            print(f"ERROR  {event_type}  [{event_id}]: {e}", file=sys.stderr)
            errors += 1
        finally:
            release_db_connection(conn)

    print(f"\nReprocess complete: {ok} routed, {skipped} skipped (no router), {errors} errors")


if __name__ == '__main__':
    main()
