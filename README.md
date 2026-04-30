# E-Learning Vyoma DataWarehouse

Automated data pipeline for Vyoma Samskrta Pathasala
(sanskritfromhome.org) built on Edmingle LMS.

## Architecture
Bronze → Silver → Gold (medallion pattern)
- Bronze: raw webhook events + raw CSV imports
- Silver: cleaned, typed, deduplicated records
- Gold: SQL views for Power BI (coming soon)

## Data Sources
- Webhooks: real-time events from Edmingle (16 event types)
- API: daily attendance pull via report_type=55 (coming soon)
- CSV: one-time historical backfill (complete)

## Pipeline Status
- silver.users        : 93,316 rows
- silver.transactions : 424,437 rows
- Webhook events live : yes (via ngrok → production VPS soon)

## Folder Structure
ingestion/          — live webhook receiver
database/           — SQL schema files per table
  schemas/          — CREATE SCHEMA statements
  bronze/           — Bronze table definitions
  silver/           — Silver table definitions
  gold/             — Gold view definitions (coming soon)
scripts/
  migrations/       — one-time CSV backfill scripts
  external/         — utility scripts (check DB, clear test data)
tests/              — end-to-end and unavailability tests

## How to Run (Local)
1. python ingestion/webhook_receiver.py
2. Point ngrok to localhost:5000
3. Set Edmingle webhook URL to ngrok URL

## How to Run Database Setup (Fresh)
psql -U postgres -d edmingle_analytics -f database/run_all.sql

## Tests
python tests/test_pipeline_e2e.py
Result: 38/38 PASS
