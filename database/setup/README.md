# Setup: The Kitchen Build

This folder contains the scripts needed to build the structure of our database.

## Data Flow

```text
  [Database] ────► [Schema: Bronze]
              ────► [Schema: Silver]
              ────► [Schema: Gold]
```

## Scripts

| File | Description |
| :--- | :--- |
| `create_schemas.sql` | Creates the namespaces for our Medallion architecture. |

## How to Run
1. Open your PostgreSQL terminal (psql).
2. Run the command:
   ```sql
   \i database/setup/create_schemas.sql
   ```

## Common Errors
| Error | Cause | Fix |
| :--- | :--- | :--- |
| "Permission denied" | No admin rights. | Log in as the `postgres` superuser. |
