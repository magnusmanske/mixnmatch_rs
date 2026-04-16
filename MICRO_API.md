# Mix'n'match Micro API

A lightweight HTTP API for running Lua code fragments and other operations.

## Starting the server

### Via CLI

```bash
cargo run -- api --port 8080
# or with a custom config:
cargo run -- api --config /path/to/config.json --port 9000
```

### Via the main server loop

Set the `PORT` environment variable before starting the server command.
The micro-API will be spawned automatically alongside the job loop:

```bash
PORT=8080 cargo run -- server
```

If `PORT` is not set, no micro-API server is started and the job loop
runs as before.

## Endpoints

All requests go to `GET /api` with query parameters.

Every request requires an `action` parameter. The response is always JSON.

### Success envelope

```json
{
  "status": "ok",
  "data": { ... }
}
```

### Error envelope

```json
{
  "error": "description of what went wrong"
}
```

HTTP status codes:
- `200` success
- `400` bad request (missing/invalid parameters)
- `404` not found (entry or code fragment missing)
- `500` internal error (Lua execution failure, DB error)

---

## Actions

### `run_lua` -- Run a Lua code fragment against an entry

Executes the Lua code fragment for the given function type against a
single entry, and returns the result without writing to the database.

**Parameters:**

| Name | Required | Description |
|---|---|---|
| `action` | yes | `run_lua` |
| `function` | yes | One of `PERSON_DATE`, `AUX_FROM_DESC`, `DESC_FROM_HTML` |
| `entry_id` | yes | Numeric entry ID |
| `html` | no | HTML content (only used by `DESC_FROM_HTML`) |

**Example:**

```
GET /api?action=run_lua&function=PERSON_DATE&entry_id=12345
```

**Response for `PERSON_DATE`:**

```json
{
  "status": "ok",
  "data": {
    "born": "1920",
    "died": "2000"
  }
}
```

**Response for `AUX_FROM_DESC`:**

```json
{
  "status": "ok",
  "data": {
    "commands": [
      {"type": "set_aux", "entry_id": 12345, "property": "214", "value": "67890"}
    ]
  }
}
```

**Response for `DESC_FROM_HTML`:**

```json
{
  "status": "ok",
  "data": {
    "descriptions": ["A famous person"],
    "born": "",
    "died": "",
    "change_type": null,
    "change_name": null,
    "location": null,
    "aux": [],
    "location_texts": [],
    "commands": []
  }
}
```

**Command types in the `commands` array:**

| Type | Fields |
|---|---|
| `set_aux` | `entry_id`, `property`, `value` |
| `set_match` | `entry_id`, `q` |
| `set_location` | `entry_id`, `lat`, `lon` |
| `set_person_dates` | `entry_id`, `born`, `died` |
| `set_description` | `entry_id`, `value` |
| `set_entry_name` | `entry_id`, `value` |
| `set_entry_type` | `entry_id`, `value` |
| `add_alias` | `entry_id`, `label`, `language` |
| `add_location_text` | `entry_id`, `property`, `value` |

**Error cases:**

- Missing `function` or `entry_id` -> 400
- Unknown function name -> 400
- Entry not found -> 404
- No Lua code fragment for this function/catalog -> 404
- Lua execution error -> 500
