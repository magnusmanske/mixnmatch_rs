# Mix'n'match Micro API

A lightweight HTTP API for running Lua code fragments and other operations.

## Starting the server

### Via CLI

```bash
cargo run -- micro-api --port 8080
# or with a custom config:
cargo run -- micro-api --config /path/to/config.json --port 9000
```

### Via the main server loop

The micro-API is automatically spawned on port 8089 alongside the job
loop when `cargo run -- server` is used. The port is defined by the
`MICRO_API_PORT` constant in `app_state.rs`.

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

All responses return HTTP 200. Errors are indicated in the JSON body:

```json
{
  "status": "bad_request",
  "error": "description of what went wrong"
}
```

Status values: `ok`, `bad_request`, `not_found`, `internal_error`.

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

- Missing `function` or `entry_id` -> `bad_request`
- Unknown function name -> `bad_request`
- Entry not found -> `not_found`
- No Lua code fragment for this function/catalog -> `not_found`
- Lua execution error -> `internal_error`

---

### `get_code_fragments` -- List code fragments for a catalog

Returns all code fragments for a catalog, plus the list of all known
function types.

**Parameters:**

| Name | Required | Description |
|---|---|---|
| `action` | yes | `get_code_fragments` |
| `catalog` | yes | Numeric catalog ID |

**Example:**

```
GET /api?action=get_code_fragments&catalog=1
```

**Response:**

```json
{
  "status": "ok",
  "data": {
    "fragments": [
      {"id": 1072, "function": "PERSON_DATE", "catalog": 1, "php": "...", "lua": "...", "json": "", "is_active": 1, "note": null, "last_run": "2021-01-06 15:26:21"}
    ],
    "all_functions": ["AUX_FROM_DESC", "BESPOKE_SCRAPER", "COORDS_FROM_HTML", "DESC_FROM_HTML", "PERSON_DATE"]
  }
}
```

---

### `save_code_fragment` -- Create or update a code fragment

Saves a code fragment to the database and queues the appropriate
processing jobs.

**Parameters:**

| Name | Required | Description |
|---|---|---|
| `action` | yes | `save_code_fragment` |
| `fragment` | yes | URL-encoded JSON object with fragment fields |

The fragment JSON must include `catalog` (positive integer) and
`function`. Optional fields: `id` (to update existing), `php`, `lua`,
`json`, `is_active`, `note`.

**Example:**

```
GET /api?action=save_code_fragment&fragment=%7B%22catalog%22%3A1%2C%22function%22%3A%22PERSON_DATE%22%2C%22php%22%3A%22...%22%7D
```

**Response:**

```json
{
  "status": "ok",
  "data": {
    "id": 1072,
    "queued_jobs": ["update_person_dates", "match_person_dates"]
  }
}
```

Jobs queued automatically by function type:
- `PERSON_DATE` -> `update_person_dates` + `match_person_dates`
- `AUX_FROM_DESC` -> `generate_aux_from_description`
- `DESC_FROM_HTML` -> `update_descriptions_from_url`
