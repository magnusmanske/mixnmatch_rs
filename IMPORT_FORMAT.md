# Importing and updating catalogs with MetaEntry JSON

Mix'n'match catalogs can be populated or refreshed from a JSON file that
describes every entry together with its associated data (auxiliary properties,
coordinates, person dates, descriptions, aliases, etc.).
This document explains the file format and how to run an import via the HTTP
API.

## Table of contents

- [Quick start](#quick-start)
- [File format](#file-format)
  - [JSON array vs. JSON-Lines](#json-array-vs-json-lines)
  - [MetaEntry object](#metaentry-object)
  - [entry (required)](#entry-required)
  - [auxiliary](#auxiliary)
  - [coordinate](#coordinate)
  - [person_dates](#person_dates)
  - [descriptions](#descriptions)
  - [aliases](#aliases)
  - [mnm_relations](#mnm_relations)
  - [kv_entries](#kv_entries)
  - [Ignored fields](#ignored-fields)
- [Import modes](#import-modes)
- [Fully-matched entry protection](#fully-matched-entry-protection)
- [HTTP API](#http-api)
  - [Inline entries](#inline-entries)
  - [Via uploaded import_file](#via-uploaded-import_file)
  - [Response](#response)
- [Practical tips](#practical-tips)
- [Complete example file](#complete-example-file)

---

## Quick start

1. Prepare a JSON file containing an array of MetaEntry objects (see below).
2. Upload it via the `upload_import_file` endpoint with `data_format` set to
   `json` (or `jsonl`).
3. Call the import endpoint with the returned UUID.

The importer matches entries by **`ext_id`** within the target catalog.
Entries that already exist are updated; new `ext_id` values create new entries.

---

## File format

### JSON array vs. JSON-Lines

The importer accepts two layouts:

**JSON array** -- a single file containing one JSON array of MetaEntry objects:

```json
[
  { "entry": { ... } },
  { "entry": { ... } }
]
```

**JSON-Lines (JSONL)** -- one MetaEntry object per line, no surrounding array
brackets, no trailing commas:

```
{"entry": { ... }}
{"entry": { ... }}
```

When uploading via the `upload_import_file` endpoint, set `data_format` to
`json` for a JSON-array file or `jsonl` for JSON-Lines.

### MetaEntry object

Every element in the file is a **MetaEntry** object.  Only `entry` is strictly
required; every other field can be omitted entirely.  Omitted fields default
to empty arrays, empty objects, or `null` as appropriate.

| Field | Type | Required | Written on import? |
|---|---|---|---|
| `entry` | object | **yes** | yes |
| `auxiliary` | array | no | yes |
| `coordinate` | object | no | yes |
| `person_dates` | object | no | yes |
| `descriptions` | object | no | yes |
| `aliases` | array | no | yes |
| `mnm_relations` | array | no | yes |
| `kv_entries` | array | no | yes |
| `issues` | array | no | **no** (ignored) |
| `log_entries` | array | no | **no** (ignored) |
| `multi_match` | array | no | **no** (ignored) |
| `statement_text` | array | no | **no** (ignored) |

### entry (required)

The core entry record.  Three fields are required: **`catalog`** (must match
the target catalog ID), **`ext_id`** (the unique identifier from the external
source), and **`ext_name`** (the display name).

All other fields are optional and can be omitted.

```json
{
  "entry": {
    "catalog": 1234,
    "ext_id": "n2014191777",
    "ext_url": "https://example.org/record/n2014191777",
    "ext_name": "Jane Doe",
    "ext_desc": "American physicist, 1920-2005",
    "type_name": "Q5"
  }
}
```

| Key | Type | Required | Description |
|---|---|---|---|
| `catalog` | integer | **yes** | **Must** equal the target catalog ID. Entries with a mismatched catalog are skipped with an error. |
| `ext_id` | string | **yes** | The external identifier for this entry (e.g. a VIAF number, an inventory number, a database key). This is the unique key used to match file entries against existing database entries. |
| `ext_name` | string | **yes** | Display name / label for the entry. |
| `ext_url` | string | no | URL pointing to the original record in the external source. Defaults to `""`. |
| `ext_desc` | string | no | Short description. Defaults to `""`. |
| `q` | integer | no | Wikidata item number (numeric, without the `Q` prefix). Omit for unmatched entries. See [Fully-matched entry protection](#fully-matched-entry-protection). |
| `user` | integer | no | The user ID who confirmed the match. `0` = automatic/preliminary match. When importing via `import_file`, this must be either omitted, `0`, or the uploading user's ID. Omit for unmatched entries. |
| `type_name` | string | no | Wikidata item ID (e.g. `"Q5"`) representing the entity type (Q5 = human, Q16521 = taxon, etc.). |

**Ignored entry fields:** The following fields are accepted in the JSON but
**ignored by the importer** -- they are managed automatically by the system:

- `id` -- internal database primary key; always assigned by the system.
- `timestamp` -- set automatically when a match is recorded.
- `random` -- assigned automatically for new entries (used for random sampling).

### auxiliary

An array of auxiliary property values attached to the entry.  Each element
carries a Wikidata property number and a string value.

```json
"auxiliary": [
  { "prop_numeric": 214, "value": "113084680" },
  { "prop_numeric": 496, "value": "0000-0001-2345-6789" }
]
```

| Key | Type | Required | Description |
|---|---|---|---|
| `prop_numeric` | integer | **yes** | Wikidata property number (e.g. `214` for VIAF ID, `496` for ORCID, `213` for ISNI). |
| `value` | string | **yes** | The property value as a string. |
| `in_wikidata` | boolean | no | Defaults to `false`. Set by the system during synchronisation. |
| `entry_is_matched` | boolean | no | Defaults to `false`. Set by the system. |

The `prop_numeric` value refers to the numeric part of a Wikidata property.
For example, property [P214](https://www.wikidata.org/wiki/Property:P214)
(VIAF ID) would be `214`.

**Ignored:** `row_id` (internal primary key) is accepted but always discarded.

### coordinate

Geographic coordinates for the entry.  Omit entirely if the entry has no
coordinates.

```json
"coordinate": {
  "lat": 51.5074,
  "lon": -0.1278,
  "precision": 0.0001
}
```

| Key | Type | Required | Description |
|---|---|---|---|
| `lat` | number | **yes** | Latitude in decimal degrees (WGS 84). |
| `lon` | number | **yes** | Longitude in decimal degrees (WGS 84). |
| `precision` | number | no | Coordinate precision in degrees. Omit for default. |

### person_dates

Birth and/or death dates for person entries.  Omit entirely if the entry is
not a person or dates are unknown.

```json
"person_dates": {
  "born": "1920-03-15",
  "died": "2005"
}
```

| Key | Type | Required | Description |
|---|---|---|---|
| `born` | string | no | Birth date. |
| `died` | string | no | Death date. |

Both fields are optional within the object; include only what you have.

**Date format:** Dates are strings in one of three precisions:

| Precision | Format | Example |
|---|---|---|
| Year only | `YYYY` | `"1920"` |
| Year and month | `YYYY-MM` | `"1920-03"` |
| Full date | `YYYY-MM-DD` | `"1920-03-15"` |
| BCE year | `-YYYY` | `"-500"` |
| BCE full | `-YYYY-MM-DD` | `"-500-06-15"` |

Months are zero-padded (01--12), days are zero-padded (01--31).

### descriptions

An object mapping language codes to description strings.  Language codes should
be [Wikimedia language codes](https://www.wikidata.org/wiki/Help:Wikimedia_language_codes/lists/all)
(usually ISO 639-1, e.g. `"en"`, `"de"`, `"fr"`).

```json
"descriptions": {
  "en": "American physicist known for crystallography research",
  "de": "US-amerikanische Physikerin"
}
```

Omit the field entirely if no descriptions are available.

### aliases

An array of alternative names in specific languages.

```json
"aliases": [
  { "language": "en", "value": "J. Doe" },
  { "language": "en", "value": "Jane M. Doe" },
  { "language": "de", "value": "Jane Doe" }
]
```

| Key | Type | Required | Description |
|---|---|---|---|
| `language` | string | **yes** | Language code. |
| `value` | string | **yes** | The alias text. |

Multiple aliases in the same language are allowed.

### mnm_relations

Relations to other Mix'n'match entries, expressed through a Wikidata property
and a target link.

```json
"mnm_relations": [
  {
    "property": 170,
    "target": { "type": "CatalogExtId", "value": {"catalog": 1234, "ext_id": "person-00042"} }
  }
]
```

| Key | Type | Required | Description |
|---|---|---|---|
| `property` | integer | **yes** | Wikidata property number (e.g. `170` for "creator"). |
| `target` | object | **yes** | A link describing the target. See below. |

**Target link types:**

The `target` field uses a tagged union with a `type` discriminator:

| type | value | Description |
|---|---|---|
| `"EntryId"` | integer | Another Mix'n'match entry by its internal ID. |
| `"CatalogExtId"` | `{"catalog": integer, "ext_id": string}` | Another entry identified by catalog + external ID. |
| `"WikidataQid"` | integer | A Wikidata item by its numeric Q-ID. |

Examples:

```json
{"type": "EntryId", "value": 98765}
{"type": "CatalogExtId", "value": {"catalog": 1234, "ext_id": "abc123"}}
{"type": "WikidataQid", "value": 42}
```

Only targets that can be resolved to an existing entry ID will be written.

### kv_entries

Arbitrary key-value metadata pairs.

```json
"kv_entries": [
  { "key": "source", "value": "batch_import_2024" },
  { "key": "category", "value": "physics" }
]
```

| Key | Type | Required | Description |
|---|---|---|---|
| `key` | string | **yes** | The metadata key. |
| `value` | string | **yes** | The metadata value. |

### Ignored fields

The following fields appear when you **export** a MetaEntry from the system but
are **ignored during import**.  You can include them in your file (they will be
silently skipped) or omit them entirely.

- **`issues`** -- managed through the issue tracking system.
- **`log_entries`** -- historical action log, created automatically.
- **`multi_match`** -- computed by the matching system.
- **`statement_text`** -- computed by the statement tracking system.

Additionally, the following per-record fields are always ignored (see the
`entry` and `auxiliary` sections above):

- `entry.id` -- internal primary key.
- `entry.timestamp` -- set automatically.
- `entry.random` -- set automatically.
- `auxiliary[].row_id` -- internal primary key.

---

## Import modes

| Mode | Value | Behaviour |
|---|---|---|
| **Add / Replace** | `add_replace` | New `ext_id` values create entries. Existing `ext_id` values are updated. No entries are deleted. This is the **default**. |
| **Add / Replace / Delete** | `add_replace_delete` | Same as above, but entries in the catalog whose `ext_id` is **not** in the file are deleted. Fully-matched entries are **never** deleted (see below). |

---

## Fully-matched entry protection

An entry is "fully matched" when a human user has confirmed its link to a
Wikidata item (`q > 0` and `user > 0`).  The importer protects these entries
in two ways:

1. **Match preservation:** When updating a fully-matched entry, the importer
   keeps the existing `q`, `user`, and `timestamp` values.  All other data
   (name, description, auxiliary properties, coordinates, etc.) is updated
   normally from the file.  The import summary counts these as
   `skipped_fully_matched`.

2. **Deletion protection:** In `add_replace_delete` mode, fully-matched
   entries are never deleted, even if their `ext_id` is absent from the file.

This ensures that human curation work is never lost by an automated import.

---

## HTTP API

```
POST /api/v1/import_catalog
Content-Type: application/json
```

### Inline entries

Send the MetaEntry objects directly in the request body:

```json
{
  "catalog_id": 1234,
  "mode": "add_replace",
  "entries": [
    { "entry": { "catalog": 1234, "ext_id": "abc", "ext_name": "Example" } }
  ]
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `catalog_id` | integer | **yes** | Target catalog ID. |
| `mode` | string | no | `"add_replace"` (default) or `"add_replace_delete"`. |
| `entries` | array | **yes** (unless `uuid` given) | Array of MetaEntry objects. |

### Via uploaded import_file

Instead of sending entries inline, reference a file previously uploaded via
`upload_import_file`.  The file's `type` must be `json` or `jsonl`.

```json
{
  "catalog_id": 1234,
  "mode": "add_replace",
  "uuid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `catalog_id` | integer | **yes** | Target catalog ID. |
| `mode` | string | no | `"add_replace"` (default) or `"add_replace_delete"`. |
| `uuid` | string | **yes** (unless `entries` given) | UUID returned by `upload_import_file`. |

When using `uuid`, the `entry.user` field in the file is validated: it must be
omitted, `0` (preliminary match), or equal to the user who uploaded the file
(`import_file.user`).  Entries with any other user value are skipped with an
error.

### Response

```json
{
  "status": "OK",
  "data": {
    "created": 150,
    "updated": 42,
    "skipped_fully_matched": 8,
    "deleted": 0,
    "errors": []
  }
}
```

| Field | Type | Description |
|---|---|---|
| `created` | integer | Number of new entries created. |
| `updated` | integer | Number of existing entries updated (not fully matched). |
| `skipped_fully_matched` | integer | Number of existing entries whose data was updated but whose Wikidata match was preserved. |
| `deleted` | integer | Number of entries deleted (only in `add_replace_delete` mode). |
| `errors` | array of strings | Per-entry error messages. |

For very large catalogs, prefer uploading a file via `upload_import_file` and
referencing it by UUID, rather than sending all entries inline.

---

## Practical tips

- **Start small.** Test your file with a handful of entries before importing a
  full catalog.

- **`ext_id` is the key.** The importer uses `catalog` + `ext_id` to decide
  whether to create or update.  Make sure every entry in your file has a unique
  `ext_id` and that `catalog` matches the target `catalog_id`.

- **Omit what you do not have.** If your source does not have coordinates,
  leave out the `coordinate` field entirely.  The same goes for
  `person_dates`, `auxiliary`, `aliases`, etc.  Only `entry` with its three
  required fields (`catalog`, `ext_id`, `ext_name`) is needed.

- **Use `type_name` when possible.** Setting `type_name` to a Wikidata class
  (e.g. `"Q5"` for humans, `"Q16521"` for taxa) greatly improves automatic
  matching quality.

- **Auxiliary properties map to Wikidata.** The `prop_numeric` value in
  `auxiliary` entries is the numeric part of a Wikidata property ID.  For
  example, use `214` for VIAF, `213` for ISNI, `496` for ORCID.  These values
  enable automatic matching against Wikidata.

- **Updates replace associated data.** When an existing entry is updated,
  all its associated data (auxiliary, coordinates, person dates, descriptions,
  aliases, MnM relations, KV entries) is deleted and re-created from the file.
  If your file omits a field that the entry currently has, that data will be
  removed.

- **Validate your JSON.** A syntax error on one line of a JSONL file will abort
  the parse.  Use `jq . < entries.json > /dev/null` or a similar tool to
  validate before importing.

---

## Complete example file

A file with two entries -- one human and one location:

```json
[
  {
    "entry": {
      "catalog": 1234,
      "ext_id": "person-00042",
      "ext_url": "https://example.org/people/00042",
      "ext_name": "Jane Doe",
      "ext_desc": "American physicist, 1920-2005",
      "type_name": "Q5"
    },
    "auxiliary": [
      { "prop_numeric": 214, "value": "113084680" }
    ],
    "person_dates": {
      "born": "1920-03-15",
      "died": "2005"
    },
    "descriptions": {
      "en": "American physicist known for crystallography research"
    },
    "aliases": [
      { "language": "en", "value": "J. Doe" }
    ]
  },
  {
    "entry": {
      "catalog": 1234,
      "ext_id": "place-00007",
      "ext_url": "https://example.org/places/00007",
      "ext_name": "Kew Gardens",
      "ext_desc": "Royal botanic garden in London",
      "type_name": "Q167346"
    },
    "coordinate": {
      "lat": 51.4787,
      "lon": -0.2956,
      "precision": 0.0001
    },
    "descriptions": {
      "en": "Royal botanic garden in southwest London"
    },
    "aliases": [
      { "language": "en", "value": "Royal Botanic Gardens, Kew" }
    ]
  }
]
```

A minimal entry with only the required fields:

```json
{"entry": {"catalog": 1234, "ext_id": "min-001", "ext_name": "Minimal Entry"}}
```
