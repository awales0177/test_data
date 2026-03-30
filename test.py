#!/usr/bin/env python3
"""
Load api/____data (or api/_data) *.json into PostgreSQL: one table per array-of-objects (schema mirrors
scalars as typed columns; lists/objects become JSONB). Only JSON fields that are
non-null on at least one row become columns. toolkit + toolkit_technologies (relational columns + readme_refs JSONB for tab markdown → readmes; details JSONB without tab bodies).
All tables use UUID v7 primary keys (`id`). JSON `id` is not stored as a column; loaders map JSON id → row UUID in memory for release_notes rows and FK inserts.
Inferred columns use snake_case (JSON CamelCase keys converted; `updatedBy` / `updated_by` → `last_updated_by`).
statistics: page_views (one row per page per day), site_visits (one row per day), usage_summary.
dataAgreements: product_agreements (no embedded release-notes / versionHistory columns) + release_notes + version_history (polymorphic join_id); `systemsIngestedFrom` / `consumingSystems` / `dataValidator` are JSON tag arrays → `systems_ingested_from` / `consuming_systems` / `data_validator` JSONB (`ensure_product_agreements_system_columns` renames legacy `parent_system` and coerces legacy TEXT → JSONB where needed). data_models append to both child tables too.
Normalized FKs: product_agreements.data_model_id; model_rules.data_model_id → data_models; sub_domains.data_domain_id → data_domains.id. Changelog join_id has no FK (polymorphic).
model_rules / country_rules: legacy JSON taggedObjects/Columns/Functions are merged into a single **tags** JSONB column at load (no separate tagged_* columns).
data_models: no referenceData/versionHistory/meta blobs on row; versionHistory → version_history table; meta_* columns; resources JSONB array of {kind, label, href}; readme/requirements text → readmes table; data_models.readme_refs JSONB list of dicts {readme?, requirements? → uuid}.
toolkit_technologies: tab markdown → readmes rows; toolkit_technologies.readme_refs JSONB list of dicts {installation?, usage?, requirements?, evaluation? → uuid}; details JSONB omits those tab bodies.
readmes: id UUID PK, readme TEXT (shared markdown store).
glossary.json → glossary_terms (markdown → readmes + markdown_refs JSONB).
datasets.json → catalog_datasets (dataset_slug = JSON id; tables/model list as JSONB).
dh_feedback → user feedback rows (user_id UUID optional → dh_users.id, feedback_text); ensured after JSON loads, no seed file.
dataPolicies.json → data_standards (standard_name, status, description, owner, links, tags; JSON `name` → standard_name, externalLink → links).
dataDomains.json → data_domains (no subdomains column; JSON domain `id` → catalog_slug) + sub_domains (data_domain_id FK; JSON subdomain id → slug).
users.json → dh_users (inferred columns; optional stable JSON **`id`** becomes row PK; else UUID v7. Typical keys: username, email, fullName, role, active, pinnedItems [{id, type}]).
Usage (Amazon RDS only — local Postgres is not supported):
  Set ``DATABASE_URL`` (or ``RDS_DATABASE_URL`` if ``DATABASE_URL`` is empty) in api/.env with your
  RDS endpoint and ``sslmode=require``, or set ``PGHOST`` and the other ``PG*`` vars (no localhost).
  Allow your IP in the RDS security group (inbound TCP on the DB port, usually 5432).

  python scripts/upload_data_to_postgres.py --init-schema
  python scripts/upload_data_to_postgres.py
  python scripts/upload_data_to_postgres.py --dry-run

  ``--init-schema`` drops and recreates the target schema — use only on a dedicated DB or empty schema.

After a successful load, prints a section listing JSON object keys that were removed or
never became columns (including explicit loader drops and keys skipped because they were
always null).

Requires:
  pip install "psycopg[binary]>=3.1"
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from uuid import UUID
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

# Filled during upload; printed at end (keys from JSON that never become table columns).
UPLOAD_JSON_KEY_REPORT: List[Dict[str, Any]] = []

_API_ROOT = Path(__file__).resolve().parent.parent
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(_API_ROOT / ".env")
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Environment variables (.env via load_dotenv, or process environment).
# Uploader targets Amazon RDS only (no localhost defaults).
# ---------------------------------------------------------------------------
ENV_DATABASE_URL: Optional[str] = os.getenv("DATABASE_URL")
ENV_RDS_DATABASE_URL: Optional[str] = os.getenv("RDS_DATABASE_URL")
ENV_PGHOST: str = (os.getenv("PGHOST") or "").strip()
ENV_PGPORT: str = os.getenv("PGPORT", "5432")
ENV_PGDATABASE: str = os.getenv("PGDATABASE", "catalog")
ENV_PGUSER: str = os.getenv("PGUSER", "postgres")
ENV_PGPASSWORD: str = os.getenv("PGPASSWORD") or ""
ENV_PGSSLMODE: str = (os.getenv("PGSSLMODE") or "require").strip()
ENV_PG_SCHEMA: str = os.getenv("PG_SCHEMA", "catalog")

# Default Postgres schema name for DDL/DML in this script (overridable by --schema).
SCHEMA: str = ENV_PG_SCHEMA

from catalog_uuid import new_uuid7
from services.postgres_catalog import (
    DH_FEEDBACK_TABLE,
    RELEASE_NOTES_TABLE,
    VERSION_HISTORY_TABLE,
    _toolkit_tech_build_readme_slots,
    _toolkit_tech_readme_refs_json_from_slots,
    ensure_dh_feedback_table,
    ensure_product_agreements_standards_id_uuid_array,
    ensure_product_agreements_system_columns,
    ensure_readmes_table,
    insert_readme_content,
)

# Seed JSON `id` (e.g. retention_policy_001) → row UUID for the same upload run (no catalog_key column).
STANDARD_SEED_ID_TO_UUID: Dict[str, str] = {}
# (agreement JSON `id` string, list of ref strings from dataStandards / dataPolicies)
_AGREEMENT_STANDARD_REFS: List[Tuple[str, List[Any]]] = []
# From last process_data_agreements: external id → product_agreements PK
_AGREEMENT_ID_BY_EXTERNAL: Dict[str, str] = {}


def _uuid_or_new_uuid7(val: Any) -> str:
    """Use JSON `id` as row UUID when valid; otherwise allocate a new UUID v7."""
    if val is None or val == "":
        return str(new_uuid7())
    try:
        return str(UUID(str(val).strip()))
    except ValueError:
        return str(new_uuid7())


def record_inferred_table_report(
    table: str,
    source_file: str,
    rows: List[dict],
    sql_columns: Dict[str, str],
    removed_before_rows: Optional[Dict[str, str]] = None,
    notes: Optional[str] = None,
) -> None:
    """Rows are the dicts passed into upload_object_array (after any strip)."""
    removed_before_rows = dict(sorted((removed_before_rows or {}).items()))
    all_json_keys: Set[str] = set()
    for r in rows:
        if isinstance(r, dict):
            all_json_keys.update(r.keys())
    stored_sql = set(sql_columns.keys())
    not_stored: List[Tuple[str, str]] = []
    for jk in sorted(all_json_keys):
        sk = json_key_to_sql_column(jk)
        if sk in stored_sql:
            continue
        if jk == "id":
            not_stored.append(
                (jk, "JSON id not stored as a data column (PK UUID is generated)"),
            )
        else:
            not_stored.append(
                (
                    jk,
                    "no column — always null on every row for this table, or excluded from inference",
                ),
            )
    UPLOAD_JSON_KEY_REPORT.append(
        {
            "table": table,
            "source_file": source_file,
            "removed_before_insert": removed_before_rows,
            "not_stored_from_row_dicts": not_stored,
            "notes": notes,
        }
    )


def record_fixed_policy_keys(source_file: str, table: str, rows: List[dict]) -> None:
    """data_standards: only a fixed set of JSON fields are loaded."""
    mapped_json = frozenset(
        {"name", "status", "description", "owner", "externalLink", "tags"}
    )
    all_k: Set[str] = set()
    for obj in rows:
        if isinstance(obj, dict):
            all_k.update(obj.keys())
    omitted = sorted(all_k - mapped_json)
    if not omitted:
        return
    removed: Dict[str, str] = {}
    for k in omitted:
        if k == "id":
            removed[k] = "not loaded; row UUID generated"
        else:
            removed[k] = "not mapped in process_data_standards (loader drops key)"
    UPLOAD_JSON_KEY_REPORT.append(
        {
            "table": table,
            "source_file": source_file,
            "removed_before_insert": removed,
            "not_stored_from_row_dicts": [],
            "notes": "Fixed-schema table: name→standard_name, externalLink→links[0]",
        }
    )


_TOOLKIT_PARENT_JSON_KEYS_USED = frozenset(
    {
        "id",
        "name",
        "displayName",
        "description",
        "category",
        "tags",
        "author",
        "version",
        "lastUpdated",
        "technologies",
        "cardImage",
        "card_image",
    }
)
_TOOLKIT_TECH_JSON_KEYS_USED = frozenset(
    {
        "id",
        "itemId",
        "name",
        "description",
        "rank",
        "language",
        "documentation",
        "githubRepo",
        "lastUpdated",
        "pros",
        "cons",
        "details",
        "installation",
        "usage",
        "requirements",
        "evaluation",
        "status",
        "likes",
        "dislikes",
        "iconOverrides",
        "maintainerTeamId",
    }
)


def _toolkit_tech_status(tech: dict) -> str:
    s = tech.get("status")
    if s is None and isinstance(tech.get("details"), dict):
        s = tech["details"].get("status")
    if s is None or s == "":
        return "production"
    v = str(s).strip().lower()
    if v in ("evaluated", "evaluation"):
        return "evaluated"
    return "production"


def _toolkit_tech_details_json(tech: dict, Json: Any) -> Any:
    """Tab markdown lives in readmes + readme_refs; details JSONB keeps likes, iconOverrides, etc. only."""
    merged: dict = {}
    d = tech.get("details")
    if isinstance(d, dict):
        merged.update(d)
    merged.pop("status", None)
    for tab in ("installation", "usage", "requirements", "evaluation"):
        merged.pop(tab, None)
    for k in ("iconOverrides", "maintainerTeamId"):
        v = tech.get(k)
        if v is not None and v != "":
            merged[k] = v
    for k in ("likes", "dislikes"):
        if tech.get(k) is not None:
            try:
                merged[k] = int(tech[k])
            except (TypeError, ValueError):
                pass
    return Json(merged)


def record_toolkit_json_drops_from_data(
    source_file: str, toolkit_root: dict
) -> None:
    """toolkit.json: only fixed parent columns + technologies subtree are loaded; details JSONB is {}."""
    blocks: List[str] = []
    toolkit = toolkit_root.get("toolkit")
    if not isinstance(toolkit, dict):
        return
    for bucket, val in sorted(toolkit.items()):
        if not isinstance(val, list):
            continue
        for i, obj in enumerate(val):
            if not isinstance(obj, dict):
                continue
            extra = sorted(set(obj.keys()) - _TOOLKIT_PARENT_JSON_KEYS_USED)
            if extra:
                blocks.append(
                    f"  bucket={bucket!r} item[{i}] parent JSON keys not loaded: {extra}"
                )
            if str(bucket).lower() != "toolkits":
                continue
            for j, tech in enumerate(obj.get("technologies") or []):
                if not isinstance(tech, dict):
                    continue
                tex = sorted(set(tech.keys()) - _TOOLKIT_TECH_JSON_KEYS_USED)
                if tex:
                    blocks.append(
                        f"  bucket=toolkits tech[{j}] JSON keys not loaded: {tex}"
                    )
    if not blocks:
        return
    UPLOAD_JSON_KEY_REPORT.append(
        {
            "table": "toolkit + toolkit_technologies",
            "source_file": source_file,
            "removed_before_insert": {
                "(all other parent keys)": "not stored — loader only writes listed columns; `details` JSONB inserted as {}",
                "(toolkit item subtree)": "only technologies[] rows go to toolkit_technologies",
            },
            "not_stored_from_row_dicts": [],
            "notes": "Per-object drops:\n" + "\n".join(blocks),
        }
    )


def record_statistics_json_drops(source_file: str, data: dict) -> None:
    """Statistics: root keys and nested keys that are ignored."""
    removed: Dict[str, str] = {}
    top = set(data.keys()) if isinstance(data, dict) else set()
    expected_top = {"pageViews", "siteVisits", "totalViews", "lastUpdated"}
    for k in sorted(top - expected_top):
        removed[k] = "root key not read by process_statistics"
    pv = data.get("pageViews")
    if isinstance(pv, dict):
        for _page, block in pv.items():
            if not isinstance(block, dict):
                continue
            for sub in sorted(set(block.keys()) - {"daily"}):
                removed.setdefault(
                    f"pageViews[*].{sub}",
                    "only `daily` is loaded; other per-page fields discarded",
                )
    sv = data.get("siteVisits")
    if isinstance(sv, dict):
        for sub in sorted(set(sv.keys()) - {"daily"}):
            removed.setdefault(
                f"siteVisits.{sub}",
                "only `siteVisits.daily` is loaded for site_visits table",
            )
    if removed:
        UPLOAD_JSON_KEY_REPORT.append(
            {
                "table": "page_views + site_visits + usage_summary",
                "source_file": source_file,
                "removed_before_insert": dict(sorted(removed.items())),
                "not_stored_from_row_dicts": [],
                "notes": "Aggregates rebuilt in DB from daily rows where applicable",
            }
        )


def print_json_key_report() -> None:
    if not UPLOAD_JSON_KEY_REPORT:
        return
    print("\n=== JSON keys not persisted as table columns ===\n")
    for block in UPLOAD_JSON_KEY_REPORT:
        print(f"Table: {block['table']}  (source: {block['source_file']})")
        if block.get("notes"):
            print(f"  Note: {block['notes']}")
        for k, why in block.get("removed_before_insert", {}).items():
            print(f"  − {k!r}  →  {why}")
        for jk, why in block.get("not_stored_from_row_dicts", []):
            print(f"  − {jk!r}  →  {why}")
        print()


def require_psycopg():
    try:
        import psycopg
        from psycopg.types.json import Json

        return psycopg, Json
    except ImportError:
        print(
            "Install psycopg: pip install 'psycopg[binary]>=3.1'",
            file=sys.stderr,
        )
        sys.exit(1)


_VALID_IDENT = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
PG_IDENT_MAX = 63

# PostgreSQL table names (catalog schema): one explicit name per (file, JSON array key).
# Keeps names self-explanatory instead of stem_key concatenation.
_FILE_TABLE_MAP: Dict[str, Dict[str, str]] = {
    "dataAgreements.json": {"agreements": "product_agreements"},
    "apis.json": {"apis": "registered_apis"},
    "countryRules.json": {"rules": "country_rules"},
    "dataDomains.json": {"domains": "data_domains"},
    "data_teams.json": {"data_teams": "data_teams"},
    "glossary.json": {"terms": "glossary_terms"},
    "notifications.json": {"notifications": "notifications"},
    "rules.json": {"rules": "model_rules"},
    "users.json": {"users": "dh_users"},
    "datasets.json": {"datasets": "catalog_datasets"},
}

# JSON field name -> PostgreSQL column name (inferred tables only).
# Other JSON keys are converted with _to_snake_case().
JSON_FIELD_TO_SQL_NAME: Dict[str, str] = {
    "updatedBy": "last_updated_by",
    "updated_by": "last_updated_by",
    # Legacy seed key maps to renamed column (see ensure_product_agreements_system_columns).
    "parentSystem": "systems_ingested_from",
}


def _to_snake_case(name: str) -> str:
    """Convert JSON CamelCase / mixed keys to snake_case for Postgres columns."""
    if not name:
        return name
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.replace("-", "_").lower()


def json_key_to_sql_column(json_key: str) -> str:
    if json_key in JSON_FIELD_TO_SQL_NAME:
        return JSON_FIELD_TO_SQL_NAME[json_key]
    return _to_snake_case(json_key)


def normalize_row_keys_for_sql(row: dict) -> dict:
    """Shallow copy: each JSON key -> snake_case (or JSON_FIELD_TO_SQL_NAME) column name."""
    return {json_key_to_sql_column(k): v for k, v in row.items()}


def _sql_column_source_keys() -> Dict[str, Tuple[str, ...]]:
    inv: Dict[str, List[str]] = {}
    for jk, sk in JSON_FIELD_TO_SQL_NAME.items():
        inv.setdefault(sk, []).append(jk)
    return {sk: tuple(keys) for sk, keys in inv.items()}


SQL_COLUMN_SOURCE_KEYS: Dict[str, Tuple[str, ...]] = _sql_column_source_keys()


def assert_safe_ident(name: str) -> str:
    n = name.lower()
    if not _VALID_IDENT.match(n) or n in {"pg_catalog", "information_schema"}:
        sys.exit(f"Unsafe identifier: {name!r}")
    return n[:PG_IDENT_MAX]


def classify_value(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int) and not isinstance(v, bool):
        return "int"
    if isinstance(v, float):
        return "float"
    if isinstance(v, str):
        return "str"
    if isinstance(v, (list, dict)):
        return "jsonb"
    return "str"


def merge_types(types: Sequence[str]) -> str:
    non_null = [t for t in types if t != "null"]
    if not non_null:
        return "TEXT"
    if all(t == "bool" for t in non_null):
        return "BOOLEAN"
    if all(t == "int" for t in non_null):
        return "BIGINT"
    if all(t in ("int", "float") for t in non_null):
        return "DOUBLE PRECISION"
    if all(t == "float" for t in non_null):
        return "DOUBLE PRECISION"
    if all(t == "str" for t in non_null):
        return "TEXT"
    if any(t == "jsonb" for t in non_null):
        return "JSONB"
    # mixed scalars (e.g. int + str)
    return "TEXT"


def infer_columns(rows: List[dict]) -> Dict[str, str]:
    if not rows:
        return {}
    keys: set[str] = set()
    for r in rows:
        keys.update(r.keys())
    columns: Dict[str, str] = {}
    for k in sorted(keys):
        types_list: List[str] = []
        for r in rows:
            if k not in r:
                continue
            types_list.append(classify_value(r[k]))
        columns[k] = merge_types(types_list)
    return columns


def infer_columns_nonempty(rows: List[dict]) -> Dict[str, str]:
    """Like infer_columns but drop keys that are null (or missing) on every row."""
    full = infer_columns(rows)
    if not full or not rows:
        return full
    keep: Dict[str, str] = {}
    for k, t in full.items():
        if any(isinstance(r, dict) and r.get(k) is not None for r in rows):
            keep[k] = t
    return keep


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def table_name(stem: str, array_key: str) -> str:
    return assert_safe_ident(f"{stem}_{array_key}")


def canonical_table_for_array(filename: str, array_key: str) -> str:
    """Resolve human-readable table name; exit if file/key is unknown."""
    m = _FILE_TABLE_MAP.get(filename)
    if not m:
        sys.exit(
            f"Add an entry to _FILE_TABLE_MAP for data file {filename!r} "
            f"(array key {array_key!r})"
        )
    t = m.get(array_key)
    if not t:
        sys.exit(
            f"No table name mapped for {filename!r} / array key {array_key!r}. "
            f"Known keys: {sorted(m)!r}"
        )
    return assert_safe_ident(t)


def _row_value_for_sql_column(row: dict, sql_col: str) -> Any:
    if sql_col in row:
        return row[sql_col]
    if sql_col in SQL_COLUMN_SOURCE_KEYS:
        for jk in SQL_COLUMN_SOURCE_KEYS[sql_col]:
            v = row.get(jk)
            if v is not None:
                return v
        return None
    return None


def row_to_tuple_for_sql_columns(
    row: dict,
    sql_columns: Dict[str, str],
    Json: Any,
) -> Tuple[Any, ...]:
    out = []
    for c in sorted(sql_columns.keys()):
        t = sql_columns[c]
        v = _row_value_for_sql_column(row, c)
        if v is None:
            out.append(None)
        elif t == "JSONB":
            out.append(Json(v))
        elif t == "BIGINT":
            out.append(int(v))
        elif t == "DOUBLE PRECISION":
            out.append(float(v))
        elif t == "BOOLEAN":
            out.append(bool(v))
        elif t == "UUID":
            out.append(None if v is None else str(v))
        else:
            out.append(
                v
                if isinstance(v, str)
                else json.dumps(v)
                if isinstance(v, (dict, list))
                else str(v)
            )
    return tuple(out)


def infer_sql_columns_uuid(rows: List[dict]) -> Dict[str, str]:
    """Column types for data columns only (no UUID pk). JSON `id` is omitted (not persisted)."""
    inferred = infer_columns_nonempty(rows)
    if rows and not inferred:
        inferred = infer_columns(rows)
    inferred.pop("id", None)
    sql_cols: Dict[str, str] = {}
    coalesced: Dict[str, str] = {}
    for k, t in inferred.items():
        sql_name = json_key_to_sql_column(k)
        if sql_name in coalesced:
            coalesced[sql_name] = merge_types([coalesced[sql_name], t])
        else:
            coalesced[sql_name] = t
    for k in sorted(coalesced.keys()):
        sql_cols[k] = coalesced[k]
    return sql_cols


def create_table_sql_uuid_pk(
    schema: str,
    table: str,
    sql_columns: Dict[str, str],
) -> str:
    parts = [f"{quote_ident('id')} UUID PRIMARY KEY"]
    for c, t in sorted(sql_columns.items()):
        parts.append(f"{quote_ident(c)} {t}")
    cols = ",\n    ".join(parts)
    return (
        f"CREATE TABLE {quote_ident(schema)}.{quote_ident(table)} (\n    {cols}\n)"
    )


def upload_object_array(
    cur: Any,
    schema: str,
    rows: List[dict],
    Json: Any,
    *,
    table: str,
    extra_sql_columns: Optional[Dict[str, str]] = None,
    row_ids: Optional[Sequence[str]] = None,
    report_ctx: Optional[Tuple[str, str, Optional[Dict[str, str]]]] = None,
) -> Tuple[str, Dict[str, str]]:
    """Returns table name and map JSON id (stringified) -> inserted row UUID (for loaders)."""
    tbl = assert_safe_ident(table)
    ext_map: Dict[str, str] = {}
    if not rows:
        cur.execute(
            f"CREATE TABLE {quote_ident(schema)}.{quote_ident(tbl)} (\n"
            f"    {quote_ident('id')} UUID PRIMARY KEY,\n"
            f"    {quote_ident('_note')} TEXT DEFAULT 'empty seed'\n)"
        )
        cur.execute(
            f"INSERT INTO {quote_ident(schema)}.{quote_ident(tbl)} ({quote_ident('id')}) "
            f"VALUES (%s)",
            (str(new_uuid7()),),
        )
        return tbl, ext_map
    sql_columns = infer_sql_columns_uuid(rows)
    if extra_sql_columns:
        for k, v in extra_sql_columns.items():
            sql_columns[k] = v
    if report_ctx and rows:
        rn, fn, rk = report_ctx
        record_inferred_table_report(rn, fn, rows, sql_columns, rk)
    norm_rows = [
        normalize_row_keys_for_sql(r) if isinstance(r, dict) else r for r in rows
    ]
    ddl = create_table_sql_uuid_pk(schema, tbl, sql_columns)
    cur.execute(ddl)
    data_cols = sorted(sql_columns.keys())
    qcols = ", ".join([quote_ident("id")] + [quote_ident(c) for c in data_cols])
    placeholders = ", ".join(["%s"] * (1 + len(data_cols)))
    insert_sql = (
        f"INSERT INTO {quote_ident(schema)}.{quote_ident(tbl)} ({qcols}) VALUES ({placeholders})"
    )
    if row_ids is not None and len(row_ids) != len(rows):
        sys.exit(
            f"row_ids length ({len(row_ids)}) must match rows ({len(rows)}) for {tbl!r}"
        )
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        uid = str(row_ids[i]) if row_ids is not None else str(new_uuid7())
        ev = row.get("id")
        if ev is not None:
            ext_map[str(ev)] = uid
        cur.execute(
            insert_sql,
            (uid,) + row_to_tuple_for_sql_columns(norm_rows[i], sql_columns, Json),
        )
    return tbl, ext_map


TOOLKIT_TABLE = "toolkit"
TOOLKIT_TECH_TABLE = "toolkit_technologies"

DATA_STANDARDS_TABLE = "data_standards"

SUB_DOMAINS_TABLE = "sub_domains"

ENTITY_TYPE_PRODUCT_AGREEMENT = "product_agreement"
ENTITY_TYPE_DATA_MODEL = "data_model"

_DATA_MODEL_JSON_DROP: frozenset[str] = frozenset(
    {"id", "changelog", "referenceData", "versionHistory", "meta", "resources"}
)


def _changelog_changes_json(entry: dict, Json: Any) -> Any:
    raw = entry.get("changes")
    if raw is None:
        return Json([])
    if isinstance(raw, list):
        return Json([str(x) for x in raw if x is not None])
    return Json([str(raw)])


def _resource_rows_from_json(resources: Any) -> List[Tuple[str, str, str]]:
    """(kind, label, href) — kind is 'url' or 'tool'."""
    out: List[Tuple[str, str, str]] = []
    if not isinstance(resources, dict):
        return out
    for key, val in resources.items():
        if key == "tools" and isinstance(val, dict):
            for tname, url in val.items():
                if url is not None and str(url).strip():
                    out.append(("tool", str(tname), str(url)))
        elif val is not None and isinstance(val, str) and val.strip():
            out.append(("url", str(key), val))
    return out


def _resources_normalized_list(resources: Any) -> List[dict]:
    return [
        {"kind": k, "label": lbl, "href": href}
        for k, lbl, href in _resource_rows_from_json(resources)
    ]


def _optional_str(val: Any) -> Optional[str]:
    if val is None:
        return None
    return str(val)


def _optional_int(val: Any) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _version_history_db_params(entry: dict, Json: Any) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Any]:
    """Map one JSON versionHistory object to normalized columns; field list → JSONB."""
    ver = _optional_str(entry.get("version"))
    changed_at = _optional_str(entry.get("timestamp"))
    updated_by = _optional_str(entry.get("updatedBy"))
    change_description = _optional_str(entry.get("changeDescription"))
    fc = entry.get("fieldChanges")
    if not isinstance(fc, list):
        cf = entry.get("changedFields")
        fc = cf if isinstance(cf, list) else []
    return ver, changed_at, updated_by, change_description, Json(fc)


def _normalize_tags(val: Any, Json: Any) -> Any:
    if isinstance(val, list):
        return Json(val)
    return Json([])


def _normalize_pros_cons(val: Any, Json: Any) -> Any:
    if isinstance(val, list):
        return Json([str(x) for x in val if x is not None])
    return Json([])


def _normalize_standards_owner(val: Any, Json: Any) -> Any:
    if val is None:
        return Json([])
    if isinstance(val, list):
        return Json([str(x) for x in val if x is not None])
    return Json([str(val)])


def _normalize_standards_links(obj: dict, Json: Any) -> Any:
    out: List[str] = []
    el = obj.get("externalLink")
    if el:
        out.append(str(el))
    return Json(out)


def process_data_standards(cur: Any, schema: str, data: dict, Json: Any) -> List[str]:
    """
    Former data policies → data_standards: fixed columns only.
    JSON `name` → standard_name; `externalLink` → links[0].
    """
    policies = data.get("policies")
    if not isinstance(policies, list):
        policies = []
    rows = [x for x in policies if isinstance(x, dict)]
    tbl = assert_safe_ident(DATA_STANDARDS_TABLE)
    sch_q = quote_ident(schema)
    tq = quote_ident(tbl)

    cur.execute(
        f"""CREATE TABLE {sch_q}.{tq} (
            {quote_ident("id")} UUID PRIMARY KEY,
            {quote_ident("standard_name")} TEXT,
            {quote_ident("status")} TEXT,
            {quote_ident("description")} TEXT,
            {quote_ident("owner")} JSONB NOT NULL DEFAULT '[]'::jsonb,
            {quote_ident("links")} JSONB NOT NULL DEFAULT '[]'::jsonb,
            {quote_ident("tags")} JSONB NOT NULL DEFAULT '[]'::jsonb
        )"""
    )

    insert_sql = f"""INSERT INTO {sch_q}.{tq} (
            {quote_ident("id")}, {quote_ident("standard_name")},
            {quote_ident("status")}, {quote_ident("description")}, {quote_ident("owner")},
            {quote_ident("links")}, {quote_ident("tags")}
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)"""

    if not rows:
        cur.execute(
            insert_sql,
            (
                str(new_uuid7()),
                None,
                None,
                None,
                Json([]),
                Json([]),
                Json([]),
            ),
        )
        return [tbl]

    for obj in rows:
        uid = str(new_uuid7())
        seed = obj.get("id")
        if seed is not None and str(seed).strip():
            STANDARD_SEED_ID_TO_UUID[str(seed).strip()] = uid
        cur.execute(
            insert_sql,
            (
                uid,
                _optional_str(obj.get("name")),
                _optional_str(obj.get("status")),
                _optional_str(obj.get("description")),
                _normalize_standards_owner(obj.get("owner"), Json),
                _normalize_standards_links(obj, Json),
                _normalize_pros_cons(obj.get("tags"), Json),
            ),
        )
    if rows:
        record_fixed_policy_keys("dataPolicies.json", tbl, rows)
    return [tbl]


def process_toolkit(cur: Any, schema: str, data: dict, Json: Any) -> List[str]:
    """
    toolkit: listing columns for each toolkit.* list item (no JSON blob column).
    toolkit_technologies: technologies[] under toolkits; pros/cons as JSONB arrays.
    """
    ensure_readmes_table(cur, schema)
    toolkit = data.get("toolkit")
    if not isinstance(toolkit, dict):
        return []
    tbl = assert_safe_ident(TOOLKIT_TABLE)
    tech_tbl = assert_safe_ident(TOOLKIT_TECH_TABLE)

    cur.execute(
        f"""CREATE TABLE {quote_ident(schema)}.{quote_ident(tbl)} (
            {quote_ident("id")} UUID PRIMARY KEY,
            {quote_ident("bucket")} TEXT NOT NULL,
            {quote_ident("item_id")} TEXT,
            {quote_ident("name")} TEXT,
            {quote_ident("display_name")} TEXT,
            {quote_ident("description")} TEXT,
            {quote_ident("category")} TEXT,
            {quote_ident("tags")} JSONB NOT NULL DEFAULT '[]'::jsonb,
            {quote_ident("author")} TEXT,
            {quote_ident("version")} TEXT,
            {quote_ident("last_updated")} TEXT,
            {quote_ident("card_image")} TEXT,
            {quote_ident("details")} JSONB NOT NULL DEFAULT '{{}}'::jsonb
        )"""
    )
    cur.execute(
        f"""CREATE INDEX {quote_ident(f"{tbl}_bucket_idx")}
            ON {quote_ident(schema)}.{quote_ident(tbl)} ({quote_ident("bucket")})"""
    )
    cur.execute(
        f"""CREATE INDEX {quote_ident(f"{tbl}_item_id_idx")}
            ON {quote_ident(schema)}.{quote_ident(tbl)} ({quote_ident("item_id")})"""
    )

    cur.execute(
        f"""CREATE TABLE {quote_ident(schema)}.{quote_ident(tech_tbl)} (
            {quote_ident("id")} UUID PRIMARY KEY,
            {quote_ident("toolkit_row_id")} UUID NOT NULL
                REFERENCES {quote_ident(schema)}.{quote_ident(tbl)} ({quote_ident("id")})
                ON DELETE CASCADE,
            {quote_ident("item_id")} TEXT,
            {quote_ident("name")} TEXT,
            {quote_ident("description")} TEXT,
            {quote_ident("rank")} INTEGER,
            {quote_ident("status")} TEXT NOT NULL DEFAULT 'production',
            {quote_ident("language")} TEXT,
            {quote_ident("documentation")} TEXT,
            {quote_ident("github_repo")} TEXT,
            {quote_ident("last_updated")} TEXT,
            {quote_ident("pros")} JSONB NOT NULL DEFAULT '[]'::jsonb,
            {quote_ident("cons")} JSONB NOT NULL DEFAULT '[]'::jsonb,
            {quote_ident("details")} JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            {quote_ident("readme_refs")} JSONB NOT NULL DEFAULT '[]'::jsonb
        )"""
    )
    cur.execute(
        f"""CREATE INDEX {quote_ident(f"{tech_tbl}_parent_idx")}
            ON {quote_ident(schema)}.{quote_ident(tech_tbl)} ({quote_ident("toolkit_row_id")})"""
    )

    insert_parent = f"""INSERT INTO {quote_ident(schema)}.{quote_ident(tbl)} (
            {quote_ident("id")}, {quote_ident("bucket")}, {quote_ident("item_id")},
            {quote_ident("name")}, {quote_ident("display_name")}, {quote_ident("description")},
            {quote_ident("category")}, {quote_ident("tags")}, {quote_ident("author")},
            {quote_ident("version")}, {quote_ident("last_updated")}, {quote_ident("card_image")},
            {quote_ident("details")}
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING {quote_ident("id")}"""

    insert_tech = f"""INSERT INTO {quote_ident(schema)}.{quote_ident(tech_tbl)} (
            {quote_ident("id")}, {quote_ident("toolkit_row_id")}, {quote_ident("item_id")}, {quote_ident("name")},
            {quote_ident("description")}, {quote_ident("rank")}, {quote_ident("status")}, {quote_ident("language")},
            {quote_ident("documentation")}, {quote_ident("github_repo")}, {quote_ident("last_updated")},
            {quote_ident("pros")}, {quote_ident("cons")}, {quote_ident("details")},
            {quote_ident("readme_refs")}
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    row_count = 0
    for bucket, val in toolkit.items():
        if not isinstance(val, list):
            continue
        b = assert_safe_ident(str(bucket))
        for obj in val:
            if not isinstance(obj, dict):
                continue
            raw_id = obj.get("id")
            item_id = str(raw_id) if raw_id is not None else None
            name = _optional_str(obj.get("name"))
            display_name = _optional_str(obj.get("displayName") or obj.get("name"))
            doc = _optional_str(obj.get("description"))
            category = _optional_str(obj.get("category"))
            tags = _normalize_tags(obj.get("tags"), Json)
            author = _optional_str(obj.get("author"))
            version = _optional_str(obj.get("version"))
            last_updated = _optional_str(obj.get("lastUpdated"))
            card_img = obj.get("cardImage")
            if card_img is None:
                card_img = obj.get("card_image")
            card_image_sql = _optional_str(card_img) if card_img is not None else None

            parent_uid = str(new_uuid7())
            cur.execute(
                insert_parent,
                (
                    parent_uid,
                    b,
                    item_id,
                    name,
                    display_name,
                    doc,
                    category,
                    tags,
                    author,
                    version,
                    last_updated,
                    card_image_sql,
                    Json({}),
                ),
            )
            parent_row = cur.fetchone()
            if not parent_row:
                continue
            parent_pk = parent_row[0]
            row_count += 1

            if b == "toolkits":
                for tech in obj.get("technologies") or []:
                    if not isinstance(tech, dict):
                        continue
                    tid = tech.get("id")
                    t_item_raw = tech.get("itemId") if tech.get("itemId") is not None else tid
                    t_item_id = str(t_item_raw) if t_item_raw is not None else None
                    r_slots = _toolkit_tech_build_readme_slots(cur, schema, tech)
                    cur.execute(
                        insert_tech,
                        (
                            str(new_uuid7()),
                            parent_pk,
                            t_item_id,
                            _optional_str(tech.get("name")),
                            _optional_str(tech.get("description")),
                            _optional_int(tech.get("rank")),
                            _toolkit_tech_status(tech),
                            _optional_str(tech.get("language")),
                            _optional_str(tech.get("documentation")),
                            _optional_str(tech.get("githubRepo")),
                            _optional_str(tech.get("lastUpdated")),
                            _normalize_pros_cons(tech.get("pros"), Json),
                            _normalize_pros_cons(tech.get("cons"), Json),
                            _toolkit_tech_details_json(tech, Json),
                            _toolkit_tech_readme_refs_json_from_slots(r_slots, Json),
                        ),
                    )

    if row_count == 0:
        cur.execute(
            f"INSERT INTO {quote_ident(schema)}.{quote_ident(tbl)} "
            f"({quote_ident('id')}, {quote_ident('bucket')}, {quote_ident('item_id')}, {quote_ident('tags')}, {quote_ident('details')}) "
            f"VALUES (%s, %s, NULL, %s, %s)",
            (str(new_uuid7()), "empty", Json([]), Json({})),
        )

    record_toolkit_json_drops_from_data("toolkit.json", data)
    return [tbl, tech_tbl]


def process_data_domains(cur: Any, schema: str, root: dict, Json: Any) -> List[str]:
    """
    data_domains: domain rows without embedded subdomains JSON.
    sub_domains: one row per JSON domains[].subdomains[] item; FK data_domain_id → data_domains.id.
    """
    domains = root.get("domains")
    if not isinstance(domains, list):
        domains = []
    rows_in = [x for x in domains if isinstance(x, dict)]
    tbl_parent = canonical_table_for_array("dataDomains.json", "domains")
    stripped: List[dict] = []
    for d in rows_in:
        row = {k: v for k, v in d.items() if k != "subdomains"}
        cid = d.get("id")
        if cid is not None:
            row["catalog_slug"] = str(cid)
        stripped.append(row)
    tbl, ext_map = upload_object_array(
        cur,
        schema,
        stripped,
        Json,
        table=tbl_parent,
        report_ctx=(
            tbl_parent,
            "dataDomains.json",
            {"subdomains": "normalized into sub_domains (not a column on data_domains)"},
        ),
    )

    sub_tbl = assert_safe_ident(SUB_DOMAINS_TABLE)
    sch_q = quote_ident(schema)
    pq = quote_ident(tbl)
    sq = quote_ident(sub_tbl)

    cur.execute(
        f"""CREATE TABLE {sch_q}.{sq} (
            {quote_ident("id")} UUID PRIMARY KEY,
            {quote_ident("data_domain_id")} UUID NOT NULL
                REFERENCES {sch_q}.{pq} ({quote_ident("id")})
                ON DELETE CASCADE,
            {quote_ident("slug")} TEXT NOT NULL,
            {quote_ident("name")} TEXT,
            {quote_ident("description")} TEXT,
            UNIQUE ({quote_ident("data_domain_id")}, {quote_ident("slug")})
        )"""
    )
    cur.execute(
        f"""CREATE INDEX {quote_ident(f"{sub_tbl}_data_domain_id_idx")}
            ON {sch_q}.{sq} ({quote_ident("data_domain_id")})"""
    )

    insert_sub = f"""INSERT INTO {sch_q}.{sq} (
            {quote_ident("id")}, {quote_ident("data_domain_id")}, {quote_ident("slug")},
            {quote_ident("name")}, {quote_ident("description")}
        ) VALUES (%s, %s, %s, %s, %s)"""

    for d in rows_in:
        did = d.get("id")
        if did is None:
            continue
        parent_uid = ext_map.get(str(did))
        if not parent_uid:
            continue
        for sd in d.get("subdomains") or []:
            if not isinstance(sd, dict):
                continue
            seid = sd.get("id")
            if seid is None:
                continue
            cur.execute(
                insert_sub,
                (
                    str(new_uuid7()),
                    parent_uid,
                    str(seid),
                    _optional_str(sd.get("name")),
                    _optional_str(sd.get("description")),
                ),
            )
    sub_keys: Set[str] = set()
    for d in rows_in:
        for sd in d.get("subdomains") or []:
            if isinstance(sd, dict):
                sub_keys.update(sd.keys())
    sub_keep = {"id", "name", "description"}
    sub_extra = sorted(sub_keys - sub_keep)
    if sub_extra:
        UPLOAD_JSON_KEY_REPORT.append(
            {
                "table": "sub_domains",
                "source_file": "dataDomains.json (subdomains[] items)",
                "removed_before_insert": {
                    k: "not loaded; only id→slug, name, description are persisted"
                    for k in sub_extra
                },
                "not_stored_from_row_dicts": [],
                "notes": None,
            }
        )
    return [tbl_parent, sub_tbl]


def _normalize_agreement_system_tag_fields(agr: dict) -> None:
    """Coerce systemsIngestedFrom / consumingSystems / dataValidator to string lists; migrate parentSystem in-place."""
    ps = agr.pop("parentSystem", None)
    if ps is not None and agr.get("systemsIngestedFrom") is None:
        if isinstance(ps, str):
            agr["systemsIngestedFrom"] = [ps.strip()] if ps.strip() else []
        elif isinstance(ps, list):
            agr["systemsIngestedFrom"] = [
                str(x).strip() for x in ps if x is not None and str(x).strip()
            ]
        else:
            agr["systemsIngestedFrom"] = (
                [str(ps).strip()] if str(ps).strip() else []
            )

    si = agr.get("systemsIngestedFrom")
    if si is None:
        agr["systemsIngestedFrom"] = []
    elif isinstance(si, str):
        agr["systemsIngestedFrom"] = [si.strip()] if si.strip() else []
    elif not isinstance(si, list):
        agr["systemsIngestedFrom"] = (
            [str(si).strip()] if str(si).strip() else []
        )
    else:
        agr["systemsIngestedFrom"] = [
            str(x).strip() for x in si if x is not None and str(x).strip()
        ]

    cs = agr.get("consumingSystems")
    if cs is None:
        agr["consumingSystems"] = []
    elif isinstance(cs, str):
        agr["consumingSystems"] = [cs.strip()] if cs.strip() else []
    elif not isinstance(cs, list):
        agr["consumingSystems"] = (
            [str(cs).strip()] if str(cs).strip() else []
        )
    else:
        agr["consumingSystems"] = [
            str(x).strip() for x in cs if x is not None and str(x).strip()
        ]

    dv = agr.get("dataValidator")
    if dv is None:
        agr["dataValidator"] = []
    elif isinstance(dv, str):
        agr["dataValidator"] = [dv.strip()] if dv.strip() else []
    elif not isinstance(dv, list):
        agr["dataValidator"] = (
            [str(dv).strip()] if str(dv).strip() else []
        )
    else:
        agr["dataValidator"] = [
            str(x).strip() for x in dv if x is not None and str(x).strip()
        ]


def process_data_agreements(cur: Any, schema: str, root: dict, Json: Any) -> List[str]:
    """
    product_agreements: same as JSON agreements[] but without embedded changelog.
    release_notes table: agreement rows here; model rows appended in process_data_models (both use join_id).
    """
    ensure_product_agreements_system_columns(cur, schema)
    agreements = root.get("agreements")
    if not isinstance(agreements, list):
        agreements = []

    stripped: List[dict] = []
    changelog_buffers: List[tuple] = []
    vh_buffers_agreements: List[Tuple[str, int, dict]] = []

    for agr in agreements:
        if not isinstance(agr, dict):
            continue
        _normalize_agreement_system_tag_fields(agr)
        aid = agr.get("id")
        cl = agr.get("changelog")
        vhist = agr.get("versionHistory")
        std_refs = agr.get("dataStandards")
        if std_refs is None:
            std_refs = agr.get("dataPolicies")
        if aid is not None and isinstance(std_refs, list) and std_refs:
            _AGREEMENT_STANDARD_REFS.append((str(aid), list(std_refs)))
        stripped.append(
            {
                k: v
                for k, v in agr.items()
                if k
                not in (
                    "changelog",
                    "versionHistory",
                    "dataPolicies",
                    "dataStandards",
                )
            }
        )
        if aid is not None and isinstance(vhist, list):
            for seq, entry in enumerate(vhist):
                if isinstance(entry, dict):
                    vh_buffers_agreements.append((str(aid), seq, entry))
        if aid is None or not isinstance(cl, list):
            continue
        for seq, entry in enumerate(cl):
            if not isinstance(entry, dict):
                continue
            changelog_buffers.append(
                (
                    str(aid),
                    ENTITY_TYPE_PRODUCT_AGREEMENT,
                    _optional_str(entry.get("version")),
                    _optional_str(entry.get("date")),
                    _changelog_changes_json(entry, Json),
                    seq,
                )
            )

    pa_tbl = canonical_table_for_array("dataAgreements.json", "agreements")
    tbl, agreement_id_by_external = upload_object_array(
        cur,
        schema,
        stripped,
        Json,
        table=pa_tbl,
        extra_sql_columns={"acceptance_criteria": "TEXT"},
        report_ctx=(
            pa_tbl,
            "dataAgreements.json",
            {
                "changelog": "loaded into release_notes table (JSON/API key remains changelog; not a column on product_agreements)",
                "versionHistory": "loaded into version_history table (not a column on product_agreements)",
                "dataStandards": "loaded into product_agreements.standards_id after upload (UUID[])",
                "dataPolicies": "same as dataStandards (legacy key)",
                "systemsIngestedFrom": "product_agreements.systems_ingested_from (JSONB string array; legacy parentSystem normalized before load)",
                "consumingSystems": "product_agreements.consuming_systems (JSONB string array)",
                "dataValidator": "product_agreements.data_validator (JSONB string array; legacy TEXT coerced on migrate)",
            },
        ),
    )
    _AGREEMENT_ID_BY_EXTERNAL.update(agreement_id_by_external)

    cur.execute(
        f"""CREATE TABLE {quote_ident(schema)}.{quote_ident(RELEASE_NOTES_TABLE)} (
            {quote_ident("id")} UUID PRIMARY KEY,
            {quote_ident("join_id")} UUID,
            {quote_ident("entity_type")} TEXT NOT NULL,
            {quote_ident("version")} TEXT,
            {quote_ident("entry_date")} TEXT,
            {quote_ident("changes")} JSONB NOT NULL DEFAULT '[]'::jsonb,
            {quote_ident("sequence")} INT NOT NULL DEFAULT 0
        )"""
    )
    cur.execute(
        f"""CREATE INDEX {quote_ident(f"{RELEASE_NOTES_TABLE}_entity_type_idx")}
            ON {quote_ident(schema)}.{quote_ident(RELEASE_NOTES_TABLE)}
            ({quote_ident("entity_type")})"""
    )
    cur.execute(
        f"""CREATE INDEX {quote_ident(f"{RELEASE_NOTES_TABLE}_join_id_idx")}
            ON {quote_ident(schema)}.{quote_ident(RELEASE_NOTES_TABLE)}
            ({quote_ident("join_id")})"""
    )

    _create_version_history_table(cur, schema)

    if changelog_buffers:
        insert_cl = f"""INSERT INTO {quote_ident(schema)}.{quote_ident(RELEASE_NOTES_TABLE)} (
            {quote_ident("id")}, {quote_ident("join_id")},
            {quote_ident("entity_type")},
            {quote_ident("version")}, {quote_ident("entry_date")},
            {quote_ident("changes")}, {quote_ident("sequence")}
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        for (
            aid_key,
            ent_type,
            ver,
            edate,
            chg,
            seq,
        ) in changelog_buffers:
            pa_uuid = agreement_id_by_external.get(aid_key)
            cur.execute(
                insert_cl,
                (
                    str(new_uuid7()),
                    pa_uuid,
                    ent_type,
                    ver,
                    edate,
                    chg,
                    seq,
                ),
            )

    if vh_buffers_agreements:
        insert_vh = f"""INSERT INTO {quote_ident(schema)}.{quote_ident(VERSION_HISTORY_TABLE)} (
            {quote_ident("id")}, {quote_ident("join_id")},
            {quote_ident("entity_type")}, {quote_ident("sequence")},
            {quote_ident("version")}, {quote_ident("changed_at")},
            {quote_ident("updated_by")}, {quote_ident("change_description")},
            {quote_ident("field_changes")}
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        for aid_key, seq, entry in vh_buffers_agreements:
            pa_uuid = agreement_id_by_external.get(aid_key)
            if pa_uuid is None:
                continue
            v, ts, ub, desc, fch = _version_history_db_params(entry, Json)
            cur.execute(
                insert_vh,
                (
                    str(new_uuid7()),
                    pa_uuid,
                    ENTITY_TYPE_PRODUCT_AGREEMENT,
                    seq,
                    v,
                    ts,
                    ub,
                    desc,
                    fch,
                ),
            )

    return [tbl, RELEASE_NOTES_TABLE, VERSION_HISTORY_TABLE]


def process_data_models(cur: Any, schema: str, root: dict, Json: Any) -> List[str]:
    """data_models: flattened meta_*; resources as JSONB [{kind, label, href}, ...]; drop referenceData, versionHistory."""
    ensure_readmes_table(cur, schema)
    models = root.get("models")
    if not isinstance(models, list):
        models = []
    rows_in = [x for x in models if isinstance(x, dict)]
    tbl_sql = assert_safe_ident("data_models")

    stripped: List[dict] = []
    changelog_buffers: List[tuple] = []
    vh_buffers_models: List[Tuple[str, int, dict]] = []
    row_uuids: List[str] = []
    model_id_by_external: Dict[str, str] = {}

    for m in rows_in:
        mid = m.get("id")
        row_uuid = str(new_uuid7())
        row_uuids.append(row_uuid)
        if mid is not None:
            model_id_by_external[str(mid)] = row_uuid
        cl = m.get("changelog")
        vhist = m.get("versionHistory")
        if mid is not None and isinstance(vhist, list):
            for seq, entry in enumerate(vhist):
                if isinstance(entry, dict):
                    vh_buffers_models.append((str(mid), seq, entry))
        meta = m.get("meta") if isinstance(m.get("meta"), dict) else {}
        resources = m.get("resources")
        row: Dict[str, Any] = {
            k: v for k, v in m.items() if k not in _DATA_MODEL_JSON_DROP
        }
        row["meta_tier"] = meta.get("tier")
        row["meta_verified"] = meta.get("verified")
        cc = meta.get("clickCount")
        row["meta_click_count"] = _optional_int(cc) if cc is not None else None
        row["resources"] = _resources_normalized_list(resources)
        readme_val = row.pop("readme", None)
        requirements_val = row.pop("requirements", None)
        ref_list: List[Dict[str, str]] = []
        if rid := insert_readme_content(cur, schema, readme_val):
            ref_list.append({"readme": rid})
        if reqid := insert_readme_content(cur, schema, requirements_val):
            ref_list.append({"requirements": reqid})
        row["readme_refs"] = ref_list
        stripped.append(row)

        if mid is None or not isinstance(cl, list):
            continue
        for seq, entry in enumerate(cl):
            if not isinstance(entry, dict):
                continue
            changelog_buffers.append(
                (
                    str(mid),
                    ENTITY_TYPE_DATA_MODEL,
                    _optional_str(entry.get("version")),
                    _optional_str(entry.get("date")),
                    _changelog_changes_json(entry, Json),
                    seq,
                )
            )

    upload_object_array(
        cur,
        schema,
        stripped,
        Json,
        table=tbl_sql,
        extra_sql_columns={
            "meta_tier": "TEXT",
            "meta_verified": "BOOLEAN",
            "meta_click_count": "BIGINT",
            "resources": "JSONB",
            "readme_refs": "JSONB",
        },
        row_ids=row_uuids,
        report_ctx=(
            tbl_sql,
            "dataModels.json",
            {
                "changelog": "loaded into release_notes table (API key unchanged)",
                "id": "JSON id not stored as column; PK UUID generated",
                "referenceData": "stripped by loader (not persisted)",
                "versionHistory": "loaded into version_history table (not columns on data_models)",
                "meta": "object removed; values flattened to meta_tier, meta_verified, meta_click_count",
                "readme": "body → readmes; FK(s) in data_models.readme_refs JSON list",
                "requirements": "body → readmes; FK(s) in data_models.readme_refs JSON list",
            },
        ),
    )

    if changelog_buffers and _table_exists(cur, schema, RELEASE_NOTES_TABLE):
        insert_cl = f"""INSERT INTO {quote_ident(schema)}.{quote_ident(RELEASE_NOTES_TABLE)} (
            {quote_ident("id")}, {quote_ident("join_id")},
            {quote_ident("entity_type")},
            {quote_ident("version")}, {quote_ident("entry_date")},
            {quote_ident("changes")}, {quote_ident("sequence")}
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        for (
            mid_key,
            ent_type,
            ver,
            edate,
            chg,
            seq,
        ) in changelog_buffers:
            dm_uuid = model_id_by_external.get(mid_key)
            cur.execute(
                insert_cl,
                (
                    str(new_uuid7()),
                    dm_uuid,
                    ent_type,
                    ver,
                    edate,
                    chg,
                    seq,
                ),
            )

    if vh_buffers_models:
        _ensure_version_history_table(cur, schema)
        insert_vh = f"""INSERT INTO {quote_ident(schema)}.{quote_ident(VERSION_HISTORY_TABLE)} (
            {quote_ident("id")}, {quote_ident("join_id")},
            {quote_ident("entity_type")}, {quote_ident("sequence")},
            {quote_ident("version")}, {quote_ident("changed_at")},
            {quote_ident("updated_by")}, {quote_ident("change_description")},
            {quote_ident("field_changes")}
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        for mid_key, seq, entry in vh_buffers_models:
            dm_uuid = model_id_by_external.get(mid_key)
            if dm_uuid is None:
                continue
            v, ts, ub, desc, fch = _version_history_db_params(entry, Json)
            cur.execute(
                insert_vh,
                (
                    str(new_uuid7()),
                    dm_uuid,
                    ENTITY_TYPE_DATA_MODEL,
                    seq,
                    v,
                    ts,
                    ub,
                    desc,
                    fch,
                ),
            )

    return [tbl_sql]


def process_glossary(cur: Any, schema: str, root: dict, Json: Any) -> List[str]:
    """glossary_terms: JSON `id` not stored; markdown body → readmes; markdown_refs lists {markdown: uuid}."""
    ensure_readmes_table(cur, schema)
    terms = root.get("terms")
    if not isinstance(terms, list):
        terms = []
    rows_in = [x for x in terms if isinstance(x, dict)]
    tbl_sql = canonical_table_for_array("glossary.json", "terms")
    stripped: List[dict] = []
    for r in rows_in:
        row = {k: v for k, v in r.items() if k != "id"}
        md_val = row.pop("markdown", None)
        ref_list: List[Dict[str, str]] = []
        if rid := insert_readme_content(cur, schema, md_val):
            ref_list.append({"markdown": rid})
        row["markdown_refs"] = ref_list
        stripped.append(row)
    row_uuids = [str(new_uuid7()) for _ in stripped]
    upload_object_array(
        cur,
        schema,
        stripped,
        Json,
        table=tbl_sql,
        row_ids=row_uuids,
        extra_sql_columns={"markdown_refs": "JSONB"},
        report_ctx=(
            tbl_sql,
            "glossary.json",
            {
                "id": "not stored as column; PK UUID generated",
                "markdown": "body stored in readmes; FK in markdown_refs JSON list",
            },
        ),
    )
    return [tbl_sql]


def _table_exists(cur: Any, schema: str, table: str) -> bool:
    cur.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, table),
    )
    return cur.fetchone() is not None


def _create_version_history_table(cur: Any, schema: str) -> None:
    cur.execute(
        f"""CREATE TABLE {quote_ident(schema)}.{quote_ident(VERSION_HISTORY_TABLE)} (
            {quote_ident("id")} UUID PRIMARY KEY,
            {quote_ident("join_id")} UUID,
            {quote_ident("entity_type")} TEXT NOT NULL,
            {quote_ident("sequence")} INT NOT NULL DEFAULT 0,
            {quote_ident("version")} TEXT,
            {quote_ident("changed_at")} TEXT,
            {quote_ident("updated_by")} TEXT,
            {quote_ident("change_description")} TEXT,
            {quote_ident("field_changes")} JSONB NOT NULL DEFAULT '[]'::jsonb
        )"""
    )
    cur.execute(
        f"""CREATE INDEX {quote_ident(f"{VERSION_HISTORY_TABLE}_entity_type_idx")}
            ON {quote_ident(schema)}.{quote_ident(VERSION_HISTORY_TABLE)}
            ({quote_ident("entity_type")})"""
    )
    cur.execute(
        f"""CREATE INDEX {quote_ident(f"{VERSION_HISTORY_TABLE}_join_id_idx")}
            ON {quote_ident(schema)}.{quote_ident(VERSION_HISTORY_TABLE)}
            ({quote_ident("join_id")})"""
    )


def _ensure_version_history_table(cur: Any, schema: str) -> None:
    if _table_exists(cur, schema, VERSION_HISTORY_TABLE):
        return
    _create_version_history_table(cur, schema)


def _column_names_lower(cur: Any, schema: str, table: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog = current_database()
          AND table_schema = %s AND table_name = %s
        """,
        (schema, table),
    )
    return {r[0].lower() for r in cur.fetchall()}


def _data_model_ids_by_short_name(cur: Any, schema: str) -> Dict[str, str]:
    if not _table_exists(cur, schema, "data_models"):
        return {}
    if not _column_names_lower(cur, schema, "data_models") >= {"short_name", "id"}:
        return {}
    cur.execute(
        f"""SELECT {quote_ident("id")}, {quote_ident("short_name")}
            FROM {quote_ident(schema)}.{quote_ident("data_models")}"""
    )
    out: Dict[str, str] = {}
    for rid, sn in cur.fetchall():
        if sn is not None:
            out[str(sn)] = str(rid)
    return out


def _collapse_rule_row_to_tags(row: dict) -> dict:
    """Merge tags + taggedObjects/Columns/Functions into a single `tags` string list; drop legacy keys."""
    d = dict(row)
    merged: List[str] = []
    for key in ("tags", "taggedObjects", "taggedColumns", "taggedFunctions"):
        v = d.pop(key, None)
        if isinstance(v, list):
            for x in v:
                if x is None:
                    continue
                s = str(x).strip()
                if s:
                    merged.append(s)
    seen_lower: Set[str] = set()
    tags_out: List[str] = []
    for s in merged:
        lk = s.lower()
        if lk not in seen_lower:
            seen_lower.add(lk)
            tags_out.append(s)
    d["tags"] = tags_out
    return d


def _ensure_model_rules_foreign_key(cur: Any, schema: str) -> None:
    if not _table_exists(cur, schema, "data_models") or not _table_exists(
        cur, schema, "model_rules"
    ):
        return
    if "data_model_id" not in _column_names_lower(cur, schema, "model_rules"):
        return
    sch = quote_ident(schema)
    mr = quote_ident("model_rules")
    dm = quote_ident("data_models")
    cur.execute(
        f"""ALTER TABLE {sch}.{mr}
            DROP CONSTRAINT IF EXISTS {quote_ident("fk_model_rules_data_model")}"""
    )
    cur.execute(
        f"""ALTER TABLE {sch}.{mr}
            ADD CONSTRAINT {quote_ident("fk_model_rules_data_model")}
            FOREIGN KEY ({quote_ident("data_model_id")})
            REFERENCES {sch}.{dm} ({quote_ident("id")})
            ON DELETE SET NULL"""
    )
    cur.execute(
        f"""CREATE INDEX IF NOT EXISTS {quote_ident("idx_model_rules_data_model_id")}
            ON {sch}.{mr} ({quote_ident("data_model_id")})"""
    )


def process_model_rules(cur: Any, schema: str, root: dict, Json: Any) -> List[str]:
    """model_rules: no model_short_name column; data_model_id FK from JSON modelShortName at load."""
    rules = root.get("rules")
    if not isinstance(rules, list):
        rules = []
    rows_in = [x for x in rules if isinstance(x, dict)]
    tbl_sql = canonical_table_for_array("rules.json", "rules")
    sch_q = quote_ident(schema)
    mr = quote_ident(tbl_sql)

    short_to_id = _data_model_ids_by_short_name(cur, schema)

    if not rows_in:
        cur.execute(
            f"""CREATE TABLE {sch_q}.{mr} (
                {quote_ident("id")} UUID PRIMARY KEY,
                {quote_ident("data_model_id")} UUID,
                {quote_ident("tags")} JSONB DEFAULT '[]'::jsonb,
                {quote_ident("_note")} TEXT DEFAULT 'empty seed'
            )"""
        )
        cur.execute(
            f"""INSERT INTO {sch_q}.{mr} ({quote_ident("id")}, {quote_ident("data_model_id")}, {quote_ident("tags")})
                VALUES (%s, NULL, %s)""",
            (str(new_uuid7()), Json([])),
        )
        _ensure_model_rules_foreign_key(cur, schema)
        return [tbl_sql]

    stripped: List[dict] = []
    for r in rows_in:
        d = {k: v for k, v in r.items() if k != "modelShortName"}
        msn = r.get("modelShortName")
        if msn is not None and str(msn).strip():
            d["data_model_id"] = short_to_id.get(str(msn).strip())
        else:
            d["data_model_id"] = None
        stripped.append(_collapse_rule_row_to_tags(d))

    upload_object_array(
        cur,
        schema,
        stripped,
        Json,
        table=tbl_sql,
        extra_sql_columns={"data_model_id": "UUID", "tags": "JSONB"},
        report_ctx=(
            tbl_sql,
            "rules.json",
            {
                "modelShortName": "not stored as text; resolved to data_model_id FK only",
                "taggedObjects": "merged into tags JSONB; not a separate column",
                "taggedColumns": "merged into tags JSONB; not a separate column",
                "taggedFunctions": "merged into tags JSONB; not a separate column",
            },
        ),
    )
    _ensure_model_rules_foreign_key(cur, schema)
    return [tbl_sql]


def process_country_rules(cur: Any, schema: str, root: dict, Json: Any) -> List[str]:
    """country_rules: single `tags` JSONB (legacy tagged* arrays merged at load)."""
    rules = root.get("rules")
    if not isinstance(rules, list):
        rules = []
    rows_in = [x for x in rules if isinstance(x, dict)]
    tbl_sql = canonical_table_for_array("countryRules.json", "rules")
    sch_q = quote_ident(schema)
    cr = quote_ident(tbl_sql)

    if not rows_in:
        cur.execute(
            f"""CREATE TABLE {sch_q}.{cr} (
                {quote_ident("id")} UUID PRIMARY KEY,
                {quote_ident("tags")} JSONB DEFAULT '[]'::jsonb,
                {quote_ident("_note")} TEXT DEFAULT 'empty seed'
            )"""
        )
        cur.execute(
            f"""INSERT INTO {sch_q}.{cr} ({quote_ident("id")}, {quote_ident("tags")})
                VALUES (%s, %s)""",
            (str(new_uuid7()), Json([])),
        )
        return [tbl_sql]

    stripped = [_collapse_rule_row_to_tags(dict(r)) for r in rows_in]
    upload_object_array(
        cur,
        schema,
        stripped,
        Json,
        table=tbl_sql,
        extra_sql_columns={"tags": "JSONB"},
        report_ctx=(
            tbl_sql,
            "countryRules.json",
            {
                "taggedObjects": "merged into tags JSONB; not a separate column",
                "taggedColumns": "merged into tags JSONB; not a separate column",
                "taggedFunctions": "merged into tags JSONB; not a separate column",
            },
        ),
    )
    return [tbl_sql]


def apply_readmes_foreign_keys(cur: Any, schema: str) -> None:
    """Optional legacy FKs on data_models.readme_id / requirements_id if those columns exist."""
    if not _table_exists(cur, schema, "readmes"):
        return
    sch_q = quote_ident(schema)
    rq = quote_ident("readmes")
    dm = quote_ident("data_models")
    dm_cols = _column_names_lower(cur, schema, "data_models")
    if "readme_id" in dm_cols:
        cur.execute(
            f"ALTER TABLE {sch_q}.{dm} DROP CONSTRAINT IF EXISTS {quote_ident('fk_data_models_readme_id')}"
        )
        cur.execute(
            f"""ALTER TABLE {sch_q}.{dm}
            ADD CONSTRAINT {quote_ident('fk_data_models_readme_id')}
            FOREIGN KEY ({quote_ident('readme_id')})
            REFERENCES {sch_q}.{rq} ({quote_ident('id')})
            ON DELETE SET NULL"""
        )
    if "requirements_id" in dm_cols:
        cur.execute(
            f"ALTER TABLE {sch_q}.{dm} DROP CONSTRAINT IF EXISTS {quote_ident('fk_data_models_requirements_id')}"
        )
        cur.execute(
            f"""ALTER TABLE {sch_q}.{dm}
            ADD CONSTRAINT {quote_ident('fk_data_models_requirements_id')}
            FOREIGN KEY ({quote_ident('requirements_id')})
            REFERENCES {sch_q}.{rq} ({quote_ident('id')})
            ON DELETE SET NULL"""
        )


def _resolve_standard_uuid_for_upload(
    cur: Any, schema: str, ref: str
) -> Optional[str]:
    ref = (ref or "").strip()
    if not ref:
        return None
    sch_q = quote_ident(schema)
    ds_tbl = quote_ident("data_standards")
    try:
        UUID(ref)
        cur.execute(
            f"SELECT id::text FROM {sch_q}.{ds_tbl} WHERE id = %s::uuid LIMIT 1",
            (ref,),
        )
        row = cur.fetchone()
        if row:
            return str(row[0])
    except ValueError:
        pass
    got = STANDARD_SEED_ID_TO_UUID.get(ref)
    return got


def _set_agreement_standards_id(
    cur: Any, schema: str, agreement_pk: str, uuids: List[str]
) -> None:
    sch_q = quote_ident(schema)
    pa_tbl = quote_ident("product_agreements")
    sic = quote_ident("standards_id")
    if not uuids:
        cur.execute(
            f"UPDATE {sch_q}.{pa_tbl} SET {sic} = NULL WHERE id = %s::uuid",
            (agreement_pk,),
        )
    else:
        cur.execute(
            f"UPDATE {sch_q}.{pa_tbl} SET {sic} = %s::uuid[] WHERE id = %s::uuid",
            (uuids, agreement_pk),
        )


def apply_product_agreement_standards_id(cur: Any, schema: str) -> None:
    """Many standards per agreement: product_agreements.standards_id UUID[].

    Each array element is a data_standards.id (logical FK; Postgres has no FK on array items).
    Drops legacy junction table and data_standards.catalog_key when present.
    """
    if not _table_exists(cur, schema, "product_agreements"):
        return
    sch_q = quote_ident(schema)
    pa_tbl = quote_ident("product_agreements")

    cur.execute(
        f"DROP TABLE IF EXISTS {sch_q}.{quote_ident('product_agreement_data_standards')}"
    )

    if _table_exists(cur, schema, "data_standards"):
        ds_cols = _column_names_lower(cur, schema, "data_standards")
        if "catalog_key" in ds_cols:
            cur.execute(
                f"DROP INDEX IF EXISTS {sch_q}.{quote_ident('uq_data_standards_catalog_key')}"
            )
            cur.execute(
                f"ALTER TABLE {sch_q}.{quote_ident('data_standards')} "
                f"DROP COLUMN {quote_ident('catalog_key')}"
            )

    cur.execute(
        f"""ALTER TABLE {sch_q}.{pa_tbl}
        ADD COLUMN IF NOT EXISTS {quote_ident("standards_id")} UUID[]"""
    )
    ensure_product_agreements_standards_id_uuid_array(cur, schema)
    ensure_product_agreements_system_columns(cur, schema)

    pa_cols = _column_names_lower(cur, schema, "product_agreements")
    if "data_policies" in pa_cols:
        dpc = quote_ident("data_policies")
        cur.execute(f"SELECT id::text, {dpc} FROM {sch_q}.{pa_tbl}")
        for aid, dp in cur.fetchall():
            if not isinstance(dp, list):
                continue
            uuids: List[str] = []
            for ref in dp:
                if ref is None:
                    continue
                sid = _resolve_standard_uuid_for_upload(cur, schema, str(ref))
                if sid:
                    uuids.append(sid)
            _set_agreement_standards_id(cur, schema, aid, uuids)
        cur.execute(
            f"ALTER TABLE {sch_q}.{pa_tbl} DROP COLUMN IF EXISTS {dpc}"
        )

    for ext_key, refs in _AGREEMENT_STANDARD_REFS:
        pa_uuid = _AGREEMENT_ID_BY_EXTERNAL.get(ext_key)
        if not pa_uuid:
            continue
        uuids = []
        for ref in refs:
            if ref is None:
                continue
            sid = _resolve_standard_uuid_for_upload(cur, schema, str(ref))
            if sid:
                uuids.append(sid)
        _set_agreement_standards_id(cur, schema, pa_uuid, uuids)

    if "data_standards" in _column_names_lower(cur, schema, "product_agreements"):
        cur.execute(
            f"ALTER TABLE {sch_q}.{pa_tbl} DROP COLUMN IF EXISTS "
            f"{quote_ident('data_standards')}"
        )


def apply_relationship_foreign_keys(cur: Any, schema: str) -> None:
    """Add UUID FK columns and constraints after all tables exist (load order independent)."""
    sch = quote_ident(schema)
    dm = quote_ident("data_models")
    pa = quote_ident("product_agreements")

    if not (
        _table_exists(cur, schema, "data_models")
        and _column_names_lower(cur, schema, "data_models") >= {"short_name", "id"}
    ):
        return

    cur.execute(
        f"""CREATE UNIQUE INDEX IF NOT EXISTS {quote_ident("uq_data_models_short_name")}
            ON {sch}.{dm} ({quote_ident("short_name")})
            WHERE {quote_ident("short_name")} IS NOT NULL"""
    )

    if _table_exists(cur, schema, "product_agreements") and _column_names_lower(
        cur, schema, "product_agreements"
    ) >= {"model_short_name", "id"}:
        cur.execute(
            f"""ALTER TABLE {sch}.{pa}
            ADD COLUMN IF NOT EXISTS {quote_ident("data_model_id")} UUID"""
        )
        cur.execute(
            f"""UPDATE {sch}.{pa} AS pa SET {quote_ident("data_model_id")} = m.{quote_ident("id")}
            FROM {sch}.{dm} AS m
            WHERE NULLIF(TRIM(pa.{quote_ident("model_short_name")}), '') IS NOT NULL
              AND pa.{quote_ident("model_short_name")} = m.{quote_ident("short_name")}"""
        )
        cur.execute(
            f"""ALTER TABLE {sch}.{pa}
            DROP CONSTRAINT IF EXISTS {quote_ident("fk_product_agreements_data_model")}"""
        )
        cur.execute(
            f"""ALTER TABLE {sch}.{pa}
            ADD CONSTRAINT {quote_ident("fk_product_agreements_data_model")}
            FOREIGN KEY ({quote_ident("data_model_id")})
            REFERENCES {sch}.{dm} ({quote_ident("id")})
            ON DELETE SET NULL"""
        )
        cur.execute(
            f"""CREATE INDEX IF NOT EXISTS {quote_ident("idx_product_agreements_data_model_id")}
            ON {sch}.{pa} ({quote_ident("data_model_id")})"""
        )


def process_dh_feedback(cur: Any, schema: str) -> List[str]:
    """Create dh_feedback (and index / optional FK to dh_users) after other tables exist."""
    ensure_dh_feedback_table(cur, schema)
    return [DH_FEEDBACK_TABLE]


def process_statistics(cur: Any, schema: str, data: dict, Json: Any) -> List[str]:
    created: List[str] = []
    pv = data.get("pageViews")
    if isinstance(pv, dict):
        cur.execute(
            f"""CREATE TABLE {quote_ident(schema)}.{quote_ident("page_views")} (
                {quote_ident("id")} UUID PRIMARY KEY,
                {quote_ident("page_name")} TEXT NOT NULL,
                {quote_ident("view_date")} DATE NOT NULL,
                {quote_ident("view_count")} BIGINT NOT NULL,
                UNIQUE ({quote_ident("page_name")}, {quote_ident("view_date")})
            )"""
        )
        cur.execute(
            f"""CREATE INDEX {quote_ident("page_views_page_name_idx")}
                ON {quote_ident(schema)}.{quote_ident("page_views")} ({quote_ident("page_name")})"""
        )
        for page_name, block in pv.items():
            if not isinstance(block, dict):
                continue
            daily = block.get("daily") or {}
            if not isinstance(daily, dict):
                continue
            for date_str, count in sorted(daily.items()):
                if count is None:
                    continue
                try:
                    views = int(count)
                except (TypeError, ValueError):
                    continue
                cur.execute(
                    f"""INSERT INTO {quote_ident(schema)}.{quote_ident("page_views")}
                        ({quote_ident("id")}, {quote_ident("page_name")}, {quote_ident("view_date")}, {quote_ident("view_count")})
                        VALUES (%s, %s, %s::date, %s)
                        ON CONFLICT ({quote_ident("page_name")}, {quote_ident("view_date")}) DO UPDATE SET
                        {quote_ident("view_count")} = EXCLUDED.{quote_ident("view_count")}""",
                    (str(new_uuid7()), page_name, str(date_str), views),
                )
        created.append("page_views")

    sv = data.get("siteVisits")
    if isinstance(sv, dict):
        cur.execute(
            f"""CREATE TABLE {quote_ident(schema)}.{quote_ident("site_visits")} (
                {quote_ident("id")} UUID PRIMARY KEY,
                {quote_ident("visit_date")} DATE NOT NULL UNIQUE,
                {quote_ident("visit_count")} BIGINT NOT NULL
            )"""
        )
        daily = sv.get("daily") or {}
        if isinstance(daily, dict):
            for date_str, count in sorted(daily.items()):
                if count is None:
                    continue
                try:
                    visits = int(count)
                except (TypeError, ValueError):
                    continue
                cur.execute(
                    f"""INSERT INTO {quote_ident(schema)}.{quote_ident("site_visits")}
                        ({quote_ident("id")}, {quote_ident("visit_date")}, {quote_ident("visit_count")})
                        VALUES (%s, %s::date, %s)
                        ON CONFLICT ({quote_ident("visit_date")}) DO UPDATE SET
                        {quote_ident("visit_count")} = EXCLUDED.{quote_ident("visit_count")}""",
                    (str(new_uuid7()), str(date_str), visits),
                )
        created.append("site_visits")

    cur.execute(
        f"""CREATE TABLE {quote_ident(schema)}.{quote_ident("usage_summary")} (
            {quote_ident("id")} UUID PRIMARY KEY,
            {quote_ident("total_views")} BIGINT,
            {quote_ident("last_updated")} TEXT
        )"""
    )
    cur.execute(
        f"""INSERT INTO {quote_ident(schema)}.{quote_ident("usage_summary")}
            ({quote_ident("id")}, {quote_ident("total_views")}, {quote_ident("last_updated")})
            VALUES (%s, %s, %s)""",
        (str(new_uuid7()), data.get("totalViews"), data.get("lastUpdated")),
    )
    created.append("usage_summary")
    record_statistics_json_drops("statistics.json", data)
    return created


def process_generic_root_dict(
    cur: Any,
    schema: str,
    filename: str,
    root: dict,
    Json: Any,
) -> List[str]:
    created: List[str] = []
    for key, val in root.items():
        if not isinstance(val, list):
            continue
        tbl_sql = canonical_table_for_array(filename, key)
        rows = [x for x in val if isinstance(x, dict)]
        if not rows:
            cur.execute(
                f"CREATE TABLE {quote_ident(schema)}.{quote_ident(tbl_sql)} (\n"
                f"    {quote_ident('id')} UUID PRIMARY KEY,\n"
                f"    {quote_ident('_note')} TEXT DEFAULT 'empty seed'\n)"
            )
            cur.execute(
                f"INSERT INTO {quote_ident(schema)}.{quote_ident(tbl_sql)} ({quote_ident('id')}) "
                f"VALUES (%s)",
                (str(new_uuid7()),),
            )
            created.append(tbl_sql)
            continue
        row_ids = None
        if filename == "users.json":
            row_ids = [_uuid_or_new_uuid7(r.get("id")) for r in rows]
        tbl, _ = upload_object_array(
            cur,
            schema,
            rows,
            Json,
            table=tbl_sql,
            report_ctx=(tbl_sql, filename, None),
            row_ids=row_ids,
        )
        created.append(tbl)
    return created


def process_datasets(cur: Any, schema: str, data: dict, Json: Any) -> List[str]:
    """catalog_datasets: scalar fields + tables[] as JSONB; JSON id duplicated to dataset_slug."""
    key = "datasets"
    if key not in data or not isinstance(data[key], list):
        return []
    tbl_sql = canonical_table_for_array("datasets.json", key)
    rows_raw = [x for x in data[key] if isinstance(x, dict)]
    rows: List[dict] = []
    for r in rows_raw:
        cp = dict(r)
        js_id = cp.get("id")
        if js_id is not None:
            cp["datasetSlug"] = js_id
        rows.append(cp)
    if not rows:
        cur.execute(
            f"CREATE TABLE {quote_ident(schema)}.{quote_ident(tbl_sql)} (\n"
            f"    {quote_ident('id')} UUID PRIMARY KEY,\n"
            f"    {quote_ident('_note')} TEXT DEFAULT 'empty seed'\n)"
        )
        cur.execute(
            f"INSERT INTO {quote_ident(schema)}.{quote_ident(tbl_sql)} ({quote_ident('id')}) "
            f"VALUES (%s)",
            (str(new_uuid7()),),
        )
        return [tbl_sql]
    tbl, _ = upload_object_array(
        cur,
        schema,
        rows,
        Json,
        table=tbl_sql,
        report_ctx=(tbl_sql, "datasets.json", None),
    )
    return [tbl]


def process_file(cur: Any, schema: str, path: Path, data: Any, Json: Any) -> List[str]:
    if path.name == "toolkit.json" and isinstance(data, dict) and "toolkit" in data:
        return process_toolkit(cur, schema, data, Json)
    if path.name == "dataAgreements.json" and isinstance(data, dict):
        return process_data_agreements(cur, schema, data, Json)
    if path.name == "dataPolicies.json" and isinstance(data, dict):
        return process_data_standards(cur, schema, data, Json)
    if path.name == "dataModels.json" and isinstance(data, dict):
        return process_data_models(cur, schema, data, Json)
    if path.name == "glossary.json" and isinstance(data, dict):
        return process_glossary(cur, schema, data, Json)
    if path.name == "rules.json" and isinstance(data, dict):
        return process_model_rules(cur, schema, data, Json)
    if path.name == "countryRules.json" and isinstance(data, dict):
        return process_country_rules(cur, schema, data, Json)
    if path.name == "statistics.json" and isinstance(data, dict):
        return process_statistics(cur, schema, data, Json)
    if path.name == "dataDomains.json" and isinstance(data, dict):
        return process_data_domains(cur, schema, data, Json)
    if path.name == "datasets.json" and isinstance(data, dict):
        return process_datasets(cur, schema, data, Json)
    if isinstance(data, dict):
        return process_generic_root_dict(cur, schema, path.name, data, Json)
    if isinstance(data, list) and data and all(isinstance(x, dict) for x in data):
        sys.exit(
            f"Root-level JSON arrays need a mapped file name and key: {path.name!r}"
        )
    sys.stderr.write(f"Skip (unsupported root type): {path.name}\n")
    return []


def _reject_local_postgres_targets(conn_str: str) -> None:
    """Block localhost — this uploader is intended for RDS (or other remote) only."""
    s = conn_str.strip().lower().replace("+srv", "")
    if "host=localhost" in s or "host=127.0.0.1" in s or "host=::1" in s:
        sys.exit(
            "upload_data_to_postgres targets Amazon RDS only; localhost/loopback hosts are not allowed."
        )
    if "//localhost" in s or "//127.0.0.1" in s or "//[::1]" in s:
        sys.exit(
            "upload_data_to_postgres targets Amazon RDS only; localhost/loopback hosts are not allowed."
        )


def build_conn_str(args: argparse.Namespace) -> str:
    """Resolve libpq connection string. URL env vars beat discrete host/port flags."""
    if args.database_url:
        out = args.database_url
        _reject_local_postgres_targets(out)
        return out
    env_url = (ENV_DATABASE_URL or "").strip() or (ENV_RDS_DATABASE_URL or "").strip()
    if env_url:
        _reject_local_postgres_targets(env_url)
        return env_url
    host = (args.host or "").strip()
    if not host:
        sys.exit(
            "No DATABASE_URL or RDS_DATABASE_URL, and PGHOST is empty. "
            "Set api/.env for your RDS instance (see env.example)."
        )
    parts = [
        f"host={host}",
        f"port={args.port}",
        f"dbname={args.dbname}",
        f"user={args.user}",
    ]
    if args.password:
        parts.append(f"password={args.password}")
    sslmode = (getattr(args, "sslmode", None) or "").strip()
    if sslmode:
        parts.append(f"sslmode={sslmode}")
    out = " ".join(parts)
    _reject_local_postgres_targets(out)
    return out


def load_json_files(data_dir: Path) -> List[Tuple[Path, Any]]:
    if not data_dir.is_dir():
        raise SystemExit(f"Data directory not found: {data_dir}")
    out: List[Tuple[Path, Any]] = []
    for path in sorted(data_dir.glob("*.json")):
        with open(path, encoding="utf-8") as f:
            out.append((path, json.load(f)))
    return out


def sort_upload_files(files: List[Tuple[Path, Any]]) -> List[Tuple[Path, Any]]:
    """Load dataPolicies before dataAgreements so seed standard id → UUID map exists."""

    def key(item: Tuple[Path, Any]) -> Tuple[int, str]:
        name = item[0].name
        prio = {"dataPolicies.json": 5, "dataAgreements.json": 40}.get(name, 100)
        return (prio, name)

    return sorted(files, key=key)


def _clear_agreement_standard_upload_buffers() -> None:
    STANDARD_SEED_ID_TO_UUID.clear()
    _AGREEMENT_STANDARD_REFS.clear()
    _AGREEMENT_ID_BY_EXTERNAL.clear()


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    _parent = script_dir.parent
    default_data = _parent / "____data" if (_parent / "____data").is_dir() else _parent / "_data"

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=default_data)
    parser.add_argument(
        "--schema",
        default=SCHEMA,
        help=f"Postgres schema for tables (default: {SCHEMA})",
    )
    parser.add_argument(
        "--database-url",
        default=None,
        help="Full postgres URL (overrides DATABASE_URL / RDS_DATABASE_URL). "
        "For RDS public endpoints use e.g. postgresql://user:pass@host:5432/db?sslmode=require",
    )
    parser.add_argument(
        "--host",
        default=ENV_PGHOST or None,
        help="RDS endpoint hostname (only if DATABASE_URL / RDS_DATABASE_URL are unset)",
    )
    parser.add_argument(
        "--port",
        default=ENV_PGPORT,
        help="DB port (RDS often 5432)",
    )
    parser.add_argument("--dbname", default=ENV_PGDATABASE)
    parser.add_argument("--user", default=ENV_PGUSER)
    parser.add_argument("--password", default=ENV_PGPASSWORD)
    parser.add_argument(
        "--sslmode",
        default=ENV_PGSSLMODE,
        metavar="MODE",
        help="Libpq sslmode when using host/port (not a URL). Default require (RDS). "
        "Also settable via PGSSLMODE.",
    )
    parser.add_argument(
        "--init-schema",
        action="store_true",
        help=f"DROP SCHEMA IF EXISTS ... CASCADE then CREATE SCHEMA for {SCHEMA}",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    UPLOAD_JSON_KEY_REPORT.clear()
    _clear_agreement_standard_upload_buffers()

    schema = assert_safe_ident(args.schema)
    psycopg, Json = require_psycopg()
    files = sort_upload_files(load_json_files(args.data_dir))

    if args.dry_run:
        print(f"Would load {len(files)} file(s) into schema {schema!r}")
        for path, data in files:
            if path.name == "toolkit.json":
                print(
                    f"  {path.name} -> readmes, {TOOLKIT_TABLE}, "
                    f"{TOOLKIT_TECH_TABLE} (*_readme_id FKs, pros/cons JSONB)"
                )
            elif path.name == "dataAgreements.json":
                print(
                    f"  {path.name} -> product_agreements, {RELEASE_NOTES_TABLE}, "
                    f"{VERSION_HISTORY_TABLE} (entity_type={ENTITY_TYPE_PRODUCT_AGREEMENT!r})"
                )
            elif path.name == "dataModels.json":
                print(
                    f"  {path.name} -> readmes + data_models (readme_refs JSONB list), "
                    f"resources JSONB, {RELEASE_NOTES_TABLE}, "
                    f"{VERSION_HISTORY_TABLE} ({ENTITY_TYPE_DATA_MODEL!r})"
                )
            elif path.name == "glossary.json":
                print(f"  {path.name} -> readmes + glossary_terms (markdown_refs JSONB)")
            elif path.name == "statistics.json":
                print(
                    f"  {path.name} -> page_views (per page/day), site_visits, usage_summary"
                )
            elif path.name == "rules.json":
                print(
                    f"  {path.name} -> model_rules "
                    f"(data_model_id, tags JSONB; tagged* merged into tags)"
                )
            elif path.name == "countryRules.json":
                print(
                    f"  {path.name} -> country_rules "
                    f"(tags JSONB; tagged* merged into tags)"
                )
            elif path.name == "dataPolicies.json":
                print(
                    f"  {path.name} -> {DATA_STANDARDS_TABLE} "
                    f"(standard_name, status, description, owner, links, tags)"
                )
            elif path.name == "dataDomains.json":
                print(
                    f"  {path.name} -> data_domains, {SUB_DOMAINS_TABLE} "
                    f"(FK data_domain_id)"
                )
            elif path.name == "datasets.json":
                print(f"  {path.name} -> catalog_datasets (dataset_slug, tables JSONB)")
            elif isinstance(data, dict) and path.name != "toolkit.json":
                keys = [k for k, v in data.items() if isinstance(v, list)]
                m = _FILE_TABLE_MAP.get(path.name, {})
                print(
                    f"  {path.name} -> tables: "
                    f"{[canonical_table_for_array(path.name, k) for k in keys]}"
                )
        print(
            "  (post-load) -> dh_feedback "
            "(user_id, feedback_text, created_at; GET/POST /api/feedback)"
        )
        return

    conn_str = build_conn_str(args)
    all_tables: List[str] = []

    try:
        with psycopg.connect(conn_str) as conn:
            with conn.cursor() as cur:
                if args.init_schema:
                    cur.execute(
                        f"DROP SCHEMA IF EXISTS {quote_ident(schema)} CASCADE"
                    )
                    cur.execute(f"CREATE SCHEMA {quote_ident(schema)}")
                else:
                    cur.execute(
                        f"CREATE SCHEMA IF NOT EXISTS {quote_ident(schema)}"
                    )

                for path, data in files:
                    tables = process_file(cur, schema, path, data, Json)
                    all_tables.extend(tables)
                    print(f"{path.name}: {len(tables)} table(s) -> {tables}")

                apply_product_agreement_standards_id(cur, schema)
                apply_relationship_foreign_keys(cur, schema)
                apply_readmes_foreign_keys(cur, schema)

                fb_tables = process_dh_feedback(cur, schema)
                all_tables.extend(fb_tables)
                print(
                    f"dh_feedback: {len(fb_tables)} table(s) -> {fb_tables}"
                )

            conn.commit()

    except psycopg.OperationalError as e:
        err = str(e).lower()
        print(f"PostgreSQL connection failed:\n{e}", file=sys.stderr)
        if "timeout" in err or "timed out" in err:
            print(
                "\nConnection timed out — traffic is not reaching PostgreSQL on port 5432. "
                "For Amazon RDS from your laptop, check:\n"
                "  • RDS → Connectivity & security → Public access = Yes (if you are not on a VPC peer/VPN).\n"
                "  • RDS security group → Inbound: PostgreSQL (5432) from your current public IP/32 "
                "(AWS console: update rule when your IP changes) or from your VPN CIDR.\n"
                "  • Instance must be in a public subnet with a route to an Internet Gateway "
                "(if using a public endpoint).\n"
                "  • Test reachability: nc -vz <endpoint-or-ip> 5432\n",
                file=sys.stderr,
            )
        sys.exit(1)

    print(f"Done. Created/loaded {len(all_tables)} table(s) in schema {schema!r}.")
    print_json_key_report()


if __name__ == "__main__":
    main()
