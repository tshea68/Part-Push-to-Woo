import os, sys, math, time, hashlib
import requests
import psycopg
from psycopg.rows import dict_row

WOO_BASE = os.environ["WOO_BASE"].rstrip("/")  # e.g. https://appgeeks.../wp-json/wc/v3
AUTH = (os.environ["WOO_CK"], os.environ["WOO_CS"])
PG_DSN = os.environ["PG_DSN"]

BATCH = 100

def sha(row):
    s = f"{row['mpn']}|{row.get('name','')}|{row.get('price') or ''}|{row.get('stock_qty') or 0}"
    return hashlib.sha1(s.encode()).hexdigest()

def fetch_parts(limit=None):
    sql = """
        SELECT
          mpn,
          COALESCE(name, mpn) AS name,
          ROUND(price::numeric, 2)::text AS price,
          COALESCE(stock_qty,0)::int AS stock_qty
        FROM parts
        WHERE price IS NOT NULL
        ORDER BY mpn
    """
    if limit and int(limit) > 0:
        sql += " LIMIT %s"
        params = (int(limit),)
    else:
        params = None

    with psycopg.connect(PG_DSN) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()

def sku_id_map():
    """Pull existing Woo products to map SKU -> product ID (paged)."""
    m, page = {}, 1
    while True:
        r = requests.get(f"{WOO_BASE}/products", params={"per_page":100, "page":page, "status":"any"}, auth=AUTH, timeout=60)
        if r.status_code == 429:
            time.sleep(2); continue
        r.raise_for_status()
        rows = r.json()
        if not rows: break
        for it in rows:
            sku = it.get("sku")
            if sku: m[sku] = it["id"]
        page += 1
    return m

def to_payload(row):
    return {
        "name": row["name"],
        "sku": row["mpn"],
        "regular_price": str(row["price"]),
        "manage_stock": True,
        "stock_quantity": int(row["stock_qty"]),
        "stock_status": "instock" if int(row["stock_qty"]) > 0 else "outofstock",
        # optional fields you can add later:
        # "tax_status": "taxable",
        # "categories": [{"id": 123}],
    }

def push_batch(body):
    """POST to Woo batch endpoint with simple retry on 429."""
    url = f"{WOO_BASE}/products/batch"
    for attempt in range(3):
        r = requests.post(url, auth=AUTH, json=body, timeout=120)
        if r.status_code == 429:
            time.sleep(2); continue
        if r.ok: return r.json()
        # fail fast for non-retryable
        try:
            msg = r.json()
        except Exception:
            msg = r.text
        raise RuntimeError(f"Woo batch error {r.status_code}: {msg}")
    raise RuntimeError("Woo batch failed after retries (429s)")

def run(limit=None, dry_run=False):
    parts = fetch_parts(limit)
    print(f"[sync] fetched {len(parts)} rows from Postgres")

    existing = sku_id_map()
    print(f"[sync] fetched {len(existing)} existing Woo products")

    create, update = [], []
    for row in parts:
        payload = to_payload(row)
        pid = existing.get(row["mpn"])
        if pid:
            payload["id"] = pid
            update.append(payload)
        else:
            create.append(payload)

    print(f"[plan] create={len(create)} update={len(update)} (batch size {BATCH})")

    if dry_run:
        print("[dry-run] not calling Woo. Sample create payload:", create[:2])
        print("[dry-run] sample update payload:", update[:2])
        return {"created": len(create), "updated": len(update), "dry_run": True}

    # Push creates
    created_total = 0
    for i in range(0, len(create), BATCH):
        chunk = {"create": create[i:i+BATCH]}
        resp = push_batch(chunk)
        created_total += len(resp.get("create", []))
        print(f"[create] {created_total}/{len(create)}")

    # Push updates
    updated_total = 0
    for i in range(0, len(update), BATCH):
        chunk = {"update": update[i:i+BATCH]}
        resp = push_batch(chunk)
        updated_total += len(resp.get("update", []))
        print(f"[update] {updated_total}/{len(update)}")

    summary = {"created": created_total, "updated": updated_total, "dry_run": False}
    print("[done]", summary)
    return summary

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=100, help="limit rows from Postgres")
    p.add_argument("--dry-run", action="store_true", help="plan only, do not call Woo")
    args = p.parse_args()
    try:
        run(limit=args.limit, dry_run=args.dry_run)
    except Exception as e:
        print("ERROR:", e)
        sys.exit(1)
