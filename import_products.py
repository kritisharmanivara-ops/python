import requests
import json
import time
import re
from urllib.parse import urlparse

# ================== CONFIG ==================
STORE_DOMAIN = "productimportexportsh.myshopify.com"
ACCESS_TOKEN = "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"
API_VERSION = "2024-07"
EXPORT_FILE = "products_export.json"

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
SLEEP_ON_429 = 2
# ============================================

session = requests.Session()

# ------------------ HELPERS ------------------

def log(*args, **kwargs):
    print(*args, **kwargs)

def safe_json(resp):
    try:
        return resp.json()
    except Exception:
        return {"__raw_text": getattr(resp, "text", "<no-text>")}

def retry_request(func, *args, **kwargs):
    for attempt in range(1, MAX_RETRIES + 1):
        resp = func(*args, **kwargs)
        if resp is None:
            time.sleep(1)
            continue
        code = getattr(resp, "status_code", None)
        if code == 429:
            wait = int(resp.headers.get("Retry-After", SLEEP_ON_429))
            log(f"[RATE] 429. Sleeping {wait}s attempt {attempt}/{MAX_RETRIES}")
            time.sleep(wait)
            continue
        return resp
    return resp

def build_rest_url(endpoint):
    return f"https://{STORE_DOMAIN}/admin/api/{API_VERSION}/{endpoint}.json"

def build_graphql_url():
    return f"https://{STORE_DOMAIN}/admin/api/{API_VERSION}/graphql.json"

def rest_request(method, endpoint, payload=None, params=None):
    url = build_rest_url(endpoint)
    headers = {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    data = json.dumps(payload) if payload is not None else None

    def call():
        return session.request(method, url, headers=headers, params=params, data=data, timeout=REQUEST_TIMEOUT)

    resp = retry_request(call)
    if not resp:
        raise RuntimeError("No response from Shopify.")

    if not resp.ok and resp.status_code != 404:
        log(f"[ERROR] REST {method} {url} -> {resp.status_code}")
        snippet = getattr(resp, "text", "")[:1000]
        log(snippet)

    return safe_json(resp)

def graphql_request(query, variables=None):
    url = build_graphql_url()
    headers = {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    payload = {"query": query}
    if variables is not None:
        payload["variables"] = variables

    def call():
        return session.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)

    resp = retry_request(call)
    if not resp:
        raise RuntimeError("No GraphQL response.")

    if resp.status_code == 429:
        time.sleep(SLEEP_ON_429)

    return safe_json(resp)

# --------------- GID ---------------

GID_RE = re.compile(r"gid:\/\/shopify\/(?P<type>[^\/]+)\/(?P<id>\d+)")

def parse_maybe_gid_or_numeric(value):
    """
    Returns (type_string_or_None, numeric_id_or_None)
    """
    if value is None:
        return None, None
    if isinstance(value, int):
        return None, int(value)
    if isinstance(value, str):
        v = value.strip()
        m = GID_RE.match(v)
        if m:
            return m.group("type"), int(m.group("id"))
        if v.isdigit():
            return None, int(v)
    return None, None

def gid_for_product(id_):
    return f"gid://shopify/Product/{id_}"

def gid_for_media(id_):
    return f"gid://shopify/MediaImage/{id_}"

# --------------- CLEANERS ---------------

def clean_variant(v):
    v = v.copy()
    blacklist = ["id","product_id","admin_graphql_api_id","created_at","updated_at","position","inventory_item_id","old_inventory_quantity"]
    for f in blacklist:
        v.pop(f, None)
    return v

def clean_product(product):
    p = product.copy()
    kill = ["id","admin_graphql_api_id","created_at","updated_at","published_at","published_scope"]
    for f in kill:
        p.pop(f, None)

    if "variants" in p and isinstance(p["variants"], list):
        p["variants"] = [clean_variant(v) for v in p["variants"]]

    if "images" in p and isinstance(p["images"], list):
        p["images"] = [{"src": i.get("src")} for i in p["images"] if i.get("src")]

    p.pop("metafields", None)
    return p

# --------------- METAFIELD DEFINITIONS ---------------

CHECK_DEF = """
query($namespace: String!, $key: String!) {
  metafieldDefinitions(namespace: $namespace, key: $key, ownerType: PRODUCT, first: 1) {
    edges { node { id } }
  }
}
"""

CREATE_DEF = """
mutation metafieldDefinitionCreate($definition: MetafieldDefinitionInput!) {
  metafieldDefinitionCreate(definition: $definition) {
    createdDefinition { id }
    userErrors { field message }
  }
}
"""

def ensure_definition(namespace, key, type_name):
    # Normalize type_name before checking/creating
    type_name = normalize_type_name(type_name)
    resp = graphql_request(CHECK_DEF, {"namespace": namespace, "key": key})
    edges = resp.get("data",{}).get("metafieldDefinitions",{}).get("edges",[])
    if edges:
        return True

    definition = {
        "name": key,
        "namespace": namespace,
        "key": key,
        "type": type_name,
        "ownerType": "PRODUCT"
    }

    resp = graphql_request(CREATE_DEF, {"definition": definition})
    ue = resp.get("data",{}).get("metafieldDefinitionCreate",{}).get("userErrors",[])
    if ue:
        log("[ERR] definition",namespace,key, ue)
        return False

    log("[OK] Definition created", namespace, key)
    return True

# ------------- METAFIELD SET GRAPHQL -------------
META_SET = """
mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields { id namespace key }
    userErrors { field message }
  }
}
"""

def create_metafield_gql(owner_id_gid, namespace, key, type_name, value):
    """
    Creates (sets) a single metafield via GraphQL metafieldsSet.
    `value` should be string or list (will be JSON-dumped for lists)
    """
    if isinstance(value, list):
        value_for_api = json.dumps(value)
    else:
        # Some types require string; keep as-is
        value_for_api = str(value) if value is not None else ""

    variables = {
        "metafields": [
            {
                "ownerId": owner_id_gid,
                "namespace": namespace,
                "key": key,
                "type": normalize_type_name(type_name),
                "value": value_for_api
            }
        ]
    }

    resp = graphql_request(META_SET, variables)
    # Try to extract userErrors
    errors = resp.get("data",{}).get("metafieldsSet",{}).get("userErrors",[])
    if errors:
        log("[WARN] Meta fail", namespace, key, errors)
        return False

    return True

# --------------- TYPE NORMALIZATION ---------------

# Map common or legacy names to Shopify GraphQL metafield types
TYPE_MAP = {
    "string": "single_line_text_field",
    "text": "multi_line_text_field",
    "html": "rich_text_field",
    "int": "number_integer",
    "integer": "number_integer",
    "number": "number_decimal",
    "float": "number_decimal",
    "bool": "boolean",
    "boolean": "boolean",
    "image": "file_reference",
    "file": "file_reference",
    "product_list": "list.product_reference",
    "product_reference_list": "list.product_reference",
    "product_reference": "product_reference",
}

def normalize_type_name(t):
    if not t:
        return "single_line_text_field"
    t = str(t).strip()
    low = t.lower()
    if low in TYPE_MAP:
        return TYPE_MAP[low]
    # If already valid-ish, return as-is
    return t

# --------------- IMAGE UPLOAD HELPER ---------------

def upload_image_to_product(new_pid, src):
    """
    POST products/{new_pid}/images with {"image": {"src": src}}
    Returns admin_graphql_api_id (MediaImage GID) or None
    """
    r = rest_request("POST", f"products/{new_pid}/images", {"image": {"src": src}})
    ni = r.get("image")
    if not ni:
        return None
    new_img_id = ni.get("id")
    new_gid = ni.get("admin_graphql_api_id") or (gid_for_media(new_img_id) if new_img_id else None)
    return new_gid

# --------------- IMPORTER ---------------

def parse_raw_value(raw):
    """
    Accepts raw which may be:
    - list
    - JSON string representing list (e.g. '["a","b"]')
    - comma separated string "a,b,c"
    - single string
    Returns Python object: list or primitive string
    """
    if raw is None:
        return None
    if isinstance(raw, list):
        return raw
    if isinstance(raw, (int, float, bool)):
        return raw
    if isinstance(raw, str):
        s = raw.strip()
        # Try JSON decode
        try:
            parsed = json.loads(s)
            return parsed
        except Exception:
            pass
        # Comma separated? Only treat as list if it contains commas
        if "," in s:
            parts = [p.strip() for p in s.split(",") if p.strip()]
            return parts
        return s
    return raw

def import_products():
    with open(EXPORT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    log(f"[INFO] Loaded {len(data)} products from {EXPORT_FILE}")

    old2new = {}                       # old_product_id -> new_product_id
    img_old2new_media_gid = {}         # old_image_numeric_id -> new_media_gid

    # -------- 1: Create Products + Images ---------
    for idx, entry in enumerate(data, start=1):
        product_raw = entry.get("product", {})
        title = product_raw.get("title", "Untitled")
        old_pid = product_raw.get("id")
        cleaned = clean_product(product_raw)

        # Create product (REST)
        resp = rest_request("POST", "products", {"product": cleaned})
        created_product = resp.get("product")
        if not created_product:
            log(f"[ERR] Failed to create product '{title}': {resp}")
            continue

        new_pid = created_product["id"]
        old2new[old_pid] = new_pid
        log(f"[{idx}] Created product: '{title}' ({old_pid} -> {new_pid})")

        # Upload images from original product.images[].src and map ids
        for img in product_raw.get("images", []):
            old_img_id = img.get("id")
            src = img.get("src")
            if not src:
                continue
            new_media_gid = upload_image_to_product(new_pid, src)
            if not new_media_gid:
                log(f"  [WARN] Failed to upload image src {src} for product {new_pid}")
                continue
            # If the export had numeric image.id, map it
            if old_img_id:
                try:
                    img_old2new_media_gid[int(old_img_id)] = new_media_gid
                except Exception:
                    pass
            log(f"  [IMG] Uploaded image for product {new_pid}: old_img_id={old_img_id} -> {new_media_gid}")

    # -------- 2: Metafields ---------
    log("[INFO] Products and images created — starting metafield import pass...")

    for idx, entry in enumerate(data, start=1):
        product_raw = entry.get("product", {})
        metafields = entry.get("metafields", [])
        old_pid = product_raw.get("id")
        new_pid = old2new.get(old_pid)
        title = product_raw.get("title", "Untitled")

        if not new_pid:
            log(f"[SKIP] Product not created previously: old_pid={old_pid} ({title})")
            continue

        new_gid_prod = gid_for_product(new_pid)
        created_count = 0

        for m in metafields:
            namespace = m.get("namespace")
            key = m.get("key")
            raw_type = m.get("type") or m.get("value_type") or "single_line_text_field"
            type_name = normalize_type_name(raw_type)
            raw_value = m.get("value")

            if not namespace or not key:
                log(f"  [WARN] Skipping invalid metafield (missing namespace/key): {m}")
                continue

            # Ensure definition exists (attempt)
            ok_def = ensure_definition(namespace, key, type_name)
            if not ok_def:
                log(f"  [WARN] Could not ensure definition for {namespace}.{key}; skipping")
                continue

            parsed = parse_raw_value(raw_value)

            # Resolve according to type
            value_for_api = None

            # --- single file_reference
            if type_name == "file_reference":
                # parsed may be numeric id, gid string, or URL string
                t, numeric = parse_maybe_gid_or_numeric(parsed) if isinstance(parsed, (str, int)) else (None, None)
                if numeric and numeric in img_old2new_media_gid:
                    value_for_api = img_old2new_media_gid[numeric]
                elif isinstance(parsed, str) and parsed.startswith("http"):
                    # upload to product and use returned gid
                    new_media = upload_image_to_product(new_pid, parsed)
                    if new_media:
                        value_for_api = new_media
                    else:
                        log(f"    [WARN] file_reference: failed to upload URL {parsed} for {title}")
                        continue
                elif isinstance(parsed, str) and GID_RE.match(str(parsed)):
                    # if it's a gid, try to use mapping or keep as-is (but gid likely invalid across stores)
                    t2, num2 = parse_maybe_gid_or_numeric(parsed)
                    if num2 and num2 in img_old2new_media_gid:
                        value_for_api = img_old2new_media_gid[num2]
                    else:
                        # keep the gid (may work if same gid is valid — unlikely)
                        value_for_api = parsed
                else:
                    log(f"    [SKIP] file_reference {namespace}.{key} — can't map value {parsed}")
                    continue

            # --- list.file_reference or other list including file_reference
            elif type_name.startswith("list") and "file_reference" in type_name:
                items = parsed if isinstance(parsed, list) else []
                new_list = []
                for it in items:
                    tt, num = parse_maybe_gid_or_numeric(it) if isinstance(it, (str, int)) else (None, None)
                    if num and num in img_old2new_media_gid:
                        new_list.append(img_old2new_media_gid[num])
                    elif isinstance(it, str) and it.startswith("http"):
                        uploaded = upload_image_to_product(new_pid, it)
                        if uploaded:
                            new_list.append(uploaded)
                    elif isinstance(it, str) and GID_RE.match(it):
                        ttmp, ntmp = parse_maybe_gid_or_numeric(it)
                        if ntmp and ntmp in img_old2new_media_gid:
                            new_list.append(img_old2new_media_gid[ntmp])
                        else:
                            new_list.append(it)
                    else:
                        log(f"    [WARN] list.file item unrecognized: {it}")
                value_for_api = new_list

            # --- list.product_reference
            elif type_name == "list.product_reference":
                items = parsed if isinstance(parsed, list) else []
                new_prod_refs = []
                for it in items:
                    tt, num = parse_maybe_gid_or_numeric(it) if isinstance(it, (str, int)) else (None, None)
                    if num and num in old2new:
                        new_prod_refs.append(gid_for_product(old2new[num]))
                    elif isinstance(it, str) and GID_RE.match(it):
                        # extract numeric id and map
                        m = GID_RE.match(it)
                        if m:
                            old_num = int(m.group("id"))
                            if old_num in old2new:
                                new_prod_refs.append(gid_for_product(old2new[old_num]))
                            else:
                                log(f"    [WARN] product_reference item old product {old_num} not imported")
                    else:
                        log(f"    [WARN] list.product_reference item unrecognized: {it}")
                value_for_api = new_prod_refs

            # --- product_reference (single)
            elif type_name == "product_reference":
                tt, num = parse_maybe_gid_or_numeric(parsed) if isinstance(parsed, (str, int)) else (None, None)
                if num and num in old2new:
                    value_for_api = gid_for_product(old2new[num])
                elif isinstance(parsed, str) and GID_RE.match(parsed):
                    m = GID_RE.match(parsed)
                    if m:
                        old_num = int(m.group("id"))
                        if old_num in old2new:
                            value_for_api = gid_for_product(old2new[old_num])
                        else:
                            log(f"    [WARN] product_reference old product {old_num} not imported")
                            continue
                else:
                    log(f"    [WARN] product_reference value unrecognized: {parsed}")
                    continue

            # --- numeric / boolean / json / single-line text: pass-through
            else:
                # For lists or complex values that are not special types, keep parsed value
                value_for_api = parsed

            # If still None (not set), skip
            if value_for_api is None:
                log(f"    [SKIP] Could not build value for {namespace}.{key} on '{title}'")
                continue

            # Create/set metafield via GraphQL
            ok = create_metafield_gql(new_gid_prod, namespace, key, type_name, value_for_api)
            if ok:
                created_count += 1
            else:
                log(f"    [WARN] Failed to create metafield {namespace}.{key} for product {new_pid}")

        log(f"[{idx}] Added {created_count} metafields to {title} (new_id={new_pid})")

    log("[DONE] Import finished.")

if __name__ == "__main__":
    import_products()

