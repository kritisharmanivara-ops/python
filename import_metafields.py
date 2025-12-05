import requests
import json

# ================= CONFIG =================
STORE_B = "productimportexportsh.myshopify.com"
ACCESS_TOKEN_B = "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"
API_VERSION = "2024-07"
EXPORT_FILE = "metafields_export.json"
# ==========================================

url = f"https://{STORE_B}/admin/api/{API_VERSION}/graphql.json"

headers = {
    "X-Shopify-Access-Token": ACCESS_TOKEN_B,
    "Content-Type": "application/json"
}

# ------------------ GraphQL Queries ------------------

CHECK_QUERY = """
query($namespace: String!, $key: String!) {
  metafieldDefinitions(namespace: $namespace, key: $key, ownerType: PRODUCT, first: 1) {
    edges { node { id } }
  }
}
"""

CREATE_MUTATION = """
mutation CreateDefinition($name: String!, $namespace: String!, $key: String!, $type: String!) {
  metafieldDefinitionCreate(definition: {
    name: $name,
    namespace: $namespace,
    key: $key,
    type: $type,
    ownerType: PRODUCT
  }) {
    createdDefinition { id }
    userErrors { field message }
  }
}
"""

# ------------------ Functions ------------------

def metafield_exists(namespace, key):
    payload = {
        "query": CHECK_QUERY,
        "variables": {"namespace": namespace, "key": key}
    }

    resp = requests.post(url, headers=headers, json=payload)
    data = resp.json()

    try:
        edges = data["data"]["metafieldDefinitions"]["edges"]
        return len(edges) > 0
    except:
        print("[WARN] Unexpected response:", data)
        return False


def create_metafield(m):
    node = m["node"]

    payload = {
        "query": CREATE_MUTATION,
        "variables": {
            "name": node["name"],
            "namespace": node["namespace"],
            "key": node["key"],
            "type": node["type"]["name"]
        }
    }

    resp = requests.post(url, headers=headers, json=payload)
    data = resp.json()

    errors = data["data"]["metafieldDefinitionCreate"]["userErrors"]
    if errors:
        print(f"[ERROR] {node['namespace']}.{node['key']} → {errors}")
    else:
        print(f"[OK] Created metafield: {node['namespace']}.{node['key']}")


def main():
    with open(EXPORT_FILE, "r", encoding="utf-8") as f:
        metafields = json.load(f)

    print(f"[INFO] Importing {len(metafields)} metafields to Store B...")

    for m in metafields:
        node = m["node"]
        namespace = node["namespace"]
        key = node["key"]

        if not metafield_exists(namespace, key):
            create_metafield(m)
        else:
            print(f"[EXISTS] {namespace}.{key}")


if __name__ == "__main__":
    main()
