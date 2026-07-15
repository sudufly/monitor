import os
import json
import requests

def getenv(k, d=None):
    v = os.environ.get(k)
    return v if v is not None and v != "" else d

def extract_list(payload, page_size):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ["data", "records", "list", "items", "rows", "result"]:
            v = payload.get(key)
            if isinstance(v, list):
                return v
        for v in payload.values():
            if isinstance(v, list):
                return v
    return None

def get_access_token(session, token_url, client_id, client_secret, scope):
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }
    if scope:
        data["scope"] = scope
    r = session.post(token_url, data=data, headers={"Accept": "application/json"}, timeout=20)
    if r.status_code < 200 or r.status_code >= 300:
        raise RuntimeError(f"token http {r.status_code} {r.text}")
    j = r.json()
    token = j.get("access_token")
    if not token:
        raise RuntimeError(f"no access_token in response: {j}")
    return token

def main():
    token_url = getenv("TOKEN_URL", "https://id.dothework.cn/sso/tn-a1f042466b134f2ab3821fc23821757c/ai-c22b961087114db2940007587d47e440/oidc/token")
    data_base = getenv("DATA_URL_BASE", "https://wedata-tcs-data-service-gateway.dothework.cn/api/v1/QueryAllDevcieData")
    client_id = getenv("CLIENT_ID")
    client_secret = getenv("CLIENT_SECRET")
    scope = getenv("SCOPE", "")
    page_size = int(getenv("PAGE_SIZE", "1000"))
    max_pages = int(getenv("MAX_PAGES", "10000"))
    output_file = getenv("OUTPUT_FILE", "device-data.jsonl")

    if not client_id or not client_secret:
        raise RuntimeError("set CLIENT_ID and CLIENT_SECRET env vars")

    session = requests.Session()
    token = get_access_token(session, token_url, client_id, client_secret, scope)

    if os.path.exists(output_file):
        os.remove(output_file)

    for page in range(1, max_pages + 1):
        params = {"pageNum": page, "pageSize": page_size}
        headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
        r = session.get(data_base, params=params, headers=headers, timeout=30)
        sc = r.status_code
        if sc == 401 or sc == 403:
            raise RuntimeError(f"unauthorized http {sc} {r.text}")
        if sc < 200 or sc >= 300:
            break
        body_text = r.text
        with open(output_file, "a", encoding="utf-8") as f:
            f.write(body_text)
            f.write("\n")
        try:
            payload = r.json()
        except Exception:
            if len(body_text) < 40:
                break
            continue
        items = extract_list(payload, page_size)
        if items is not None:
            if len(items) == 0 or len(items) < page_size:
                break

if __name__ == "__main__":
    main()