"""
用GitHub API直接推送所有輸出檔案，不依賴git push
"""
import requests, base64, os, sys

TOKEN = os.environ.get("GH_TOKEN", "")
REPO = os.environ.get("GH_REPO", "Z3X1/SideProject_WhaleTracker")
HEADERS = {"Authorization": f"token {TOKEN}", "Accept": "application/vnd.github.v3+json"}

def push(local_path, gh_path, message):
    if not os.path.exists(local_path):
        print(f"skip {local_path} (not found)")
        return
    with open(local_path, "rb") as f:
        content = base64.b64encode(f.read()).decode()
    r = requests.get(f"https://api.github.com/repos/{REPO}/contents/{gh_path}", headers=HEADERS)
    sha = r.json().get("sha") if r.status_code == 200 else None
    payload = {"message": message, "content": content}
    if sha:
        payload["sha"] = sha
    r2 = requests.put(
        f"https://api.github.com/repos/{REPO}/contents/{gh_path}",
        headers=HEADERS, json=payload
    )
    ok = r2.status_code in [200, 201]
    print(f"{'✅' if ok else '❌'} {gh_path}: {r2.status_code}")
    return ok

push("docs/oracle/index.html", "docs/oracle/index.html", "auto: GEX Oracle dashboard")
push("data/oracle_market_data.json", "data/oracle_market_data.json", "auto: Oracle market data")
