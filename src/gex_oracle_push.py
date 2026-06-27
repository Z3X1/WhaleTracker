#!/usr/bin/env python3
import requests, base64, os, hashlib, json

TOKEN = os.environ.get("GH_TOKEN", "")
REPO = os.environ.get("GH_REPO", "Z3X1/SideProject_WhaleTracker")
HEADERS = {"Authorization": f"token {TOKEN}", "Accept": "application/vnd.github.v3+json"}
PASSWORD_HASH = "3ac22acab4270f1d078564ef14475d2ad239398b61104e839cb73b7c1f65eb63"

def get_sha(path):
    r = requests.get(f"https://api.github.com/repos/{REPO}/contents/{path}", headers=HEADERS)
    return r.json().get("sha") if r.status_code == 200 else None

def push_bytes(gh_path, content_bytes, message):
    sha = get_sha(gh_path)
    payload = {"message": message, "content": base64.b64encode(content_bytes).decode()}
    if sha:
        payload["sha"] = sha
    r = requests.put(f"https://api.github.com/repos/{REPO}/contents/{gh_path}", headers=HEADERS, json=payload)
    ok = r.status_code in [200, 201]
    print(f"{'✅' if ok else '❌'} {gh_path}: {r.status_code}")
    return ok

def wrap_with_password(html_content):
    """用JS密碼保護包裝HTML，不使用base64避免UTF-8亂碼"""
    # 把內容存成JSON字串，保留Unicode
    content_json = json.dumps(html_content)
    
    protected = f"""<!DOCTYPE html>
<html lang="zh-TW" translate="no">
<head>
<meta charset="UTF-8">
<meta name="google" content="notranslate">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>GEX Oracle</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:#0a0e17;color:#e2e8f0;font-family:monospace;display:flex;justify-content:center;align-items:center;height:100vh}}
.box{{text-align:center;padding:40px;border:1px solid #1e293b;border-radius:12px;background:#111827;width:300px}}
h2{{color:#3b82f6;margin-bottom:24px;letter-spacing:3px;font-size:18px}}
input{{background:#0a0e17;border:1px solid #3b82f6;color:#e2e8f0;padding:12px 20px;border-radius:6px;font-size:16px;width:100%;text-align:center;outline:none}}
button{{background:#3b82f6;color:white;border:none;padding:12px;border-radius:6px;font-size:14px;cursor:pointer;margin-top:12px;width:100%;letter-spacing:1px}}
button:hover{{background:#2563eb}}
.err{{color:#ef4444;margin-top:10px;font-size:12px;height:16px}}
</style>
</head>
<body>
<div class="box">
<h2>⚡ GEX ORACLE</h2>
<input type="password" id="pw" placeholder="Password" onkeydown="if(event.key==='Enter')check()">
<button onclick="check()">ENTER</button>
<div class="err" id="err"></div>
</div>
<script>
const HASH="{pw_hash}";
const DATA={content_json};
async function sha256(s){{
  const b=new TextEncoder().encode(s);
  const h=await crypto.subtle.digest('SHA-256',b);
  return Array.from(new Uint8Array(h)).map(x=>x.toString(16).padStart(2,'0')).join('');
}}
async function check(){{
  const pw=document.getElementById('pw').value;
  const h=await sha256(pw);
  if(h===HASH){{
    // 先寫入，再設定translate=no
    document.open('text/html','replace');
    document.write(DATA);
    document.close();
    // 確保不觸發翻譯
    if(document.documentElement) document.documentElement.setAttribute('translate','no');
  }}else{{
    document.getElementById('err').textContent='Wrong password';
    document.getElementById('pw').value='';
    document.getElementById('pw').focus();
  }}
}}
document.getElementById('pw').focus();
</script>
</body>
</html>"""
    return protected

# 推送
files = {
    "docs/oracle/index.html": "docs/oracle/index.html",
    "data/oracle_market_data.json": "data/oracle_market_data.json",
}

for gh_path, local_path in files.items():
    if not os.path.exists(local_path):
        print(f"skip {local_path}")
        continue
    
    if gh_path == "docs/oracle/index.html":
        with open(local_path, "r", encoding="utf-8") as f:
            html = f.read()
        protected = wrap_with_password(html)
        push_bytes(gh_path, protected.encode("utf-8"), "auto: GEX Oracle dashboard (password protected)")
    else:
        with open(local_path, "rb") as f:
            push_bytes(gh_path, f.read(), f"auto: {gh_path}")
