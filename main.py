#!/usr/bin/env python3
"""
================================================================================
  ðŸ”¥ ZEE5 Channel Fetcher â€” by @ssk4570
  Fetches all ZEE5 channels + clean m3u8 stream URLs.
  Output: api/zee_api.json

  State files (also in api/):
    api/channel.json       â† catalog (1 year TTL)
    api/tokens.json        â† device_id, guest_token, dd_token, access_token (23h TTL)
    api/country_map.json   â† per-channel working country

  Each entry:  name | group | url | apiby
================================================================================
"""

import os
import re
import json
import time
import uuid
import base64
import logging
import datetime
import requests
import urllib3
import concurrent.futures
from datetime import timedelta, timezone as dt_timezone

urllib3.disable_warnings()


# â”€â”€ CONFIG â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
API_DIR     = os.path.join(SCRIPT_DIR, "api")
os.makedirs(API_DIR, exist_ok=True)

OUTPUT_FILE      = os.path.join(API_DIR, "zee_api.json")
OUTPUT_NAME      = "api/zee_api.json"
CHANNEL_JSON     = os.path.join(API_DIR, "channel.json")
TOKENS_JSON      = os.path.join(API_DIR, "tokens.json")
COUNTRY_MAP_FILE = os.path.join(API_DIR, "country_map.json")

ZEE5_API_URL       = "https://spapi.zee5.com/singlePlayback/v2/getDetails/secure"
COUNTRY_LIST       = ["IN", "GB", "DE", "SG", "CA", "AE", "GL", "US"]
TOKEN_CACHE_TTL    = 23 * 3600
CHANNEL_CACHE_TTL  = 365 * 24 * 3600
MAX_WORKERS        = 10
PROGRESS_EVERY     = 25

APIBY = "@ssk4570"

BASE_PARAMS = {
    "content_id":               "",
    "show_id":                  "",
    "device_id":                "",
    "session_id":               "",
    "check_parental_control":   "false",
    "current_parental_control": "",
    "country":                  "IN",
    "platform_name":            "ctv_android",
    "state":                    "DL",
    "translation":              "en",
    "display_language":         "en",
    "user_language":            "hi,en,pa,hr",
    "collection_id":            "",
    "app_version":              "5.82.6",
    "gender":                   "",
    "age":                      "0",
    "brand":                    "Redmi",
    "model":                    "Redmi Note 8 Pro",
    "utm_source":               "",
    "utm_medium":               "",
    "utm_campaign":             "",
    "utm_term":                 "",
    "utm_content":              "",
    "version":                  "15",
    "is_marketing":             "0",
    "is_latest":                "0",
    "is_kids_profile":          "false",
}

PAYLOAD_TEMPLATE = {
    "Authorization":    "",
    "X-Z5-Guest-Token": "",
}

HEADERS_TEMPLATE = {
    "User-Agent":              "com.graymatrix.did;Android TV;5.82.6(203230929);Android(10);Xiaomi(Redmi Note 8 Pro)",
    "Accept-Encoding":         "gzip",
    "g_id":                    "",
    "x-z5-appversion":         "5.82.6",
    "x-z5-appversionnumber":   "203230929",
    "x-z5-appplatform":        "Android TV",
    "x-user-type":             "guest",
    "x-z5-device-id":          "",
    "x-z5-guest-token":        "",
    "content-type":            "application/json; charset=utf-8",
}


# â”€â”€ LOGGING â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
class _Fmt(logging.Formatter):
    C="\033[96m"; G="\033[92m"; Y="\033[93m"; R="\033[91m"; Z="\033[0m"
    def format(self, record):
        dt = datetime.datetime.now(dt_timezone(timedelta(hours=5, minutes=30))).strftime("%H:%M:%S")
        c = self.G
        if record.levelno >= logging.ERROR:    c = self.R
        elif record.levelno >= logging.WARNING: c = self.Y
        return f"{self.C}[{dt}]{self.Z} {c}{record.levelname:<7}{self.Z} {record.getMessage()}"

_h = logging.StreamHandler()
_h.setFormatter(_Fmt())
log = logging.getLogger("ZEE5")
log.setLevel(logging.INFO)
log.addHandler(_h)


# â”€â”€ HELPERS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def fetch_url(url, method="GET", headers=None, data=None, timeout=15):
    try:
        if method == "POST":
            r = requests.post(url, headers=headers, data=data, timeout=timeout, verify=False)
        else:
            r = requests.get(url, headers=headers, timeout=timeout, verify=False)
        return {"code": r.status_code, "body": r.text}
    except Exception as e:
        return {"code": 0, "body": "", "error": str(e)}


def generate_device_id():
    return str(uuid.uuid4())


def generate_guest_token():
    return "Z5X_" + uuid.uuid4().hex + uuid.uuid4().hex


def generate_dd_token():
    payload = {
        "os_name":          "Android",
        "os_version":       "10",
        "platform_name":    "Xiaomi",
        "platform_version": "29",
        "device_name":      "Redmi Note 8 Pro",
        "app_name":         "ctv_android",
        "app_version":      "5.82.6",
        "player_capabilities": {
            "audio_channel":  ["STEREO"],
            "video_codec":    ["H264"],
            "resolution":     ["HD", "FHD", "UHD"],
            "dynamic_range":  ["SDR", "HDR", "HLG"],
        },
        "security_capabilities": {
            "encryption":              ["WIDEVINE_AES_CTR", "WIDEVINE_AES_CBCS"],
            "widevine_security_level": ["L1"],
            "hdcp_version":            ["HDCP_V1"],
        },
    }
    return base64.b64encode(
        json.dumps(payload, separators=(",", ":")).encode()
    ).decode()


def fetch_fresh_access_token(device_id):
    log.info("[*] Fetching fresh ZEE5 access token...")
    params = {
        "ccode": "IN", "lang": "en", "version": "4",
        "country": "IN", "state": "DL", "translation": "en",
        "platform_name": "android_tv", "partner_name": "",
    }
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    auth_headers = {
        "Host":            "launchapi.zee5.com",
        "User-Agent":      "com.graymatrix.did;Android TV;5.82.6(203230929);Android(10);Xiaomi(Redmi Note 8 Pro)",
        "Accept-Encoding": "gzip",
        "g_id":            device_id,
        "Cookie":          "variant=v2",
    }

    res = fetch_url("https://stcf1.zee5.com/launch?" + qs, headers=auth_headers)
    if res["code"] == 403:
        res = fetch_url("https://launchapi.zee5.com/launch?" + qs, headers=auth_headers)

    if res["code"] == 200 and res["body"]:
        try:
            data = json.loads(res["body"])
            token = (data.get("platform_token") or {}).get("token")
            if token:
                return token
        except Exception:
            pass
    return ""


def get_all_tokens(force_refresh=False):
    """Load cached tokens or generate fresh ones (respects force_refresh)."""
    if not force_refresh and os.path.exists(TOKENS_JSON) and (time.time() - os.path.getmtime(TOKENS_JSON) < TOKEN_CACHE_TTL):
        try:
            with open(TOKENS_JSON) as f:
                t = json.load(f)
            if t and all(k in t for k in ("access_token", "dd_token", "device_id", "guest_token")):
                log.info("âœ“ Loaded tokens from api/tokens.json")
                return t
        except Exception:
            pass

    if force_refresh:
        log.info("ðŸ”„ Forcing fresh token generation...")

    device_id    = generate_device_id()
    guest_token  = generate_guest_token()
    dd_token     = generate_dd_token()
    access_token = fetch_fresh_access_token(device_id)

    tokens = {
        "device_id":    device_id,
        "guest_token":  guest_token,
        "dd_token":     dd_token,
        "access_token": access_token,
    }
    try:
        with open(TOKENS_JSON, "w") as f:
            json.dump(tokens, f, indent=2)
        log.info("âœ“ Saved tokens â†’ api/tokens.json")
    except Exception:
        pass
    return tokens


def get_channel_country(channel_id):
    if os.path.exists(COUNTRY_MAP_FILE):
        try:
            with open(COUNTRY_MAP_FILE) as f:
                m = json.load(f)
            if channel_id in m:
                return m[channel_id]
        except Exception:
            pass
    return "IN"


def save_channel_country(channel_id, country):
    m = {}
    if os.path.exists(COUNTRY_MAP_FILE):
        try:
            with open(COUNTRY_MAP_FILE) as f:
                m = json.load(f) or {}
        except Exception:
            pass
    m[channel_id] = country
    try:
        with open(COUNTRY_MAP_FILE, "w") as f:
            json.dump(m, f, indent=2)
    except Exception:
        pass


# â”€â”€ STREAM URL â€” returns (token, reason) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def fetch_stream_url(channel_id, force_refresh=False):
    """
    Returns (token, reason).
      - On success: (m3u8_url, "ok")
      - On failure: (None, short_reason_string)

    short_reason_string explains WHY so the caller can log it clearly.
    """
    target_country = get_channel_country(channel_id)
    countries      = list(dict.fromkeys([target_country] + COUNTRY_LIST))

    tokens       = get_all_tokens(force_refresh=force_refresh)
    device_id    = tokens["device_id"]
    guest_token  = tokens["guest_token"]
    dd_token     = tokens["dd_token"]
    access_token = tokens["access_token"]

    last_reason    = "no-attempt"
    had_auth_error = False

    for country in countries:
        params = dict(BASE_PARAMS)
        params["channel_id"] = channel_id
        params["device_id"]  = device_id
        params["country"]    = country

        headers = dict(HEADERS_TEMPLATE)
        headers["g_id"]             = device_id
        headers["x-z5-device-id"]   = device_id
        headers["x-z5-guest-token"] = guest_token

        payload = dict(PAYLOAD_TEMPLATE)
        payload["X-Z5-Guest-Token"] = guest_token
        payload["x-dd-token"]       = dd_token

        if access_token:
            headers["x-access-token"] = access_token
            payload["x-access-token"] = access_token

        url = ZEE5_API_URL + "?" + "&".join(f"{k}={v}" for k, v in params.items())
        res = fetch_url(url, "POST", headers, json.dumps(payload))

        # â”€â”€ Diagnose response â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        if res["code"] == 0:
            last_reason = f"net:{res.get('error','')[:40]}"
            continue

        if res["code"] in (401, 403):
            last_reason = f"auth:{res['code']}"
            had_auth_error = True
            continue

        if res["code"] != 200:
            last_reason = f"http:{res['code']}"
            continue

        try:
            data = json.loads(res["body"])
        except Exception:
            last_reason = "json:invalid"
            continue

        ec  = data.get("error_code")
        msg = (data.get("error_message") or data.get("message") or "").strip()

        if str(ec) == "607":
            last_reason = "geo:607"
            continue

        if ec and str(ec) not in ("0", "200", "None", ""):
            last_reason = f"api:{ec}:{msg[:30]}"
            continue

        token = (data.get("keyOsDetails") or {}).get("video_token")
        if token:
            if country != target_country:
                save_channel_country(channel_id, country)
            return token, "ok"

        # 200 but no token â€” check for DRM / subscription hints
        hints = []
        for k in ("drm", "isDrm", "is_premium", "isPremium", "subscription_required", "is_free"):
            if k in data:
                hints.append(f"{k}={data[k]}")
        hint_str = f" ({', '.join(hints)})" if hints else ""
        last_reason = f"no-token{hint_str}"
        # Try next country

    # Auth failed across all countries â€” refresh tokens once and retry
    if had_auth_error and not force_refresh:
        log.info(f"    ðŸ”„ auth failure for {channel_id} â€” refreshing tokens & retrying...")
        return fetch_stream_url(channel_id, force_refresh=True)

    return None, last_reason


# â”€â”€ CATALOG â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def load_channels():
    if os.path.exists(CHANNEL_JSON) and (time.time() - os.path.getmtime(CHANNEL_JSON) < CHANNEL_CACHE_TTL):
        try:
            with open(CHANNEL_JSON) as f:
                data = json.load(f)
            channels = data.get("items", [])
            if channels:
                log.info(f"âœ“ Loaded {len(channels)} channels from api/channel.json")
                return channels
        except Exception:
            pass

    log.info("ðŸ“¥ Fetching channel catalog from ZEE5...")
    res = fetch_url("https://catalogapi.zee5.com/v1/channel?page_size=200")
    if res["code"] == 200 and res["body"]:
        try:
            with open(CHANNEL_JSON, "w") as f:
                f.write(res["body"])
            log.info("âœ“ Saved catalog â†’ api/channel.json")
        except Exception:
            pass
        try:
            data = json.loads(res["body"])
            channels = data.get("items", [])
            log.info(f"âœ“ Fetched {len(channels)} channels")
            return channels
        except Exception:
            pass

    log.error("âœ— Failed to fetch channels")
    return []


def _pick_group(ch):
    genres = ch.get("genres") or ch.get("genre") or ch.get("category") or []
    if isinstance(genres, list) and genres:
        g0 = genres[0]
        if isinstance(g0, dict):
            return g0.get("name") or g0.get("title") or g0.get("id") or ""
        elif isinstance(g0, str):
            return g0
    elif isinstance(genres, str):
        return genres
    return ch.get("category_name") or ""


def save(results):
    try:
        os.makedirs(API_DIR, exist_ok=True)
        tmp = OUTPUT_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        os.replace(tmp, OUTPUT_FILE)
        log.info(f"ðŸ’¾ Saved â†’ {OUTPUT_NAME}   ({len(results)} channels)")
        return True
    except Exception as e:
        log.error(f"âŒ Save failed: {e}")
        return False


# â”€â”€ MAIN â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def main():
    log.info("=" * 68)
    log.info("  ðŸ”¥ ZEE5 Channel Fetcher â€” @ssk4570")
    log.info("=" * 68)
    log.info(f"ðŸ“‚ Script dir: {SCRIPT_DIR}")
    log.info(f"ðŸ’¾ Output    : {OUTPUT_NAME}")
    log.info(f"ðŸ“ State dir : api/")
    log.info("=" * 68)

    channels = load_channels()
    if not channels:
        save([])
        return

    results      = []
    failed       = 0
    done         = 0
    fail_reasons = {}   # reason â†’ count

    def process(ch):
        cid   = ch.get("id", "")
        title = ch.get("title") or ch.get("original_title") or ch.get("name") or ""
        if not cid:
            return None

        m3u8, reason = fetch_stream_url(cid)
        if not m3u8:
            return {"_fail": reason, "name": title or cid, "group": _pick_group(ch)}

        return {
            "name":  title or f"Channel {cid}",
            "group": _pick_group(ch),
            "url":   m3u8,
            "apiby": APIBY,
        }

    log.info(f"ðŸš€ Processing {len(channels)} channels (workers={MAX_WORKERS})...")
    log.info("-" * 68)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(process, ch): ch for ch in channels}
        for fut in concurrent.futures.as_completed(futures):
            done += 1
            try:
                r = fut.result()
                if r and "_fail" not in r:
                    results.append(r)
                    log.info(f"  âœ… [{done}/{len(channels)}] {r['name']}")
                else:
                    failed += 1
                    if r:
                        reason = r["_fail"]
                        # Group similar reasons for the summary
                        bucket = reason.split(":")[0]
                        fail_reasons[bucket] = fail_reasons.get(bucket, 0) + 1
                        log.warning(f"  âœ— [{done}/{len(channels)}] {r['name']} â€” {reason}")
                    else:
                        log.warning(f"  âœ— [{done}/{len(channels)}] (empty)")
            except Exception as e:
                failed += 1
                log.warning(f"  âœ— [{done}/{len(channels)}] crash: {e}")

            if done % PROGRESS_EVERY == 0:
                log.info(f"  ðŸ“Š {done}/{len(channels)} | {len(results)} ok, {failed} skipped")

    log.info("-" * 68)
    log.info(f"âœ¨ Done | âœ… {len(results)} ok | âŠ˜ {failed} skipped")

    # â”€â”€ Failure breakdown â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if fail_reasons:
        log.info("")
        log.info("ðŸ“Š Failure breakdown:")
        reason_labels = {
            "geo":           "ðŸŒ Geo-blocked (607) â€” not available in any country tried",
            "auth":          "ðŸ”’ Auth error (401/403) â€” token invalid even after refresh",
            "api":           "âš   API error â€” server rejected request",
            "no-token":      "ðŸš« No video token â€” channel is premium / DRM-locked / offline",
            "http":          "ðŸ”Œ HTTP error â€” server-side issue",
            "net":           "ðŸ“¡ Network error â€” couldn't reach ZEE5",
            "json":          "ðŸ› Malformed response",
            "no-attempt":    "â“ No attempt was made",
        }
        for bucket, count in sorted(fail_reasons.items(), key=lambda x: -x[1]):
            label = reason_labels.get(bucket, bucket)
            log.info(f"   {count:3d} Ã— {label}")
        log.info("")

    results.sort(key=lambda x: x["name"].lower())
    save(results)
    log.info("=" * 68)


if __name__ == "__main__":
    main()
