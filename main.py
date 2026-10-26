#!/usr/bin/env python3
"""
================================================================================
  🔥 ZEE5 DEBUGGER — by @ssk4570 (Debug Version)
  Fetches channels & prints detailed debug info for any failures.
================================================================================
"""

import os
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

# ── CONFIG ────────────────────────────────────────────────────────────────────
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
API_DIR     = os.path.join(SCRIPT_DIR, "api")
os.makedirs(API_DIR, exist_ok=True)

CHANNEL_JSON     = os.path.join(API_DIR, "channel.json")
TOKENS_JSON      = os.path.join(API_DIR, "tokens.json")
COUNTRY_MAP_FILE = os.path.join(API_DIR, "country_map.json")

ZEE5_API_URL       = "https://spapi.zee5.com/singlePlayback/v2/getDetails/secure"
COUNTRY_LIST       = ["IN", "GB", "DE", "SG", "CA", "AE", "GL", "US"]
MAX_WORKERS        = 3  # डीबग करताना कमी कामगार ठेवा म्हणजे लॉग वाचायला सोपे जाईल

# ── LOGGING ───────────────────────────────────────────────────────────────────
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
log = logging.getLogger("ZEE5_DEBUG")
log.setLevel(logging.INFO)
log.addHandler(_h)


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
        "os_name": "Android", "os_version": "10", "platform_name": "Xiaomi",
        "platform_version": "29", "device_name": "Redmi Note 8 Pro",
        "app_name": "ctv_android", "app_version": "5.82.6",
        "player_capabilities": {"audio_channel": ["STEREO"], "video_codec": ["H264"], "resolution": ["HD", "FHD", "UHD"], "dynamic_range": ["SDR", "HDR", "HLG"]},
        "security_capabilities": {"encryption": ["WIDEVINE_AES_CTR"], "widevine_security_level": ["L1"], "hdcp_version": ["HDCP_V1"]}
    }
    return base64.b64encode(json.dumps(payload, separators=(",", ":")).encode()).decode()


def fetch_fresh_access_token(device_id):
    log.info("[*] Attempting to fetch fresh ZEE5 access token...")
    params = {
        "ccode": "IN", "lang": "en", "version": "4",
        "country": "IN", "state": "DL", "translation": "en",
        "platform_name": "android_tv", "partner_name": "",
    }
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    auth_headers = {
        "Host": "launchapi.zee5.com",
        "User-Agent": "com.graymatrix.did;Android TV;5.82.6(203230929);Android(10);Xiaomi(Redmi Note 8 Pro)",
        "Accept-Encoding": "gzip",
        "g_id": device_id,
        "Cookie": "variant=v2",
    }

    # Test stcf1
    res = fetch_url("https://stcf1.zee5.com/launch?" + qs, headers=auth_headers)
    log.info(f"    -> stcf1 launch response code: {res['code']}")
    
    if res["code"] == 403:
        log.warning("    -> stcf1 returned 403. Trying launchapi.zee5.com...")
        res = fetch_url("https://launchapi.zee5.com/launch?" + qs, headers=auth_headers)
        log.info(f"    -> launchapi response code: {res['code']}")

    if res["code"] == 200 and res["body"]:
        try:
            data = json.loads(res["body"])
            token = (data.get("platform_token") or {}).get("token")
            if token:
                log.info("    -> Successfully got access token!")
                return token
            else:
                log.error(f"    -> JSON response lacks platform_token: {res['body'][:200]}")
        except Exception as e:
            log.error(f"    -> Failed to parse launch JSON: {e} | Body: {res['body'][:200]}")
    else:
        log.error(f"    -> Launch API failed completely. Body snippet: {res['body'][:300]}")
    
    return ""


def get_all_tokens():
    device_id = generate_device_id()
    guest_token = generate_guest_token()
    dd_token = generate_dd_token()
    access_token = fetch_fresh_access_token(device_id)
    return {
        "device_id": device_id,
        "guest_token": guest_token,
        "dd_token": dd_token,
        "access_token": access_token,
    }


def debug_single_channel(channel_id):
    log.info(f"\n==================== DEBUGGING CHANNEL ID: {channel_id} ====================")
    tokens = get_all_tokens()
    
    params = {
        "content_id": "", "show_id": "", "device_id": tokens["device_id"],
        "session_id": "", "check_parental_control": "false", "country": "IN",
        "platform_name": "ctv_android", "state": "DL", "translation": "en",
        "display_language": "en", "user_language": "hi,en,pa,hr",
        "app_version": "5.82.6", "brand": "Redmi", "model": "Redmi Note 8 Pro",
        "version": "15", "channel_id": channel_id
    }

    headers = {
        "User-Agent": "com.graymatrix.did;Android TV;5.82.6(203230929);Android(10);Xiaomi(Redmi Note 8 Pro)",
        "Accept-Encoding": "gzip",
        "g_id": tokens["device_id"],
        "x-z5-appversion": "5.82.6",
        "x-z5-appversionnumber": "203230929",
        "x-z5-appplatform": "Android TV",
        "x-user-type": "guest",
        "x-z5-device-id": tokens["device_id"],
        "x-z5-guest-token": tokens["guest_token"],
        "content-type": "application/json; charset=utf-8",
    }
    if tokens["access_token"]:
        headers["x-access-token"] = tokens["access_token"]

    payload = {
        "Authorization": "",
        "X-Z5-Guest-Token": tokens["guest_token"],
        "x-dd-token": tokens["dd_token"],
    }
    if tokens["access_token"]:
        payload["x-access-token"] = tokens["access_token"]

    url = ZEE5_API_URL + "?" + "&".join(f"{k}={v}" for k, v in params.items())
    
    log.info(f"POST Request URL: {url}")
    log.info(f"Headers sent: {json.dumps(headers, indent=2)}")
    log.info(f"Payload sent: {json.dumps(payload, indent=2)}")

    res = fetch_url(url, "POST", headers, json.dumps(payload))
    log.info(f"Response Status Code: {res['code']}")
    log.info(f"Response Body (First 500 chars): {res['body'][:500]}")

    if res["code"] == 200:
        try:
            data = json.loads(res["body"])
            log.info(f"Parsed JSON Error Code: {data.get('error_code')}")
            log.info(f"Parsed JSON Message: {data.get('error_message') or data.get('message')}")
            video_token = (data.get("keyOsDetails") or {}).get("video_token")
            if video_token:
                log.info(f"SUCCESS! Got Video Token: {video_token[:50]}...")
            else:
                log.warning("Response was 200, but NO video_token found in keyOsDetails.")
        except Exception as e:
            log.error(f"Failed to parse JSON response: {e}")
    else:
        log.error(f"API request failed with status {res['code']}")


def main():
    log.info("Starting ZEE5 API Debugger...")
    
    # कॅटलॉगवरून पहिले चॅनेल फेच करा किंवा टेस्टसाठी थेट आयडी टाका
    res = fetch_url("https://catalogapi.zee5.com/v1/channel?page_size=5")
    if res["code"] == 200 and res["body"]:
        try:
            data = json.loads(res["body"])
            items = data.get("items", [])
            if items:
                first_ch = items[0]
                cid = first_ch.get("id")
                cname = first_ch.get("title") or first_ch.get("name")
                log.info(f"Testing with First Channel from Catalog -> Name: {cname} | ID: {cid}")
                debug_single_channel(cid)
                return
        except Exception as e:
            log.error(f"Catalog parse error: {e}")
    
    log.error("Could not fetch catalog channels. Testing with a fallback known Zee Marathi ID or generic ID.")
    debug_single_channel("0-9-zee-marathi")


if __name__ == "__main__":
    main()
  
