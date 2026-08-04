from fyers_apiv3 import fyersModel
import webbrowser
from urllib.parse import urlparse, parse_qs
import hashlib
import requests

"""
Fyers API – Login flow (generate authcode, then exchange for access_token)

1. API docs:   https://myapi.fyers.in/docsv3
2. Create app: https://myapi.fyers.in/dashboard/

Flow: SessionModel → generate_authcode() → open URL → paste auth_code/URL → generate_token()
"""

# ============================================
# 1. Input parameters (from your app on dashboard)
# ============================================
redirect_uri = "https://myapi.fyers.in"   # Must EXACTLY match Redirect URI in app (with or without trailing /)
client_id = "JUJ1HCA83I-100"              # APP_ID from dashboard
secret_key = "2K19R5V3HJ"                 # app_secret from dashboard
grant_type = "authorization_code"          # always "authorization_code"
response_type = "code"                     # always "code"
state = "sample"                        # session manager; returned with auth_code

# ============================================
# 2. Generate authcode URL and open in browser
# ============================================
print("=" * 60)
print("FYERS API LOGIN")
print("=" * 60)

appSession = fyersModel.SessionModel(
    client_id=client_id,
    redirect_uri=redirect_uri,
    response_type=response_type,
    state=state,
    secret_key=secret_key,
    grant_type=grant_type,
)

generateTokenUrl = appSession.generate_authcode()
print(generateTokenUrl)
print("\nOpening browser... After login, copy the full redirect URL or auth_code.\n")
webbrowser.open(generateTokenUrl, new=1)

# ============================================
# STEP 3: After login, paste the auth_code here
# ============================================
print("=" * 60)
print("STEP 3: After logging in, you'll be redirected to:")
print("https://myapi.fyers.in/?auth_code=XXXXX&state=sample_state")
print("\nIMPORTANT: Copy the ENTIRE auth_code value from the URL")
print("The auth_code is usually a long string (100+ characters)")
print("\nYou can either:")
print("  1. Paste just the auth_code value")
print("  2. Paste the full URL (we'll extract the auth_code automatically)")
print("=" * 60)

user_input = input("\nPaste your auth_code or full URL here: ").strip()

if not user_input:
    print("Error: No auth_code provided!")
    exit(1)

# Try to extract auth_code from URL if full URL was pasted
auth_code = user_input
if "auth_code=" in user_input:
    try:
        # Extract auth_code from URL
        parsed_url = urlparse(user_input)
        params = parse_qs(parsed_url.query)
        if 'auth_code' in params:
            auth_code = params['auth_code'][0]
            print(f"\n✓ Extracted auth_code from URL (length: {len(auth_code)} characters)")
        else:
            print("\n⚠ Could not find auth_code in URL. Using input as-is.")
    except Exception as e:
        print(f"\n⚠ Could not parse URL: {e}. Using input as-is.")

# If user pasted "auth_code_value&state=sample_state", keep only the JWT part
if "&" in auth_code:
    auth_code = auth_code.split("&")[0]
    print(f"✓ Stripped trailing &state=... from auth_code")

# Validate auth_code length (should be reasonably long)
if len(auth_code) < 20:
    print(f"\n⚠ WARNING: Auth code seems too short ({len(auth_code)} characters)")
    print("Make sure you copied the COMPLETE auth_code from the URL")
    confirm = input("Continue anyway? (y/n): ").strip().lower()
    if confirm != 'y':
        print("Exiting. Please try again with the complete auth_code.")
        exit(1)

# ============================================
# 3. Paste auth_code (or full URL) and generate access_token
# ============================================
print("\nGenerating access token...")

try:
    appSession.set_token(auth_code)
    response = appSession.generate_token()
    
    if "access_token" in response:
        access_token = response["access_token"]
        print("\n" + "=" * 60)
        print("SUCCESS! Access token generated")
        print("=" * 60)
        # Save token to file for future use
        with open("access_token.txt", "w") as f:
            f.write(access_token)
        print("\n✓ Access token saved to 'access_token.txt'")
        print("Keep this file safe and do not commit it to version control.")
        print("Use it like:")
        print("  from pathlib import Path")
        print("  token = Path('access_token.txt').read_text().strip()")
        print("  fyers = fyersModel.FyersModel(token=token, client_id=client_id)")
        
    else:
        print("\n" + "=" * 60)
        print("ERROR: Failed to generate access token")
        print("=" * 60)
        print(f"Response: {response}")
        
        if response.get('code') == -437 or 'invalid auth code' in str(response.get('message', '')).lower():
            print("\n" + "=" * 60)
            print("TROUBLESHOOTING: Invalid Auth Code")
            print("=" * 60)
            print("This usually means:")
            print("  1. The auth_code was incomplete or incorrect")
            print("  2. The auth_code has expired (they expire quickly)")
            print("  3. You copied only part of the auth_code")
            print("\nSOLUTION:")
            print("  1. Run the script again to get a fresh login URL")
            print("  2. After login, copy the COMPLETE auth_code from the URL")
            print("  3. The auth_code should be a long string (usually 100+ characters)")
            print("  4. Make sure there are no spaces or line breaks in the code")
            print("  5. You can paste the full URL - we'll extract the auth_code automatically")
        
        elif response.get('code') == -5 or 'invalid app id hash' in str(response.get('message', '')).lower():
            # Fallback: try token API with appIdHash using App ID WITHOUT "-100" suffix
            print("\nTrying fallback: App ID without '-100' suffix for hash...")
            app_id_base = client_id.rsplit("-", 1)[0] if "-" in client_id else client_id
            hash_input = f"{app_id_base}:{secret_key}"
            app_id_hash = hashlib.sha256(hash_input.encode()).hexdigest()
            try:
                r = requests.post(
                    "https://api-t1.fyers.in/api/v3/validate-authcode",
                    json={"grant_type": "authorization_code", "appIdHash": app_id_hash, "code": auth_code},
                    timeout=10,
                )
                resp = r.json()
                if resp.get("s") == "ok" and resp.get("access_token"):
                    access_token = resp["access_token"]
                    with open("access_token.txt", "w") as f:
                        f.write(access_token)
                    print("\n" + "=" * 60)
                    print("SUCCESS! Access token generated (fallback worked)")
                    print("=" * 60)
                    print("\n✓ Access token saved to 'access_token.txt'")
                    import sys
                    sys.exit(0)
            except Exception as fb_err:
                print(f"Fallback failed: {fb_err}")
            
            print("\n" + "=" * 60)
            print("TROUBLESHOOTING: Invalid App ID Hash (code -5)")
            print("=" * 60)
            print("This usually means App ID / Secret / Redirect URI don't match the dashboard.")
            print("\nSOLUTION:")
            print("  1. Go to https://myapi.fyers.in/dashboard/")
            print("  2. Open your app and check:")
            print("     - App ID: copy again (exactly JUJ1HCA83I-100, no spaces)")
            print("     - Secret ID: copy again (exactly as shown, no spaces)")
            print("  3. Redirect URL in app MUST match exactly what you use here:")
            print("     - If dashboard shows https://myapi.fyers.in/  → use that (with slash)")
            print("     - If dashboard shows https://myapi.fyers.in   → change redirect_uri in this file to no slash")
            print("  4. Update client_id and secret_key at the top of this file, then run again.")
            print("  5. If Secret was regenerated, use the NEW Secret ID only.")
        
except Exception as e:
    print("\n" + "=" * 60)
    print("ERROR occurred")
    print("=" * 60)
    print(f"Error: {e}")
    print(f"Response: {response if 'response' in locals() else 'N/A'}")

