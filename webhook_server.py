from flask import Flask, request, jsonify
import json, traceback
from datetime import datetime, timedelta
from odoo_connector import (
    create_odoo_contact, update_odoo_contact, find_existing_contact,
    create_odoo_opportunity, find_existing_opportunity,
    connect_odoo, get_or_create_opportunity_tags,
    update_odoo_opportunity, find_odoo_user_id,
    create_odoo_activity, get_model_id, ODOO_URL
)
from geopy.geocoders import Nominatim

app = Flask(__name__)
geolocator = Nominatim(user_agent="WavcorWebhook")

@app.route("/wix_form_webhook", methods=["POST"])
def handle_form():
    print("🔔 Received webhook request", flush=True)
    try:
        # Log request headers and raw body
        print("Request headers:", dict(request.headers), flush=True)
        raw_body = request.data.decode("utf-8")
        print("Raw body:", raw_body, flush=True)

        # Parse JSON first
        data = request.get_json(force=True)
        print("📦 Incoming JSON Keys:", list(data.keys()), flush=True)
        print("✅ Parsed JSON data:", json.dumps(data, indent=2), flush=True)

        # Handle nested data structure if needed
        payload = data.get("debug_echo", data)

        # Example expected keys (update if Wix uses different names)
        name = f"{payload.get('firstName', '')} {payload.get('lastName', '')}".strip() or payload.get("name")
        email = payload.get("email")
        phone = payload.get("phone")
        city = payload.get("city")
        province = payload.get("provinceState") or payload.get("province")
        products = payload.get("productInterest", "") or payload.get("products", "")
        message = payload.get("message", "")

        print(f"DEBUG parsed fields → Name: {name}, Email: {email}, City: {city}, Province: {province}", flush=True)
        print(f"DEBUG Message: {message}", flush=True)

        # --- Return parsed info for testing ---
        return jsonify({
            "status": "ok",
            "parsed_fields": {
                "name": name,
                "email": email,
                "phone": phone,
                "city": city,
                "province": province,
                "products": products,
                "message": message
            }
        }), 200

    except Exception as e:
        print("❌ Exception occurred:", flush=True)
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    print("🚀 Flask webhook server starting...", flush=True)
    app.run(host="0.0.0.0", port=8080, debug=True)
