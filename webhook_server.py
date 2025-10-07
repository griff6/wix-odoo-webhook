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
    print("🔔 Received webhook request")
    try:
        # Log the raw request first
        print("Request headers:", dict(request.headers))
        print("Raw body:", request.data.decode("utf-8"))

        data = request.get_json(force=True)
        print("✅ Parsed JSON data:", json.dumps(data, indent=2))

        # Example expected keys (update if Wix uses different names)
        name = f"{data.get('firstName', '')} {data.get('lastName', '')}".strip()
        email = data.get("email")
        phone = data.get("phone")
        city = data.get("city")
        province = data.get("provinceState")
        products = data.get("productInterest", "")
        message = data.get("message", "")

        print(f"DEBUG parsed fields → Name: {name}, Email: {email}, City: {city}, Province: {province}")
        print(f"DEBUG Message: {message}")

        # --- Insert your normal Odoo logic here ---
        # For testing, just return what was received:
        return jsonify({
            "status": "ok",
            "debug_echo": {
                "name": name,
                "email": email,
                "city": city,
                "province": province,
                "products": products,
                "message": message
            }
        })

    except Exception as e:
        print("❌ Exception occurred:")
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    print("🚀 Flask webhook server starting...")
    app.run(host="0.0.0.0", port=8080, debug=True)
