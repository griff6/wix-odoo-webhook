from flask import Flask, request, jsonify
import json, traceback, sys
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
    print("🔔 Received webhook request", file=sys.stdout, flush=True)
    app.logger.info("🔔 Received webhook request")
    try:
        # Log request headers and raw data
        headers = dict(request.headers)
        print("Request headers:", headers, file=sys.stdout, flush=True)
        app.logger.info(f"Request headers: {headers}")

        raw_body = request.data.decode("utf-8")
        print("Raw body:", raw_body, file=sys.stdout, flush=True)
        app.logger.info(f"Raw body: {raw_body}")

        # Try parsing JSON
        data = request.get_json(force=True)
        print("✅ Parsed JSON data:", json.dumps(data, indent=2), file=sys.stdout, flush=True)
        app.logger.info(f"✅ Parsed JSON data: {json.dumps(data, indent=2)}")

        # Extract fields (adjust names if needed)
        name = f"{data.get('firstName', '')} {data.get('lastName', '')}".strip()
        email = data.get("email")
        phone = data.get("phone")
        city = data.get("city")
        province = data.get("provinceState")
        products = data.get("productInterest", "")
        message = data.get("message", "")

        print(f"DEBUG parsed fields → Name: {name}, Email: {email}, City: {city}, Province: {province}", file=sys.stdout, flush=True)
        print(f"DEBUG Message: {message}", file=sys.stdout, flush=True)

        # Echo data back for testing
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
        }), 200

    except Exception as e:
        print("❌ Exception occurred:", file=sys.stdout, flush=True)
        traceback.print_exc(file=sys.stdout)
        sys.stdout.flush()
        app.logger.error(f"Exception: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    print("🚀 Flask webhook server starting...", file=sys.stdout, flush=True)
    app.run(host="0.0.0.0", port=8080, debug=True)
