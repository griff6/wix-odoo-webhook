# webhook_server.py
from flask import Flask, request, jsonify
from odoo_connector import (
    create_odoo_contact,
    update_odoo_contact,
    find_existing_contact,
    create_odoo_opportunity,
    find_existing_opportunity,
    connect_odoo,
    get_or_create_opportunity_tags,
    update_odoo_opportunity,
    find_odoo_user_id,
    create_odoo_activity,
    get_model_id,
    ODOO_URL,
)
from datetime import datetime, timedelta
from geopy.geocoders import Nominatim
import traceback

app = Flask(__name__)
geolocator = Nominatim(user_agent="WavcorWebhook")

def get_lat_lon_from_address(city, province_state, country="Canada"):
    try:
        location = geolocator.geocode(f"{city}, {province_state}, {country}", timeout=10)
        if location:
            return location.latitude, location.longitude
    except Exception as e:
        print("Geocode error:", e)
    return None, None


@app.route("/wix_form_webhook", methods=["POST"])
def handle_form():
    try:
        data = request.get_json(force=True)
        print("Received Wix form data:", data)

        # Normalize fields to your current data structure
        customer = {
            "Name": f"{data.get('firstName', '')} {data.get('lastName', '')}".strip(),
            "Email": data.get("email", ""),
            "Phone": data.get("phone", ""),
            "City": data.get("city", ""),
            "Prov/State": data.get("provinceState", ""),
            "Products Interest": [data.get("productInterest", "")],
            "Message": data.get("message", ""),
        }

        # --- Contact handling ---
        existing_contact = find_existing_contact(customer)
        if existing_contact:
            print(f"Updating existing contact: {existing_contact['name']}")
            update_odoo_contact(existing_contact["id"], customer)
            contact_id = existing_contact["id"]
        else:
            print(f"Creating new contact: {customer['Name']}")
            contact_id = create_odoo_contact(customer)

        if not contact_id:
            return jsonify({"status": "error", "message": "Failed to create or update contact"}), 400

        # --- Opportunity handling ---
        opportunity_name = customer["Name"]
        existing_opp = find_existing_opportunity(opportunity_name)
        uid, models = connect_odoo()

        if not uid:
            return jsonify({"status": "error", "message": "Failed to connect to Odoo"}), 500

        tags = get_or_create_opportunity_tags(models, uid, customer["Products Interest"])

        if existing_opp:
            print("Updating existing opportunity...")
            update_odoo_opportunity(
                existing_opp["id"],
                {
                    "partner_id": contact_id,
                    "description": customer["Message"],
                    "tag_ids": [(6, 0, tags)] if tags else False,
                },
            )
            opportunity_id = existing_opp["id"]
        else:
            print("Creating new opportunity...")
            opportunity_id = create_odoo_opportunity(
                {
                    "name": opportunity_name,
                    "partner_id": contact_id,
                    "description": customer["Message"],
                    "tag_ids": [(6, 0, tags)] if tags else False,
                }
            )

        # --- Optional: create follow-up activity ---
        hal_id = find_odoo_user_id(models, uid, "Hal Pepler")
        if hal_id and opportunity_id:
            model_id = get_model_id(models, uid, "crm.lead")
            if model_id:
                activity = {
                    "res_model_id": model_id,
                    "res_id": opportunity_id,
                    "user_id": hal_id,
                    "summary": "Follow up on form submission",
                    "date_deadline": (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d"),
                    "note": f"Auto-created follow-up for {opportunity_name}",
                }
                create_odoo_activity(models, uid, activity)

        print(f"✅ Processed successfully for {customer['Name']}")
        return jsonify({"status": "ok", "contact_id": contact_id, "opportunity_id": opportunity_id})

    except Exception as e:
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
