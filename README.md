# wix-odoo-webhook

## Daily Shipment Email Job

`daily_shipment_report.py` queries Odoo for completed outbound shipments since the last successful run and emails a summary that includes:
- what orders shipped
- what products shipped
- what is still backordered
- direct sales order links

### 1. Set environment variables

Use your shell profile or a cron-specific env file:

```bash
export SHIPMENT_REPORT_RECIPIENTS="jason@wavcor.ca,jgriffit@gmail.com"
export SHIPMENT_REPORT_SENDER="jason@wavcor.ca"
export SMTP_HOST="smtp.office365.com"
export SMTP_PORT="587"
export SMTP_USERNAME="jason@wavcor.ca"
export SMTP_PASSWORD="ZTg0131517!"
export SMTP_USE_TLS="true"
export SHIPMENT_REPORT_TIMEZONE="America/Regina"
```

Optional:

```bash
export SHIPMENT_REPORT_SEND_EMPTY="false"
export SHIPMENT_REPORT_FIRST_RUN_LOOKBACK_HOURS="24"
export SHIPMENT_REPORT_STATE_FILE=".shipment_report_state.json"
```

### 2. Test once manually

```bash
SHIPMENT_REPORT_DRY_RUN=true python3 daily_shipment_report.py
```

### 3. Run automatically every day at 5:00 PM

Edit cron:

```bash
crontab -e
```

Add:

```cron
0 17 * * * cd "/Users/jasongriffith/Library/CloudStorage/OneDrive-SharedLibraries-WavcorInternationalInc/Wavcor International - Engineering/Software/Lead Tools/wix-odoo-webhook" && /bin/zsh -lc 'source ~/.zshrc && python3 daily_shipment_report.py >> daily_shipment_report.log 2>&1'
```

This uses the system timezone configured on the machine and runs at 17:00 (5 PM) daily.
