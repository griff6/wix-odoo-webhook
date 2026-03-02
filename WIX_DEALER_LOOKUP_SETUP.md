## Wix Dealer Lookup Page Setup

### 1) Add page elements in Wix Editor
Create a page and add:
- Text Input: `#cityInput`
- Dropdown: `#provinceDropdown`
- Button: `#lookupButton`
- Text: `#resultText`

For `#provinceDropdown`, use values like:
- `AB`, `BC`, `MB`, `NB`, `NL`, `NS`, `NT`, `NU`, `ON`, `PE`, `QC`, `SK`, `YT`

### 2) Page Code (Velo)
Paste this into the page code panel:

```js
$w.onReady(function () {
  $w("#resultText").text = "";

  $w("#lookupButton").onClick(async () => {
    const city = ($w("#cityInput").value || "").trim();
    const province = ($w("#provinceDropdown").value || "").trim();

    if (!city || !province) {
      $w("#resultText").text = "Enter both city and province.";
      return;
    }

    $w("#lookupButton").disable();
    $w("#resultText").text = "Looking up nearest dealer...";

    try {
      const response = await fetch("https://YOUR-RENDER-URL/nearest_dealer", {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
          // If you set DEALER_LOOKUP_API_KEY on Render, add:
          // "X-API-Key": "YOUR_KEY"
        },
        body: JSON.stringify({ city, province })
      });

      const data = await response.json();

      if (data.status !== "ok" || !data.dealer) {
        $w("#resultText").text = data.message || "No dealer found.";
        return;
      }

      const d = data.dealer;
      $w("#resultText").text =
        `Closest Dealer: ${d.location || "N/A"}\n` +
        `Contact: ${d.contact || "N/A"}\n` +
        `Phone: ${d.phone || "N/A"}\n` +
        `Drive Distance: ${d.distance_km ?? "N/A"} km\n` +
        `Drive Time: ${d.drive_time_hr ?? "N/A"} hr`;
    } catch (err) {
      $w("#resultText").text = "Lookup failed. Try again.";
      console.error(err);
    } finally {
      $w("#lookupButton").enable();
    }
  });
});
```

### 3) Backend endpoint
Your Flask app now exposes:
- `POST /nearest_dealer`

Request body:

```json
{
  "city": "Lumsden",
  "province": "SK"
}
```

Response includes:
- `dealer.location`
- `dealer.contact`
- `dealer.phone`
- `dealer.distance_km`
- `dealer.drive_time_hr`

### 4) Optional security
If you set environment variable `DEALER_LOOKUP_API_KEY` on Render, the endpoint requires header:
- `X-API-Key: <your key>`
