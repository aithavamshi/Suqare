# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c4d43324-5c08-441c-8aa3-3bff9e40e537",
# META       "default_lakehouse_name": "restaurant_lh",
# META       "default_lakehouse_workspace_id": "63a177d1-d524-4da3-ab74-da6dc7a0df48",
# META       "known_lakehouses": [
# META         {
# META           "id": "c4d43324-5c08-441c-8aa3-3bff9e40e537"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# **Configuration**

# CELL ********************

import requests
import json
from datetime import datetime, timezone
from pathlib import Path  

print("Fabric CI/CD Demo - Version 2")
print("Hello")
print("This is my new change")
print("Hii")

print("this is the change")

#testing in the new branch
# ============================================================
# SQUARE CONFIGURATION
# ============================================================

SQUARE_BASE_URL = "https://connect.squareupsandbox.com"

SQUARE_ACCESS_TOKEN = "EAAAl6mpp8GtAKOEO128xD-Ma5FRfLfGjrduVVpIfT7aG48DwKKQKCcW6Yqdg7Hm"


SQUARE_VERSION = "2026-07-15"

LOCATION_ID = "LMCQ87A8CY2GG"

HEADERS = {
    "Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}",
    "Square-Version": SQUARE_VERSION,
    "Content-Type": "application/json"
}

print("Square configuration loaded.")
print("Base URL:", SQUARE_BASE_URL)
print("Location:", LOCATION_ID)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Authentication test**

# CELL ********************

response = requests.get(
    f"{SQUARE_BASE_URL}/v2/locations",
    headers=HEADERS,
    timeout=30
)

print("HTTP Status:", response.status_code)

if response.ok:
    print("✅ Square API authentication successful.")
else:
    print("❌ Square API authentication failed.")
    print(response.text)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Orders API test**

# CELL ********************

orders_url = f"{SQUARE_BASE_URL}/v2/orders/search"

payload = {
    "location_ids": [
        LOCATION_ID
    ],
    "limit": 2000,
    "return_entries": False
}

response = requests.post(
    orders_url,
    headers=HEADERS,
    json=payload,
    timeout=30
)

print("HTTP Status:", response.status_code)

if response.ok:
    orders_response = response.json()

    print("✅ Orders API call successful.")
    print(
        "Orders returned:",
        len(orders_response.get("orders", []))
    )

    print(
        "Cursor:",
        orders_response.get("cursor")
    )

else:
    print("❌ Orders API failed.")
    print(response.text)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

import requests

orders_url = f"{SQUARE_BASE_URL}/v2/orders/search"

all_orders = []
cursor = None

while True:

    payload = {
        "location_ids": [
            LOCATION_ID
        ],
        "limit": 1000,
        "return_entries": False
    }

    # Add cursor only after the first request
    if cursor:
        payload["cursor"] = cursor

    response = requests.post(
        orders_url,
        headers=HEADERS,
        json=payload,
        timeout=30
    )

    print("HTTP Status:", response.status_code)

    if not response.ok:
        print("❌ Orders API failed.")
        print(response.text)
        break

    orders_response = response.json()

    orders = orders_response.get("orders", [])

    print(
        f"Orders received in this page: {len(orders)}"
    )

    all_orders.extend(orders)

    cursor = orders_response.get("cursor")

    print(
        "Next cursor:",
        cursor
    )

    # No cursor means there are no more pages
    if not cursor:
        break

print("===================================")
print("✅ Orders API extraction completed")
print("Total orders:", len(all_orders))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Inspect one order**

# CELL ********************

orders = orders_response.get("orders", [])

if orders:
    print(
        json.dumps(
            orders[0],
            indent=2
        )
    )
else:
    print("⚠️ No orders returned.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Save raw response**

# CELL ********************

bronze_path = "/lakehouse/default/Files/square/orders"

Path(bronze_path).mkdir(
    parents=True,
    exist_ok=True
)

timestamp = datetime.now(timezone.utc).strftime(
    "%Y%m%d_%H%M%S"
)

file_path = (
    f"{bronze_path}/"
    f"orders_{timestamp}.json"
)

with open(
    file_path,
    "w",
    encoding="utf-8"
) as f:

    json.dump(
        orders_response,
        f,
        indent=2
    )

print("✅ Raw Orders response saved.")
print("File:", file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Orders**

# CELL ********************

import requests
import json
from datetime import datetime, timezone
from pathlib import Path

# ============================================================
# SQUARE ORDERS INGESTION
# ============================================================

ORDERS_URL = f"{SQUARE_BASE_URL}/v2/orders/search"

all_orders = []

cursor = None
page_number = 0

LIMIT = 1000

while True:

    page_number += 1

    payload = {
        "location_ids": [
            LOCATION_ID
        ],

        "limit": LIMIT,

        "return_entries": False
    }

    # --------------------------------------------------------
    # Add cursor only after first page
    # --------------------------------------------------------

    if cursor:

        payload["cursor"] = cursor

    # --------------------------------------------------------
    # API request
    # --------------------------------------------------------

    response = requests.post(
        ORDERS_URL,
        headers=HEADERS,
        json=payload,
        timeout=60
    )

    print(
        f"Page {page_number} "
        f"| HTTP {response.status_code}"
    )

    response.raise_for_status()

    data = response.json()

    # --------------------------------------------------------
    # Get orders
    # --------------------------------------------------------

    page_orders = data.get(
        "orders",
        []
    )

    print(
        f"Orders returned: "
        f"{len(page_orders)}"
    )

    # --------------------------------------------------------
    # Add to complete collection
    # --------------------------------------------------------

    all_orders.extend(
        page_orders
    )

    # --------------------------------------------------------
    # Get next cursor
    # --------------------------------------------------------

    cursor = data.get(
        "cursor"
    )

    if cursor:

        print(
            "More orders available..."
        )

    else:

        print(
            "No more pages."
        )

        break


# ============================================================
# SUMMARY
# ============================================================

print(
    "\n========================================"
)

print(
    "SQUARE ORDERS INGESTION SUMMARY"
)

print(
    "========================================"
)

print(
    f"Pages retrieved : {page_number}"
)

print(
    f"Total orders    : {len(all_orders)}"
)

print(
    "========================================"
)

bronze_path = (
    "/lakehouse/default/"
    "Files/square/orders"
)

Path(
    bronze_path
).mkdir(
    parents=True,
    exist_ok=True
)

timestamp = datetime.now(
    timezone.utc
).strftime(
    "%Y%m%d_%H%M%S"
)

file_path = (
    f"{bronze_path}/"
    f"orders_{timestamp}.json"
)

raw_orders = {
    "ingested_at": datetime.now(
        timezone.utc
    ).isoformat(),

    "source": "Square Orders API",

    "location_id": LOCATION_ID,

    "record_count": len(all_orders),

    "orders": all_orders
}

with open(
    file_path,
    "w",
    encoding="utf-8"
) as f:

    json.dump(
        raw_orders,
        f,
        indent=2
    )

print(
    "✅ Complete raw Orders response saved."
)

print(
    "File:",
    file_path
)

print(
    "Records:",
    len(all_orders)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ****customers****

# CELL ********************

# ============================================================
# SQUARE CUSTOMERS INGESTION
# ============================================================

CUSTOMERS_URL = (
    f"{SQUARE_BASE_URL}/v2/customers"
)

all_customers = []

cursor = None
page_number = 0

LIMIT = 100

while True:

    page_number += 1

    params = {
        "limit": LIMIT
    }

    if cursor:
        params["cursor"] = cursor

    response = requests.get(
        CUSTOMERS_URL,
        headers=HEADERS,
        params=params,
        timeout=60
    )

    print(
        f"Page {page_number} "
        f"| HTTP {response.status_code}"
    )

    response.raise_for_status()

    data = response.json()

    page_customers = data.get(
        "customers",
        []
    )

    print(
        f"Customers returned: "
        f"{len(page_customers)}"
    )

    all_customers.extend(
        page_customers
    )

    cursor = data.get(
        "cursor"
    )

    if cursor:
        print(
            "More customers available..."
        )
    else:
        print(
            "No more pages."
        )
        break


print(
    "\n========================================"
)

print(
    "SQUARE CUSTOMERS INGESTION SUMMARY"
)

print(
    "========================================"
)

print(
    f"Pages retrieved : {page_number}"
)

print(
    f"Total customers : {len(all_customers)}"
)

print(
    "========================================"
)
# ============================================================
# SAVE RAW CUSTOMERS RESPONSE
# ============================================================

customers_bronze_path = (
    "/lakehouse/default/"
    "Files/square/customers"
)

Path(
    customers_bronze_path
).mkdir(
    parents=True,
    exist_ok=True
)

timestamp = datetime.now(
    timezone.utc
).strftime(
    "%Y%m%d_%H%M%S"
)

customers_file_path = (
    f"{customers_bronze_path}/"
    f"customers_{timestamp}.json"
)

raw_customers = {

    "ingested_at":
        datetime.now(
            timezone.utc
        ).isoformat(),

    "source":
        "Square Customers API",

    "record_count":
        len(all_customers),

    "customers":
        all_customers
}

with open(
    customers_file_path,
    "w",
    encoding="utf-8"
) as f:

    json.dump(
        raw_customers,
        f,
        indent=2
    )

print(
    "✅ Raw Customers response saved."
)

print(
    "File:",
    customers_file_path
)

print(
    "Records:",
    len(all_customers)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **catalog**

# CELL ********************

# ============================================================
# SQUARE CATALOG INGESTION
# ============================================================

CATALOG_URL = (
    f"{SQUARE_BASE_URL}/v2/catalog/search"
)

all_catalog_objects = []

cursor = None
page_number = 0

LIMIT = 1000

while True:

    page_number += 1

    payload = {
        "object_types": [
            "ITEM",
            "ITEM_VARIATION"
        ],

        "limit": LIMIT
    }

    # --------------------------------------------------------
    # Pagination
    # --------------------------------------------------------

    if cursor:

        payload["cursor"] = cursor

    # --------------------------------------------------------
    # API REQUEST
    # --------------------------------------------------------

    response = requests.post(
        CATALOG_URL,
        headers=HEADERS,
        json=payload,
        timeout=60
    )

    print(
        f"Page {page_number} "
        f"| HTTP {response.status_code}"
    )

    response.raise_for_status()

    data = response.json()

    # --------------------------------------------------------
    # Catalog objects
    # --------------------------------------------------------

    page_objects = data.get(
        "objects",
        []
    )

    print(
        f"Catalog objects returned: "
        f"{len(page_objects)}"
    )

    all_catalog_objects.extend(
        page_objects
    )

    # --------------------------------------------------------
    # Next cursor
    # --------------------------------------------------------

    cursor = data.get(
        "cursor"
    )

    if cursor:

        print(
            "More catalog objects available..."
        )

    else:

        print(
            "No more pages."
        )

        break


# ============================================================
# SUMMARY
# ============================================================

print(
    "\n========================================"
)

print(
    "SQUARE CATALOG INGESTION SUMMARY"
)

print(
    "========================================"
)

print(
    f"Pages retrieved  : {page_number}"
)

print(
    f"Total objects    : "
    f"{len(all_catalog_objects)}"
)

print(
    "========================================"
)

# ============================================================
# CATALOG OBJECT TYPE SUMMARY
# ============================================================

from collections import Counter

object_types = Counter(
    obj.get("type")
    for obj in all_catalog_objects
)

print(
    "Catalog object types:"
)

for object_type, count in object_types.items():

    print(
        f"{object_type}: {count}"
    )


# ============================================================
# SAVE RAW CATALOG RESPONSE
# ============================================================

catalog_bronze_path = (
    "/lakehouse/default/"
    "Files/square/catalog"
)

Path(
    catalog_bronze_path
).mkdir(
    parents=True,
    exist_ok=True
)

timestamp = datetime.now(
    timezone.utc
).strftime(
    "%Y%m%d_%H%M%S"
)

catalog_file_path = (
    f"{catalog_bronze_path}/"
    f"catalog_{timestamp}.json"
)

raw_catalog = {

    "ingested_at":
        datetime.now(
            timezone.utc
        ).isoformat(),

    "source":
        "Square Catalog API",

    "record_count":
        len(all_catalog_objects),

    "objects":
        all_catalog_objects
}

with open(
    catalog_file_path,
    "w",
    encoding="utf-8"
) as f:

    json.dump(
        raw_catalog,
        f,
        indent=2
    )

print(
    "✅ Raw Catalog response saved."
)

print(
    "File:",
    catalog_file_path
)

print(
    "Objects:",
    len(all_catalog_objects)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Payments**

# CELL ********************

# ============================================================
# SQUARE PAYMENTS INGESTION
# ============================================================

PAYMENTS_URL = (
    f"{SQUARE_BASE_URL}/v2/payments"
)

all_payments = []

cursor = None
page_number = 0

LIMIT = 100

while True:

    page_number += 1

    params = {
        "limit": LIMIT
    }

    # --------------------------------------------------------
    # Pagination
    # --------------------------------------------------------

    if cursor:

        params["cursor"] = cursor

    # --------------------------------------------------------
    # API REQUEST
    # --------------------------------------------------------

    response = requests.get(
        PAYMENTS_URL,
        headers=HEADERS,
        params=params,
        timeout=60
    )

    print(
        f"Page {page_number} "
        f"| HTTP {response.status_code}"
    )

    response.raise_for_status()

    data = response.json()

    # --------------------------------------------------------
    # Payments
    # --------------------------------------------------------

    page_payments = data.get(
        "payments",
        []
    )

    print(
        f"Payments returned: "
        f"{len(page_payments)}"
    )

    all_payments.extend(
        page_payments
    )

    # --------------------------------------------------------
    # Next cursor
    # --------------------------------------------------------

    cursor = data.get(
        "cursor"
    )

    if cursor:

        print(
            "More payments available..."
        )

    else:

        print(
            "No more pages."
        )

        break


# ============================================================
# SUMMARY
# ============================================================

print(
    "\n========================================"
)

print(
    "SQUARE PAYMENTS INGESTION SUMMARY"
)

print(
    "========================================"
)

print(
    f"Pages retrieved : {page_number}"
)

print(
    f"Total payments  : {len(all_payments)}"
)

print(
    "========================================"
)


# ============================================================
# SAVE RAW PAYMENTS RESPONSE
# ============================================================

payments_bronze_path = (
    "/lakehouse/default/"
    "Files/square/payments"
)

Path(
    payments_bronze_path
).mkdir(
    parents=True,
    exist_ok=True
)

timestamp = datetime.now(
    timezone.utc
).strftime(
    "%Y%m%d_%H%M%S"
)

payments_file_path = (
    f"{payments_bronze_path}/"
    f"payments_{timestamp}.json"
)

raw_payments = {

    "ingested_at":
        datetime.now(
            timezone.utc
        ).isoformat(),

    "source":
        "Square Payments API",

    "record_count":
        len(all_payments),

    "payments":
        all_payments
}

with open(
    payments_file_path,
    "w",
    encoding="utf-8"
) as f:

    json.dump(
        raw_payments,
        f,
        indent=2
    )

print(
    "✅ Raw Payments response saved."
)

print(
    "File:",
    payments_file_path
)

print(
    "Records:",
    len(all_payments)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from collections import Counter

payment_statuses = Counter(
    payment.get("status")
    for payment in all_payments
)

payment_sources = Counter(
    payment.get("source_type")
    for payment in all_payments
)

print("Payment statuses:")

for status, count in payment_statuses.items():
    print(f"{status}: {count}")

print("\nPayment source types:")

for source_type, count in payment_sources.items():
    print(f"{source_type}: {count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# BUILD INVENTORY CATALOG VARIATION LIST
# ============================================================

inventory_variations = []

for obj in all_catalog_objects:

    if obj.get("type") != "ITEM_VARIATION":
        continue

    variation_data = obj.get("item_variation_data", {})

    variation_id = obj.get("id")
    item_id = variation_data.get("item_id")
    sku = variation_data.get("sku")

    if variation_id:
        inventory_variations.append({
            "catalog_object_id": variation_id,
            "item_id": item_id,
            "sku": sku
        })

unique_variation_ids = len(
    set(x["catalog_object_id"] for x in inventory_variations)
)

print("========================================")
print("INVENTORY CATALOG VARIATIONS")
print("========================================")
print(f"Total variations: {len(inventory_variations)}")
print(f"Unique variation IDs: {unique_variation_ids}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Inventory**

# CELL ********************

# ============================================================
# SQUARE INVENTORY BATCH RETRIEVE
# ============================================================

INVENTORY_URL = (
    f"{SQUARE_BASE_URL}/v2/inventory/counts/batch-retrieve"
)

catalog_object_ids = [
    item["catalog_object_id"]
    for item in inventory_variations
]

inventory_payload = {

    "catalog_object_ids":
        catalog_object_ids,

    "location_ids": [
        LOCATION_ID
    ]
}

print(
    "\n========================================"
)

print(
    "SQUARE INVENTORY REQUEST"
)

print(
    "========================================"
)

print(
    f"Catalog objects : "
    f"{len(catalog_object_ids)}"
)

print(
    f"Location        : "
    f"{LOCATION_ID}"
)

response = requests.post(
    INVENTORY_URL,
    headers=HEADERS,
    json=inventory_payload,
    timeout=60
)

print(
    f"HTTP Status: "
    f"{response.status_code}"
)

response.raise_for_status()

inventory_response = response.json()

print(
    "\n========== RAW INVENTORY RESPONSE =========="
)

print(
    json.dumps(
        inventory_response,
        indent=2
    )
)

# ============================================================
# SAVE RAW INVENTORY RESPONSE
# ============================================================

inventory_bronze_path = (
    "/lakehouse/default/"
    "Files/square/inventory"
)

Path(
    inventory_bronze_path
).mkdir(
    parents=True,
    exist_ok=True
)

timestamp = datetime.now(
    timezone.utc
).strftime(
    "%Y%m%d_%H%M%S"
)

inventory_file_path = (
    f"{inventory_bronze_path}/"
    f"inventory_{timestamp}.json"
)

raw_inventory = {

    "ingested_at":
        datetime.now(
            timezone.utc
        ).isoformat(),

    "source":
        "Square Inventory API",

    "location_id":
        LOCATION_ID,

    "requested_variations":
        len(catalog_object_ids),

    "returned_counts":
        len(inventory_counts),

    "counts":
        inventory_counts
}

with open(
    inventory_file_path,
    "w",
    encoding="utf-8"
) as f:

    json.dump(
        raw_inventory,
        f,
        indent=2
    )

print(
    "\n========================================"
)

print(
    "✅ RAW INVENTORY RESPONSE SAVED"
)

print(
    "========================================"
)

print(
    "File:"
)

print(
    inventory_file_path
)


# ============================================================
# INVENTORY RESPONSE VALIDATION
# ============================================================

inventory_counts = inventory_response.get(
    "counts",
    []
)

print(
    "\n========================================"
)

print(
    "INVENTORY DISCOVERY SUMMARY"
)

print(
    "========================================"
)

print(
    f"Requested variations : "
    f"{len(catalog_object_ids)}"
)

print(
    f"Square counts returned: "
    f"{len(inventory_counts)}"
)

#print(f"Records: "f"{len(inventory_counts)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from collections import Counter

inventory_states = Counter(
    count.get("state")
    for count in inventory_counts
)

print(
    "\nInventory states:"
)

for state, count in inventory_states.items():

    print(
        f"{state}: {count}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Employees**

# CELL ********************

# ============================================================
# SQUARE EMPLOYEES INGESTION
# ============================================================

EMPLOYEES_URL = (
    f"{SQUARE_BASE_URL}/v2/employees"
)

all_employees = []

cursor = None
page_number = 0

LIMIT = 100

while True:

    page_number += 1

    params = {
        "limit": LIMIT
    }

    if cursor:
        params["cursor"] = cursor

    response = requests.get(
        EMPLOYEES_URL,
        headers=HEADERS,
        params=params,
        timeout=60
    )

    print(
        f"Page {page_number} "
        f"| HTTP {response.status_code}"
    )

    response.raise_for_status()

    data = response.json()

    page_employees = data.get(
        "employees",
        []
    )

    print(
        f"Employees returned: "
        f"{len(page_employees)}"
    )

    all_employees.extend(
        page_employees
    )

    cursor = data.get("cursor")

    if cursor:
        print("More employees available...")
    else:
        print("No more pages.")
        break


# ============================================================
# SUMMARY
# ============================================================

print(
    "\n========================================"
)

print(
    "SQUARE EMPLOYEES INGESTION SUMMARY"
)

print(
    "========================================"
)

print(
    f"Pages retrieved : {page_number}"
)

print(
    f"Total employees : {len(all_employees)}"
)

print(
    "========================================"
)

# ============================================================
# SAVE RAW EMPLOYEES RESPONSE
# ============================================================

employees_bronze_path = (
    "/lakehouse/default/"
    "Files/square/employees"
)

Path(
    employees_bronze_path
).mkdir(
    parents=True,
    exist_ok=True
)

timestamp = datetime.now(
    timezone.utc
).strftime(
    "%Y%m%d_%H%M%S"
)

employees_file_path = (
    f"{employees_bronze_path}/"
    f"employees_{timestamp}.json"
)

raw_employees = {

    "ingested_at":
        datetime.now(
            timezone.utc
        ).isoformat(),

    "source":
        "Square Employees API",

    "record_count":
        len(all_employees),

    "employees":
        all_employees
}

with open(
    employees_file_path,
    "w",
    encoding="utf-8"
) as f:

    json.dump(
        raw_employees,
        f,
        indent=2
    )

print(
    "\n========================================"
)

print(
    "✅ RAW EMPLOYEES RESPONSE SAVED"
)

print(
    "========================================"
)

print(
    "File:",
    employees_file_path
)

print(
    "Records:",
    len(all_employees)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if all_employees:

    print(
        json.dumps(
            all_employees[0],
            indent=2
        )
    )

else:

    print(
        "⚠️ No employees returned."
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Location**

# CELL ********************

import requests
import json

locations_url = f"{SQUARE_BASE_URL}/v2/locations"

response = requests.get(
    locations_url,
    headers=HEADERS,
    timeout=30
)

print("HTTP Status:", response.status_code)

if response.ok:

    locations_response = response.json()

    print("✅ Locations API call successful.")

    print(
        "Locations returned:",
        len(locations_response.get("locations", []))
    )

    # Convert Python dictionary → VALID JSON
    json_string = json.dumps(
        locations_response,
        indent=2
    )

    # Save JSON
    mssparkutils.fs.put(
        "Files/square/location/locations.json",
        json_string,
        True
    )

    print("✅ Raw locations JSON saved.")

else:

    print("❌ Locations API failed.")
    print(response.text)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json
import os
from datetime import datetime

ACCESS_TOKEN = "EAAAl6mpp8GtAKOEO128xD-Ma5FRfLfGjrduVVpIfT7aG48DwKKQKCcW6Yqdg7Hm"
BASE_URL = "https://connect.squareupsandbox.com"

headers = {
    "Square-Version": "2026-07-15",
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

print("========================================")
print("LOCATION BRONZE INGESTION")
print("========================================")

# ============================================================
# STEP 1 — FETCH LOCATIONS (no pagination needed)
# ============================================================

response = requests.get(f"{BASE_URL}/v2/locations", headers=headers)

print("Status Code:", response.status_code)

data = response.json()

if "errors" in data:
    print("Square returned an error:", data["errors"])
    raise SystemExit("Stopping — Square API returned an error.")

locations = data.get("locations", [])

print(f"Total locations fetched: {len(locations)}")

# ============================================================
# STEP 2 — ADD INGESTION TIMESTAMP
# ============================================================

data["ingested_at"] = datetime.now().isoformat()

# ============================================================
# STEP 3 — SAVE RAW JSON TO BRONZE (Files layer)
# ============================================================

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

bronze_folder = "/lakehouse/default/Files/square/location/"
bronze_filename = f"location_{timestamp}.json"
bronze_path = bronze_folder + bronze_filename

os.makedirs(bronze_folder, exist_ok=True)

with open(bronze_path, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2)

print("========================================")
print("✅ BRONZE FILE SAVED")
print("========================================")
print("Saved to:", bronze_path)
print("Locations in file:", len(locations))
print("Ingested at:", data["ingested_at"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
