# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "297b840a-87f4-404f-94a9-d2e5fe349e36",
# META       "default_lakehouse_name": "square_LH",
# META       "default_lakehouse_workspace_id": "63a177d1-d524-4da3-ab74-da6dc7a0df48",
# META       "known_lakehouses": [
# META         {
# META           "id": "297b840a-87f4-404f-94a9-d2e5fe349e36"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# **Customer**

# CELL ********************

import requests

ACCESS_TOKEN = "EAAAl6mpp8GtAKOEO128xD-Ma5FRfLfGjrduVVpIfT7aG48DwKKQKCcW6Yqdg7Hm"
BASE_URL = "https://connect.squareupsandbox.com"

headers = {
    "Square-Version": "2025-07-16",
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

# ============================================================
# STEP 1 — FETCH ALL CUSTOMERS (handles pagination properly)
# ============================================================

all_customers = []
cursor = None

while True:
    params = {"limit": 100, "count": True}
    if cursor:
        params["cursor"] = cursor

    response = requests.get(f"{BASE_URL}/v2/customers", headers=headers, params=params)
    data = response.json()

    if "errors" in data:
        print("Square returned an error:", data["errors"])
        break

    page_customers = data.get("customers", [])
    all_customers.extend(page_customers)

    print(f"Fetched {len(page_customers)} customers (running total: {len(all_customers)})")

    cursor = data.get("cursor")
    if not cursor:
        break

print(f"\nTotal customers fetched: {len(all_customers)}")
print("Total customers from API:", data.get("count"))


# ============================================================
# STEP 2 — FLATTEN CUSTOMERS IN PLAIN PYTHON
# ============================================================

customer_rows = []

for c in all_customers:

    address = c.get("address", {})
    preferences = c.get("preferences", {})

    customer_rows.append({
        "square_customer_id": c.get("id"),
        "created_at": c.get("created_at"),
        "updated_at": c.get("updated_at"),
        "given_name": c.get("given_name"),
        "family_name": c.get("family_name"),
        "email_address": c.get("email_address"),
        "phone_number": c.get("phone_number"),
        "reference_id": c.get("reference_id"),
        "company_name": c.get("company_name"),
        "note": c.get("note"),
        "creation_source": c.get("creation_source"),
        "version": c.get("version"),

        "address_line_1": address.get("address_line_1"),
        "address_line_2": address.get("address_line_2"),
        "locality": address.get("locality"),
        "administrative_district_level_1": address.get("administrative_district_level_1"),
        "postal_code": address.get("postal_code"),
        "country": address.get("country"),

        "email_unsubscribed": preferences.get("email_unsubscribed")
    })

print(f"\nTotal flattened customer rows: {len(customer_rows)}")


# ============================================================
# STEP 3 — BUILD THE SPARK DATAFRAME (with schema)
# ============================================================

from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType
from pyspark.sql import functions as F

customer_schema = StructType([
    StructField("square_customer_id", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("given_name", StringType(), True),
    StructField("family_name", StringType(), True),
    StructField("email_address", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("reference_id", StringType(), True),
    StructField("company_name", StringType(), True),
    StructField("note", StringType(), True),
    StructField("creation_source", StringType(), True),
    StructField("version", LongType(), True),
    StructField("address_line_1", StringType(), True),
    StructField("address_line_2", StringType(), True),
    StructField("locality", StringType(), True),
    StructField("administrative_district_level_1", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("email_unsubscribed", BooleanType(), True)
])

if len(customer_rows) == 0:
    print("No customers found.")
else:
    df_customers = spark.createDataFrame(customer_rows, schema=customer_schema)

    df_customers = df_customers.withColumn("created_at", F.to_timestamp("created_at"))
    df_customers = df_customers.withColumn("updated_at", F.to_timestamp("updated_at"))

    df_customers.printSchema()
    display(df_customers)


# ============================================================
# STEP 4 — SAVE TO THE LAKEHOUSE
# ============================================================

if len(customer_rows) > 0:
    df_customers.write.format("delta").mode("overwrite").saveAsTable("SquareCustomers")
    print("Saved: SquareCustomers")
else:
    print("Skipped saving SquareCustomers — no customers found.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **categories**

# CELL ********************

import requests

#ACCESS_TOKEN = "EAAAlzu3caSyQQMdUZqrlOK4k0RvK9YOKUu3KJ36Hf2PHQVZD3c0P5D2Iy9HWppJ"
ACCESS_TOKEN = "EAAAl6mpp8GtAKOEO128xD-Ma5FRfLfGjrduVVpIfT7aG48DwKKQKCcW6Yqdg7Hm"

BASE_URL = "https://connect.squareupsandbox.com"

headers = {
    "Square-Version": "2026-07-15",
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

# ============================================================
# STEP 1 — FETCH ALL CATEGORIES (handles pagination properly)
# ============================================================

all_categories = []
cursor = None

while True:
    params = {"types": "CATEGORY"}
    if cursor:
        params["cursor"] = cursor

    response = requests.get(
        f"{BASE_URL}/v2/catalog/list",
        headers=headers,
        params=params
    )

    data = response.json()

    if "errors" in data:
        print("Square returned an error:", data["errors"])
        break

    page_categories = data.get("objects", [])
    all_categories.extend(page_categories)

    print(f"Fetched {len(page_categories)} categories (running total: {len(all_categories)})")

    cursor = data.get("cursor")
    if not cursor:
        break

print(f"\nTotal categories fetched: {len(all_categories)}")


# ============================================================
# STEP 2 — FLATTEN category_data IN PLAIN PYTHON
# ============================================================

flat_rows = []

for category in all_categories:

    category_data = category.get("category_data", {})
    parent_category = category_data.get("parent_category", {})

    flat_rows.append({
        "square_category_id": category.get("id"),
        "type": category.get("type"),
        "version": category.get("version"),
        "is_deleted": category.get("is_deleted"),
        "present_at_all_locations": category.get("present_at_all_locations"),
        "created_at": category.get("created_at"),
        "updated_at": category.get("updated_at"),
        "category_name": category_data.get("name"),
        "is_top_level": category_data.get("is_top_level"),
        "online_visibility": category_data.get("online_visibility"),
        "category_type": category_data.get("category_type"),
        "parent_category_id": parent_category.get("id")
    })

print(f"\nTotal flattened rows: {len(flat_rows)}")


# ============================================================
# STEP 3 — BUILD THE SPARK DATAFRAME (with schema) AND PREVIEW
# ============================================================

from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

schema = StructType([
    StructField("square_category_id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("version", LongType(), True),
    StructField("is_deleted", BooleanType(), True),
    StructField("present_at_all_locations", BooleanType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("category_name", StringType(), True),
    StructField("is_top_level", BooleanType(), True),
    StructField("online_visibility", BooleanType(), True),
    StructField("category_type", StringType(), True),
    StructField("parent_category_id", StringType(), True)
])

if len(flat_rows) == 0:
    print("No categories found.")
else:
    df = spark.createDataFrame(flat_rows, schema=schema)
    display(df)

df.write.format("delta").mode("overwrite").saveAsTable("categories")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
from pyspark.sql.functions import col
ACCESS_TOKEN =  "EAAAlzu3caSyQQMdUZqrlOK4k0RvK9YOKUu3KJ36Hf2PHQVZD3c0P5D2Iy9HWppJ"

headers = {
    "Square-Version": "2026-07-15",
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}
response = requests.get(
    "https://connect.squareupsandbox.com/v2/catalog/list?types=ITEM",
    headers=headers
)

print(response.status_code)

data = response.json()

for obj in data.get("objects", []):
    print(
        obj["id"],
        "->",
        obj.get("item_data", {}).get("name")
    )

#display(obj)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

ACCESS_TOKEN = "EAAAlzu3caSyQQMdUZqrlOK4k0RvK9YOKUu3KJ36Hf2PHQVZD3c0P5D2Iy9HWppJ"
BASE_URL = "https://connect.squareupsandbox.com"

headers = {
    "Square-Version": "2026-05-20",
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

# Set this to True only when you're ready to actually delete
CONFIRM_DELETE = True


# ============================================================
# STEP 1 — GET ALL ITEM IDs
# ============================================================

def get_all_item_ids():

    item_ids = []
    cursor = None

    while True:

        params = {"types": "ITEM"}

        if cursor:
            params["cursor"] = cursor

        response = requests.get(
            f"{BASE_URL}/v2/catalog/list",
            headers=headers,
            params=params
        )

        response.raise_for_status()

        data = response.json()

        objects = data.get("objects", [])

        for obj in objects:
            if obj.get("type") == "ITEM":
                item_ids.append(obj["id"])

        cursor = data.get("cursor")

        if not cursor:
            break

    return item_ids


# ============================================================
# STEP 2 — DELETE ITEMS
# ============================================================

def delete_items(item_ids):

    if not item_ids:
        print("No ITEM objects found.")
        return

    print(f"\nFound {len(item_ids)} ITEM objects.")
    print("\nFirst few IDs:")

    for item_id in item_ids[:10]:
        print(f"  {item_id}")

    print("\n⚠️ WARNING:")
    print("This will delete ALL ITEM objects from the Square Sandbox.")
    print("Their ITEM_VARIATION children will also be deleted.")

    if not CONFIRM_DELETE:
        print("\nCONFIRM_DELETE is False — deletion cancelled.")
        print("Set CONFIRM_DELETE = True at the top and re-run to actually delete.")
        return

    # Square allows up to 200 object IDs per batch-delete request.
    for start in range(0, len(item_ids), 200):

        batch = item_ids[start:start + 200]

        print(f"\nDeleting {len(batch)} items...")

        response = requests.post(
            f"{BASE_URL}/v2/catalog/batch-delete",
            headers=headers,
            json={"object_ids": batch}
        )

        print(f"HTTP Status: {response.status_code}")

        response.raise_for_status()

        result = response.json()

        deleted = result.get("deleted_object_ids", [])

        print(f"Deleted objects returned by Square: {len(deleted)}")


# ============================================================
# MAIN
# ============================================================

print("\n========== SQUARE ITEM CLEANUP ==========")

item_ids = get_all_item_ids()

print(f"Total ITEMs found: {len(item_ids)}")

delete_items(item_ids)

print("\n==========================================")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
from pyspark.sql.types import StructType, StructField, StringType, LongType
    
#ACCESS_TOKEN = "EAAAlzu3caSyQQMdUZqrlOK4k0RvK9YOKUu3KJ36Hf2PHQVZD3c0P5D2Iy9HWppJ"
ACCESS_TOKEN = "EAAAl6mpp8GtAKOEO128xD-Ma5FRfLfGjrduVVpIfT7aG48DwKKQKCcW6Yqdg7Hm"

BASE_URL = "https://connect.squareupsandbox.com"

headers = {
    "Square-Version": "2026-07-15",
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

# ============================================================
# STEP 1 — FETCH ALL ITEMS (handles pagination properly)
# ============================================================

all_items = []
cursor = None

while True:
    params = {"types": "ITEM"}
    if cursor:
        params["cursor"] = cursor

    response = requests.get(
        f"{BASE_URL}/v2/catalog/list",
        headers=headers,
        params=params
    )

    data = response.json()

    if "errors" in data:
        print("Square returned an error:", data["errors"])
        break

    page_items = data.get("objects", [])
    all_items.extend(page_items)

    print(f"Fetched {len(page_items)} items (running total: {len(all_items)})")

    cursor = data.get("cursor")
    if not cursor:
        break

print(f"\nTotal items fetched: {len(all_items)}")


# ============================================================
# STEP 2 — FLATTEN INTO ONE ROW PER VARIATION
# ============================================================

flat_rows = []

for item in all_items:

    item_id = item.get("id")
    item_data = item.get("item_data", {})

    item_name = item_data.get("name")
    description = item_data.get("description")
    #category_id = item_data.get("category_id")
    categories = item_data.get("categories", [])

    category_id = ""

    if categories:
        category_id = categories[0].get("id", "")
    is_taxable = item_data.get("is_taxable")
    product_type = item_data.get("product_type")

    variations = item_data.get("variations", [])

    if not variations:
        flat_rows.append({
            "square_item_id": item_id,
            "item_name": item_name,
            "description": description,
            "category_id": category_id,
            "is_taxable": is_taxable,
            "product_type": product_type,
            "square_variation_id": None,
            "variation_name": None,
            "sku": None,
            "price_amount": None,
            "price_currency": None
        })
        continue

    for variation in variations:
        variation_data = variation.get("item_variation_data", {})
        price_money = variation_data.get("price_money", {})

        flat_rows.append({
            "square_item_id": item_id,
            "item_name": item_name,
            "description": description,
            "category_id": category_id,
            "is_taxable": is_taxable,
            "product_type": product_type,
            "square_variation_id": variation.get("id"),
            "variation_name": variation_data.get("name"),
            "sku": variation_data.get("sku"),
            "price_amount": price_money.get("amount"),
            "price_currency": price_money.get("currency")
        })

print(f"\nTotal flattened rows (item + variation combos): {len(flat_rows)}")


# ============================================================
# STEP 3 — BUILD THE SPARK DATAFRAME (with schema) AND PREVIEW
# ============================================================

schema = StructType([
    StructField("square_item_id", StringType(), True),
    StructField("item_name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("is_taxable", StringType(), True),
    StructField("product_type", StringType(), True),
    StructField("square_variation_id", StringType(), True),
    StructField("variation_name", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("price_amount", LongType(), True),
    StructField("price_currency", StringType(), True)
])

if len(flat_rows) == 0:
    print("No items found.")
else:
    df = spark.createDataFrame(flat_rows, schema=schema)
    display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, trim

# ============================================================
# STEP 4 — LOAD BUSINESS -> SQUARE MENU MAPPING
# ============================================================

MAPPING_FILE = "Files/menu_square_ids.csv"

mapping_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .option("encoding", "UTF-8")
    .csv(MAPPING_FILE)
)

print("Mapping columns:")
print(mapping_df.columns)

print(f"Mapping rows: {mapping_df.count()}")

display(mapping_df)
# ============================================================
# STEP 5 — PREPARE MAPPING
# ============================================================

mapping_df = mapping_df.select(
    trim(col("BusinessMenuID")).alias("business_menu_id"),
    trim(col("BusinessCategoryID")).alias("business_category_id"),
    trim(col("SquareItemID")).alias("mapping_square_item_id"),
    trim(col("SquareVariationID")).alias("mapping_square_variation_id")
)

print(f"Mapping rows: {mapping_df.count()}")

display(mapping_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# MENU

# CELL ********************

import requests

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)

#ACCESS_TOKEN = "EAAAlzu3caSyQQMdUZqrlOK4k0RvK9YOKUu3KJ36Hf2PHQVZD3c0P5D2Iy9HWppJ"
ACCESS_TOKEN = "EAAAl6mpp8GtAKOEO128xD-Ma5FRfLfGjrduVVpIfT7aG48DwKKQKCcW6Yqdg7Hm"


BASE_URL = "https://connect.squareupsandbox.com"

headers = {
    "Square-Version": "2026-07-15",
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}


# ============================================================
# STEP 1 — FETCH ALL SQUARE ITEMS
# ============================================================

all_items = []
cursor = None

while True:

    params = {
        "types": "ITEM"
    }

    if cursor:
        params["cursor"] = cursor

    response = requests.get(
        f"{BASE_URL}/v2/catalog/list",
        headers=headers,
        params=params
    )

    data = response.json()

    if "errors" in data:
        print("Square API error:")
        print(data["errors"])
        break

    page_items = data.get(
        "objects",
        []
    )

    all_items.extend(
        page_items
    )

    print(
        f"Fetched {len(page_items)} items "
        f"(running total: {len(all_items)})"
    )

    cursor = data.get("cursor")

    if not cursor:
        break


print(
    f"\nTotal items fetched: {len(all_items)}"
)


# ============================================================
# STEP 2 — FLATTEN ITEM + VARIATIONS
# ============================================================

flat_rows = []

for item in all_items:

    item_id = item.get("id")

    item_data = item.get(
        "item_data",
        {}
    )

    item_name = item_data.get(
        "name"
    )

    description = item_data.get(
        "description"
    )

    categories = item_data.get(
        "categories",
        []
    )

    category_id = ""

    if categories:

        category_id = categories[0].get(
            "id",
            ""
        )

    is_taxable = item_data.get(
        "is_taxable"
    )

    product_type = item_data.get(
        "product_type"
    )

    variations = item_data.get(
        "variations",
        []
    )

    for variation in variations:

        variation_data = variation.get(
            "item_variation_data",
            {}
        )

        price_money = variation_data.get(
            "price_money",
            {}
        )

        flat_rows.append({

            "square_item_id":
                item_id,

            "item_name":
                item_name,

            "description":
                description,

            "category_id":
                category_id,

            "is_taxable":
                str(is_taxable),

            "product_type":
                product_type,

            "square_variation_id":
                variation.get("id"),

            "variation_name":
                variation_data.get("name"),

            "sku":
                variation_data.get("sku"),

            "price_amount":
                price_money.get("amount"),

            "price_currency":
                price_money.get("currency")
        })


print(
    f"Total flattened rows: {len(flat_rows)}"
)


# ============================================================
# STEP 3 — CREATE SPARK DATAFRAME
# ============================================================

schema = StructType([

    StructField(
        "square_item_id",
        StringType(),
        True
    ),

    StructField(
        "item_name",
        StringType(),
        True
    ),

    StructField(
        "description",
        StringType(),
        True
    ),

    StructField(
        "category_id",
        StringType(),
        True
    ),

    StructField(
        "is_taxable",
        StringType(),
        True
    ),

    StructField(
        "product_type",
        StringType(),
        True
    ),

    StructField(
        "square_variation_id",
        StringType(),
        True
    ),

    StructField(
        "variation_name",
        StringType(),
        True
    ),

    StructField(
        "sku",
        StringType(),
        True
    ),

    StructField(
        "price_amount",
        LongType(),
        True
    ),

    StructField(
        "price_currency",
        StringType(),
        True
    )
])


df = spark.createDataFrame(
    flat_rows,
    schema=schema
)


print(
    "\nSquare API DataFrame rows:",
    df.count()
)

#display(df)
#df.write.format("delta").mode("overwrite").saveAsTable("MENU")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

joined_df = (
    df.alias("square")
    .join(
        mapping_df.alias("mapping"),

        (
            (col("square.square_item_id") ==
             col("mapping.mapping_square_item_id"))
            &
            (col("square.square_variation_id") ==
             col("mapping.mapping_square_variation_id"))
        ),

        "left"
    )
)

print(
    "Joined rows:",
    joined_df.count()
)

#display(joined_df)
joined_df.write.format("delta").mode("overwrite").saveAsTable("MENU")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

final_menu_df = joined_df.select(

    # Business IDs from mapping
    col("mapping.business_menu_id")
        .alias("business_menu_id"),

    col("mapping.business_category_id")
        .alias("business_category_id"),

    # Menu information from Square
    col("square.item_name")
        .alias("item_name"),

    col("square.description")
        .alias("description"),

    col("square.variation_name")
        .alias("variation_name"),

    col("square.sku")
        .alias("sku"),

    # Square category ID
    # IMPORTANT: this comes from Square API data
    col("square.category_id")
        .alias("square_category_id"),

    # Square IDs
    col("square.square_item_id")
        .alias("square_item_id"),

    col("square.square_variation_id")
        .alias("square_variation_id"),

    # Price
    col("square.price_amount")
        .alias("price_amount"),

    col("square.price_currency")
        .alias("price_currency"),

    # Product attributes
    col("square.is_taxable")
        .alias("is_taxable"),

    col("square.product_type")
        .alias("product_type")
)

print(
    "Final menu rows:",
    final_menu_df.count()
)

print(
    "Columns:",
    final_menu_df.columns
)

display(final_menu_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Total rows:",
      final_menu_df.count())

print(
    "Missing Business Menu ID:",
    final_menu_df.filter(
        col("business_menu_id").isNull()
    ).count()
)

print(
    "Missing Business Category ID:",
    final_menu_df.filter(
        col("business_category_id").isNull()
    ).count()
)

print(
    "Missing Square Category ID:",
    final_menu_df.filter(
        col("square_category_id").isNull()
    ).count()
)

print(
    "Missing Square Item ID:",
    final_menu_df.filter(
        col("square_item_id").isNull()
    ).count()
)

print(
    "Missing Square Variation ID:",
    final_menu_df.filter(
        col("square_variation_id").isNull()
    ).count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

duplicate_check = (
    final_menu_df
    .groupBy(
        "business_menu_id",
        "square_variation_id"
    )
    .count()
    .filter(
        col("count") > 1
    )
)

print(
    "Duplicate item + variation mappings:",
    duplicate_check.count()
)

display(duplicate_check)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

TABLE_NAME = "menu_master"

(
    final_menu_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TABLE_NAME)
)

print(
    f"Table '{TABLE_NAME}' created successfully."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

menu_master_df = spark.table("menu_master")

print(
    "Rows in menu_master:",
    menu_master_df.count()
)

display(menu_master_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json

ACCESS_TOKEN = "EAAAlzu3caSyQQMdUZqrlOK4k0RvK9YOKUu3KJ36Hf2PHQVZD3c0P5D2Iy9HWppJ"

BASE_URL = "https://connect.squareupsandbox.com"

headers = {
    "Square-Version": "2026-07-15",
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}


# ============================================================
# SEARCH SQUARE ORDERS
# ============================================================

payload = {
    "location_ids": [
        "YOUR_LOCATION_ID"
    ],
    "limit": 100,
    "return_entries": False
}


response = requests.post(
    f"{BASE_URL}/v2/orders",
    headers=headers,
    json=payload
)
# /searchhttps://connect.squareupsandbox.com/v2/orders \

print("HTTP Status:", response.status_code)

data = response.json()

print(
    json.dumps(
        data,
        indent=2
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

ACCESS_TOKEN = "EAAAlzu3caSyQQMdUZqrlOK4k0RvK9YOKUu3KJ36Hf2PHQVZD3c0P5D2Iy9HWppJ"

headers = {
    "Square-Version": "2026-07-15",
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

response = requests.get(
    "https://connect.squareupsandbox.com/v2/locations",
    headers=headers
)

print(response.status_code)
print(response.json())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json

ACCESS_TOKEN = "EAAAl6mpp8GtAKOEO128xD-Ma5FRfLfGjrduVVpIfT7aG48DwKKQKCcW6Yqdg7Hm"

LOCATION_ID = "L3E53W0VC81ZX"
BASE_URL = "https://connect.squareupsandbox.com"

headers = {
    "Square-Version": "2026-07-15",
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

# ============================================================
# STEP 1 — FETCH ALL ORDERS (handles pagination properly)
# ============================================================

all_orders = []
cursor = None

while True:
    payload = {
        "location_ids": [LOCATION_ID],
        "limit": 100,
        "return_entries": False
    }

    if cursor:
        payload["cursor"] = cursor

    response = requests.post(
        f"{BASE_URL}/v2/orders/search",
        headers=headers,
        json=payload
    )

    data = response.json()

    if "errors" in data:
        print("Square returned an error:", data["errors"])
        break

    page_orders = data.get("orders", [])
    all_orders.extend(page_orders)

    print(f"Fetched {len(page_orders)} orders (running total: {len(all_orders)})")

    cursor = data.get("cursor")
    if not cursor:
        break

print(f"\nTotal orders fetched: {len(all_orders)}")


# ============================================================
# STEP 2 — FLATTEN ORDER HEADER FIELDS IN PLAIN PYTHON
# (line items handled separately — see note below)
# ============================================================

flat_rows = []

for order in all_orders:

    total_money = order.get("total_money", {})
    total_tax = order.get("total_tax_money", {})
    total_discount = order.get("total_discount_money", {})

    flat_rows.append({
        "square_order_id": order.get("id"),
        "location_id": order.get("location_id"),
        "state": order.get("state"),
        "created_at": order.get("created_at"),
        "updated_at": order.get("updated_at"),
        "customer_id": order.get("customer_id"),
        "total_amount": total_money.get("amount"),
        "total_currency": total_money.get("currency"),
        "total_tax_amount": total_tax.get("amount"),
        "total_discount_amount": total_discount.get("amount")
    })

print(f"\nTotal flattened rows: {len(flat_rows)}")


# ============================================================
# STEP 3 — BUILD THE SPARK DATAFRAME (with schema) AND PREVIEW
# ============================================================

from pyspark.sql.types import StructType, StructField, StringType, LongType

schema = StructType([
    StructField("square_order_id", StringType(), True),
    StructField("location_id", StringType(), True),
    StructField("state", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("total_amount", LongType(), True),
    StructField("total_currency", StringType(), True),
    StructField("total_tax_amount", LongType(), True),
    StructField("total_discount_amount", LongType(), True)
])

if len(flat_rows) == 0:
    print("No orders found.")
else:
    df = spark.createDataFrame(flat_rows, schema=schema)
    display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

ACCESS_TOKEN = "EAAAl6mpp8GtAKOEO128xD-Ma5FRfLfGjrduVVpIfT7aG48DwKKQKCcW6Yqdg7Hm"

BASE_URL = "https://connect.squareupsandbox.com"

headers = {
    "Square-Version": "2026-07-15",
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

response = requests.get(
    f"{BASE_URL}/v2/locations",
    headers=headers
)

print("Status:", response.status_code)
print(response.json())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Orders

# CELL ********************

import requests

ACCESS_TOKEN = "EAAAl6mpp8GtAKOEO128xD-Ma5FRfLfGjrduVVpIfT7aG48DwKKQKCcW6Yqdg7Hm"
BASE_URL = "https://connect.squareupsandbox.com"
LOCATION_ID = "LMCQ87A8CY2GG"

headers = {
    "Square-Version": "2026-07-15",
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

# ============================================================
# STEP 1 — FETCH ALL ORDERS (handles pagination properly)
# ============================================================

all_orders = []
cursor = None

while True:
    payload = {
        "location_ids": [LOCATION_ID],
        "limit": 100,
        "return_entries": False
    }
    if cursor:
        payload["cursor"] = cursor

    response = requests.post(
        f"{BASE_URL}/v2/orders/search",
        headers=headers,
        json=payload
    )

    data = response.json()

    if "errors" in data:
        print("Square returned an error:", data["errors"])
        break

    page_orders = data.get("orders", [])
    all_orders.extend(page_orders)

    print(f"Fetched {len(page_orders)} orders (running total: {len(all_orders)})")

    cursor = data.get("cursor")
    if not cursor:
        break

print(f"\nTotal orders fetched: {len(all_orders)}")


# ============================================================
# STEP 2a — FLATTEN ORDER HEADERS (one row per order)
# ============================================================

order_header_rows = []

for order in all_orders:

    total_money = order.get("total_money", {})
    total_tax = order.get("total_tax_money", {})
    total_discount = order.get("total_discount_money", {})
    total_tip = order.get("total_tip_money", {})
    total_service_charge = order.get("total_service_charge_money", {})
    net_amounts = order.get("net_amounts", {})

    order_header_rows.append({
        "square_order_id": order.get("id"),
        "location_id": order.get("location_id"),
        "state": order.get("state"),
        "version": order.get("version"),
        "created_at": order.get("created_at"),
        "updated_at": order.get("updated_at"),
        "closed_at": order.get("closed_at"),
        "customer_id": order.get("customer_id"),
        "reference_id": order.get("reference_id"),
        "source_name": order.get("source", {}).get("name"),
        "total_amount": total_money.get("amount"),
        "total_currency": total_money.get("currency"),
        "total_tax_amount": total_tax.get("amount"),
        "total_discount_amount": total_discount.get("amount"),
        "total_tip_amount": total_tip.get("amount"),
        "total_service_charge_amount": total_service_charge.get("amount")
    })

print(f"Total order header rows: {len(order_header_rows)}")


# ============================================================
# STEP 2b — EXPLODE LINE ITEMS (one row per item per order)
# ============================================================

line_item_rows = []

for order in all_orders:

    order_id = order.get("id")
    line_items = order.get("line_items", [])

    for li in line_items:

        base_price = li.get("base_price_money", {})
        gross_sales = li.get("gross_sales_money", {})
        total_tax_li = li.get("total_tax_money", {})
        total_discount_li = li.get("total_discount_money", {})
        total_money_li = li.get("total_money", {})

        line_item_rows.append({
            "square_order_id": order_id,
            "line_item_uid": li.get("uid"),
            "catalog_object_id": li.get("catalog_object_id"),
            "item_name": li.get("name"),
            "variation_name": li.get("variation_name"),
            "quantity": li.get("quantity"),
            "base_price_amount": base_price.get("amount"),
            "gross_sales_amount": gross_sales.get("amount"),
            "total_tax_amount": total_tax_li.get("amount"),
            "total_discount_amount": total_discount_li.get("amount"),
            "line_total_amount": total_money_li.get("amount"),
            "currency": total_money_li.get("currency")
        })

print(f"Total line item rows: {len(line_item_rows)}")


# ============================================================
# STEP 3 — BUILD BOTH SPARK DATAFRAMES (with schema)
# ============================================================

from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

order_header_schema = StructType([
    StructField("square_order_id", StringType(), True),
    StructField("location_id", StringType(), True),
    StructField("state", StringType(), True),
    StructField("version", LongType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("closed_at", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("reference_id", StringType(), True),
    StructField("source_name", StringType(), True),
    StructField("total_amount", LongType(), True),
    StructField("total_currency", StringType(), True),
    StructField("total_tax_amount", LongType(), True),
    StructField("total_discount_amount", LongType(), True),
    StructField("total_tip_amount", LongType(), True),
    StructField("total_service_charge_amount", LongType(), True)
])

line_item_schema = StructType([
    StructField("square_order_id", StringType(), True),
    StructField("line_item_uid", StringType(), True),
    StructField("catalog_object_id", StringType(), True),
    StructField("item_name", StringType(), True),
    StructField("variation_name", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("base_price_amount", LongType(), True),
    StructField("gross_sales_amount", LongType(), True),
    StructField("total_tax_amount", LongType(), True),
    StructField("total_discount_amount", LongType(), True),
    StructField("line_total_amount", LongType(), True),
    StructField("currency", StringType(), True)
])

if len(order_header_rows) == 0:
    print("No orders found.")
else:
    orders_df = spark.createDataFrame(order_header_rows, schema=order_header_schema)
    print("\n===== ORDER HEADERS =====")
    display(orders_df)

if len(line_item_rows) == 0:
    print("No line items found.")
else:
    order_details_df = spark.createDataFrame(line_item_rows, schema=line_item_schema)
    print("\n===== ORDER LINE ITEMS =====")
    display(order_details_df)


# ============================================================
# STEP 4 — SAVE BOTH TABLES TO THE LAKEHOUSE
# ============================================================

if len(order_header_rows) > 0:
    orders_df.write.format("delta").mode("overwrite").saveAsTable("SquareOrdersHeader")
    print("Saved: SquareOrdersHeader")
else:
    print("Skipped saving SquareOrdersHeader — no orders found.")

if len(line_item_rows) > 0:
    order_details_df.write.format("delta").mode("overwrite").saveAsTable("SquareOrdersLine")
    print("Saved: SquareOrdersLine")
else:
    print("Skipped saving SquareOrdersLine — no line items found.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# payments 

# CELL ********************

import requests
import json

API_URL = "https://connect.squareupsandbox.com/v2/payments"
ACCESS_TOKEN = "EAAAl6mpp8GtAKOEO128xD-Ma5FRfLfGjrduVVpIfT7aG48DwKKQKCcW6Yqdg7Hm"

headers = {
    "Square-Version": "2026-07-15",
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

params = {
    "limit": 100,
    "sort_order": "DESC",
    "sort_field": "CREATED_AT"
}

# ============================================================
# STEP 1 — FETCH ALL PAYMENTS (pagination)
# ============================================================

all_payments = []

while True:
    response = requests.get(API_URL, headers=headers, params=params)
    print("Status Code:", response.status_code)

    if response.status_code != 200:
        print("API Error:", response.text)
        response.raise_for_status()

    response_json = response.json()
    payments = response_json.get("payments", [])
    all_payments.extend(payments)

    print("Records fetched in this page:", len(payments))
    print("Total records fetched:", len(all_payments))

    cursor = response_json.get("cursor")
    if not cursor:
        break
    params["cursor"] = cursor

print("===================================")
print("Total payments fetched:", len(all_payments))
print("===================================")


# ============================================================
# STEP 2a — FLATTEN PAYMENT HEADER (one row per payment)
# ============================================================

payment_rows = []

for p in all_payments:

    amount_money = p.get("amount_money", {})
    total_money = p.get("total_money", {})
    approved_money = p.get("approved_money", {})
    card_details = p.get("card_details", {})
    card = card_details.get("card", {})
    timeline = card_details.get("card_payment_timeline", {})
    app_details = p.get("application_details", {})
    cash_details = p.get("cash_details", {})
    cash_amount = cash_details.get("buyer_supplied_money", {})
    external_details = p.get("external_details", {})

    payment_rows.append({
        "payment_id": p.get("id"),
        "created_at": p.get("created_at"),
        "updated_at": p.get("updated_at"),
        "location_id": p.get("location_id"),
        "order_id": p.get("order_id"),
        "customer_id": p.get("customer_id"),
        "status": p.get("status"),
        "source_type": p.get("source_type"),
        "receipt_number": p.get("receipt_number"),
        "receipt_url": p.get("receipt_url"),
        "delay_action": p.get("delay_action"),
        "delay_duration": p.get("delay_duration"),
        "delayed_until": p.get("delayed_until"),
        "version_token": p.get("version_token"),

        "amount": amount_money.get("amount"),
        "currency": amount_money.get("currency"),

        "total_amount": total_money.get("amount"),
        "total_currency": total_money.get("currency"),

        "approved_amount": approved_money.get("amount"),
        "approved_currency": approved_money.get("currency"),

        "card_status": card_details.get("status"),
        "card_brand": card.get("card_brand"),
        "card_last_4": card.get("last_4"),
        "card_exp_month": card.get("exp_month"),
        "card_exp_year": card.get("exp_year"),
        "card_fingerprint": card.get("fingerprint"),
        "card_type": card.get("card_type"),
        "card_prepaid_type": card.get("prepaid_type"),
        "entry_method": card_details.get("entry_method"),
        "cvv_status": card_details.get("cvv_status"),
        "avs_status": card_details.get("avs_status"),
        "auth_result_code": card_details.get("auth_result_code"),
        "statement_description": card_details.get("statement_description"),
        "authorized_at": timeline.get("authorized_at"),
        "captured_at": timeline.get("captured_at"),

        "square_product": app_details.get("square_product"),
        "application_id": app_details.get("application_id"),

        "cash_buyer_supplied_amount": cash_amount.get("amount"),
        "cash_buyer_supplied_currency": cash_amount.get("currency"),

        "external_type": external_details.get("type"),
        "external_source": external_details.get("source")
    })

print(f"\nTotal flattened payment rows: {len(payment_rows)}")


# ============================================================
# STEP 2b — EXPLODE processing_fee (one row per fee per payment)
# ============================================================

fee_rows = []

for p in all_payments:

    payment_id = p.get("id")
    fees = p.get("processing_fee", [])

    for fee in fees:
        fee_amount_money = fee.get("amount_money", {})

        fee_rows.append({
            "payment_id": payment_id,
            "processing_fee_effective_at": fee.get("effective_at"),
            "processing_fee_type": fee.get("type"),
            "processing_fee_amount": fee_amount_money.get("amount"),
            "processing_fee_currency": fee_amount_money.get("currency")
        })

print(f"Total processing fee rows: {len(fee_rows)}")


# ============================================================
# STEP 3 — BUILD SPARK DATAFRAMES (with schema) AND SAVE
# ============================================================

from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from pyspark.sql import functions as F

payment_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("location_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("source_type", StringType(), True),
    StructField("receipt_number", StringType(), True),
    StructField("receipt_url", StringType(), True),
    StructField("delay_action", StringType(), True),
    StructField("delay_duration", StringType(), True),
    StructField("delayed_until", StringType(), True),
    StructField("version_token", StringType(), True),
    StructField("amount", LongType(), True),
    StructField("currency", StringType(), True),
    StructField("total_amount", LongType(), True),
    StructField("total_currency", StringType(), True),
    StructField("approved_amount", LongType(), True),
    StructField("approved_currency", StringType(), True),
    StructField("card_status", StringType(), True),
    StructField("card_brand", StringType(), True),
    StructField("card_last_4", StringType(), True),
    StructField("card_exp_month", IntegerType(), True),
    StructField("card_exp_year", IntegerType(), True),
    StructField("card_fingerprint", StringType(), True),
    StructField("card_type", StringType(), True),
    StructField("card_prepaid_type", StringType(), True),
    StructField("entry_method", StringType(), True),
    StructField("cvv_status", StringType(), True),
    StructField("avs_status", StringType(), True),
    StructField("auth_result_code", StringType(), True),
    StructField("statement_description", StringType(), True),
    StructField("authorized_at", StringType(), True),
    StructField("captured_at", StringType(), True),
    StructField("square_product", StringType(), True),
    StructField("application_id", StringType(), True),
    StructField("cash_buyer_supplied_amount", LongType(), True),
    StructField("cash_buyer_supplied_currency", StringType(), True),
    StructField("external_type", StringType(), True),
    StructField("external_source", StringType(), True)
])

fee_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("processing_fee_effective_at", StringType(), True),
    StructField("processing_fee_type", StringType(), True),
    StructField("processing_fee_amount", LongType(), True),
    StructField("processing_fee_currency", StringType(), True)
])

df_payments = spark.createDataFrame(payment_rows, schema=payment_schema)

# convert timestamp-looking text columns into real timestamps
for column in ["created_at", "updated_at", "authorized_at", "captured_at", "delayed_until"]:
    df_payments = df_payments.withColumn(column, F.to_timestamp(column))

#df_payments.printSchema()
#df_payments.show(truncate=False)

if len(fee_rows) == 0:
    print("No processing fees found.")
    df_fees = spark.createDataFrame([], schema=fee_schema)
else:
    df_fees = spark.createDataFrame(fee_rows, schema=fee_schema)
    df_fees = df_fees.withColumn(
        "processing_fee_effective_at",
        F.to_timestamp("processing_fee_effective_at")
    )
    df_fees.printSchema()
    df_fees.show(truncate=False)

# ============================================================
# STEP 4 — SAVE TO LAKEHOUSE
# ============================================================

df_payments.write.format("delta").mode("overwrite").saveAsTable("SquarePayments")
df_fees.write.format("delta").mode("overwrite").saveAsTable("SquarePaymentProcessingFees")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ***Locations***

# CELL ********************

import requests

ACCESS_TOKEN = "EAAAl6mpp8GtAKOEO128xD-Ma5FRfLfGjrduVVpIfT7aG48DwKKQKCcW6Yqdg7Hm"
BASE_URL = "https://connect.squareupsandbox.com"

headers = {
    "Square-Version": "2026-07-15",
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

# ============================================================
# STEP 1 — FETCH ALL LOCATIONS (no pagination needed)
# ============================================================

response = requests.get(f"{BASE_URL}/v2/locations", headers=headers)
data = response.json()

print("Status Code:", response.status_code)

if "errors" in data:
    print("Square returned an error:", data["errors"])

all_locations = data.get("locations", [])

print(f"\nTotal locations fetched: {len(all_locations)}")


# ============================================================
# STEP 2 — FLATTEN LOCATIONS IN PLAIN PYTHON
# ============================================================

location_rows = []

for loc in all_locations:

    address = loc.get("address", {})
    coordinates = loc.get("coordinates", {})
    business_hours = loc.get("business_hours", {})
    tax_ids = loc.get("tax_ids", {})

    location_rows.append({
        "square_location_id": loc.get("id"),
        "name": loc.get("name"),
        "business_name": loc.get("business_name"),
        "type": loc.get("type"),
        "status": loc.get("status"),
        "created_at": loc.get("created_at"),
        "merchant_id": loc.get("merchant_id"),
        "country": loc.get("country"),
        "language_code": loc.get("language_code"),
        "currency": loc.get("currency"),
        "phone_number": loc.get("phone_number"),
        "website_url": loc.get("website_url"),
        "description": loc.get("description"),
        "timezone": loc.get("timezone"),

        "address_line_1": address.get("address_line_1"),
        "address_line_2": address.get("address_line_2"),
        "locality": address.get("locality"),
        "administrative_district_level_1": address.get("administrative_district_level_1"),
        "postal_code": address.get("postal_code"),
        "address_country": address.get("country"),

        "latitude": coordinates.get("latitude"),
        "longitude": coordinates.get("longitude"),

        "ein_tax_id": tax_ids.get("eu_vat") or tax_ids.get("us_ein"),

        "mcc": loc.get("mcc")
    })

print(f"\nTotal flattened location rows: {len(location_rows)}")


# ============================================================
# STEP 3 — BUILD THE SPARK DATAFRAME (with schema)
# ============================================================

from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import functions as F

location_schema = StructType([
    StructField("square_location_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("business_name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("country", StringType(), True),
    StructField("language_code", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("website_url", StringType(), True),
    StructField("description", StringType(), True),
    StructField("timezone", StringType(), True),
    StructField("address_line_1", StringType(), True),
    StructField("address_line_2", StringType(), True),
    StructField("locality", StringType(), True),
    StructField("administrative_district_level_1", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("address_country", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("ein_tax_id", StringType(), True),
    StructField("mcc", StringType(), True)
])

if len(location_rows) == 0:
    print("No locations found.")
else:
    df_locations = spark.createDataFrame(location_rows, schema=location_schema)
    df_locations = df_locations.withColumn("created_at", F.to_timestamp("created_at"))

    df_locations.printSchema()
    display(df_locations)


# ============================================================
# STEP 4 — SAVE TO THE LAKEHOUSE
# ============================================================

if len(location_rows) > 0:
    df_locations.write.format("delta").mode("overwrite").saveAsTable("SquareLocations")
    print("Saved: SquareLocations")
else:
    print("Skipped saving SquareLocations — no locations found.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ****Inventory****

# CELL ********************

import requests
from pyspark.sql import functions as F


# =========================================================
# CONFIGURATION
# =========================================================

ACCESS_TOKEN = "EAAAl6mpp8GtAKOEO128xD-Ma5FRfLfGjrduVVpIfT7aG48DwKKQKCcW6Yqdg7Hm"

API_URL = (
    "https://connect.squareupsandbox.com/"
    "v2/inventory/counts/batch-retrieve"
)

headers = {
    "Square-Version": "2026-07-15",
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}


# =========================================================
# REQUEST BODY
# =========================================================

payload = {
    "limit": 1000,

    # Uncomment if you only want IN_STOCK
    # "states": ["IN_STOCK"],

    # Uncomment for a specific location
    # "location_ids": ["YOUR_LOCATION_ID"],

    # Incremental load
    # "updated_after": "2026-08-19T00:00:00Z"
}


# =========================================================
# API EXTRACTION
# =========================================================

all_inventory = []

cursor = None


while True:

    # -----------------------------------------------------
    # Add pagination cursor
    # -----------------------------------------------------

    if cursor:
        payload["cursor"] = cursor
    else:
        payload.pop("cursor", None)


    # -----------------------------------------------------
    # API request
    # -----------------------------------------------------

    response = requests.post(
        API_URL,
        headers=headers,
        json=payload,
        timeout=60
    )


    print("Status Code:", response.status_code)


    # -----------------------------------------------------
    # Error handling
    # -----------------------------------------------------

    if response.status_code != 200:

        print("Square API Error:")
        print(response.text)

        response.raise_for_status()


    # -----------------------------------------------------
    # Parse JSON
    # -----------------------------------------------------

    response_json = response.json()


    # -----------------------------------------------------
    # Extract inventory counts
    # -----------------------------------------------------

    inventory_counts = response_json.get(
        "counts",
        []
    )


    all_inventory.extend(
        inventory_counts
    )


    print(
        "Current page:",
        len(inventory_counts)
    )

    print(
        "Total records:",
        len(all_inventory)
    )


    # -----------------------------------------------------
    # Pagination
    # -----------------------------------------------------

    cursor = response_json.get(
        "cursor"
    )


    if not cursor:
        break


# =========================================================
# CREATE SPARK DATAFRAME
# =========================================================

if all_inventory:

    df_inventory = spark.createDataFrame(
        all_inventory
    )

else:

    print("No inventory records returned.")

    df_inventory = None


# =========================================================
# TRANSFORMATION
# =========================================================

if df_inventory is not None:

    df_inventory = df_inventory.select(
        F.col("catalog_object_id"),
        F.col("catalog_object_type"),
        F.col("state"),
        F.col("location_id"),
        F.col("quantity"),
        F.col("calculated_at")
    )


    # -----------------------------------------------------
    # Data types
    # -----------------------------------------------------

    df_inventory = df_inventory.withColumn(
        "quantity",
        F.col("quantity").cast(
            "decimal(18,3)"
        )
    )


    df_inventory = df_inventory.withColumn(
        "calculated_at",
        F.to_timestamp(
            "calculated_at"
        )
    )


    # -----------------------------------------------------
    # Display
    # -----------------------------------------------------

    df_inventory.printSchema()

    df_inventory.show(
        truncate=False
    )


df_inventory.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("SquareInventory")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# SQUARE INVENTORY API
# MICROSOFT FABRIC - PYSPARK NOTEBOOK
# ============================================================

import requests

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType


# ============================================================
# 1. CONFIGURATION
# ============================================================

ACCESS_TOKEN = "EAAAl6mpp8GtAKOEO128xD-Ma5FRfLfGjrduVVpIfT7aG48DwKKQKCcW6Yqdg7Hm"

API_URL = (
    "https://connect.squareupsandbox.com/"
    "v2/inventory/counts/batch-retrieve"
)

headers = {
    "Square-Version": "2026-07-15",
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}


# ============================================================
# 2. API REQUEST BODY
# ============================================================
# Keep this empty except for limit if you want all available
# inventory counts.
#
# You can later add:
#
# "location_ids": ["LOCATION_ID"]
# "states": ["IN_STOCK"]
# "updated_after": "2026-08-01T00:00:00Z"
#
# ============================================================

payload = {
    "limit": 1000
}


# ============================================================
# 3. FETCH ALL INVENTORY RECORDS
# ============================================================

all_inventory = []

cursor = None

page_number = 1


while True:

    print("--------------------------------------------")
    print(f"Fetching page: {page_number}")
    print("--------------------------------------------")


    # --------------------------------------------------------
    # Add pagination cursor
    # --------------------------------------------------------

    if cursor:

        payload["cursor"] = cursor

    else:

        payload.pop(
            "cursor",
            None
        )


    # --------------------------------------------------------
    # Call Square API
    # --------------------------------------------------------

    response = requests.post(
        API_URL,
        headers=headers,
        json=payload,
        timeout=60
    )


    print(
        "HTTP Status:",
        response.status_code
    )


    # --------------------------------------------------------
    # Error handling
    # --------------------------------------------------------

    if response.status_code != 200:

        print("Square API Error:")
        print(response.text)

        response.raise_for_status()


    # --------------------------------------------------------
    # Parse JSON response
    # --------------------------------------------------------

    response_json = response.json()


    # --------------------------------------------------------
    # Get inventory counts
    # --------------------------------------------------------

    counts = response_json.get(
        "counts",
        []
    )


    print(
        "Records in current page:",
        len(counts)
    )


    # Add current page to complete list

    all_inventory.extend(
        counts
    )


    print(
        "Total records fetched:",
        len(all_inventory)
    )


    # --------------------------------------------------------
    # Get next cursor
    # --------------------------------------------------------

    cursor = response_json.get(
        "cursor"
    )


    # --------------------------------------------------------
    # Stop if no next page
    # --------------------------------------------------------

    if not cursor:

        print("No more pages.")

        break


    page_number += 1


print()
print("============================================")
print("API EXTRACTION COMPLETED")
print("Total inventory records:", len(all_inventory))
print("============================================")


# ============================================================
# 4. CREATE SPARK DATAFRAME
# ============================================================

if len(all_inventory) == 0:

    print("No inventory records were returned.")

else:

    df_inventory = spark.createDataFrame(
        all_inventory
    )

    print()
    print("============================================")
    print("ORIGINAL API SCHEMA")
    print("============================================")

    df_inventory.printSchema()


# ============================================================
# 5. FUNCTION TO FLATTEN STRUCT COLUMNS
# ============================================================

def flatten_structs(df):

    while True:

        struct_columns = []

        # Find all STRUCT columns

        for field in df.schema.fields:

            if isinstance(
                field.dataType,
                StructType
            ):

                struct_columns.append(
                    field.name
                )


        # Stop when no STRUCT columns remain

        if not struct_columns:

            break


        # Take first STRUCT column

        struct_column = struct_columns[0]


        # Get nested fields

        struct_field = df.schema[
            struct_column
        ]

        nested_fields = (
            struct_field.dataType.fields
        )


        new_columns = []


        # ----------------------------------------------------
        # Keep existing columns except STRUCT
        # ----------------------------------------------------

        for column in df.columns:

            if column != struct_column:

                new_columns.append(
                    F.col(
                        f"`{column}`"
                    )
                )


        # ----------------------------------------------------
        # Expand STRUCT fields
        # ----------------------------------------------------

        for nested_field in nested_fields:

            nested_name = nested_field.name

            new_column_name = (
                f"{struct_column}_{nested_name}"
            )


            new_columns.append(
                F.col(
                    f"`{struct_column}`."
                    f"`{nested_name}`"
                ).alias(
                    new_column_name
                )
            )


        # ----------------------------------------------------
        # Create new DataFrame
        # ----------------------------------------------------

        df = df.select(
            *new_columns
        )


    return df


# ============================================================
# 6. FLATTEN STRUCT COLUMNS
# ============================================================

if df_inventory is not None:

    print()
    print("============================================")
    print("FLATTENING STRUCT COLUMNS")
    print("============================================")


    df_inventory = flatten_structs(
        df_inventory
    )


    print("STRUCT flattening completed.")


# ============================================================
# 7. FIND ARRAY COLUMNS
# ============================================================

if df_inventory is not None:

    print()
    print("============================================")
    print("ARRAY COLUMNS")
    print("============================================")


    array_columns = []


    for field in df_inventory.schema.fields:

        if isinstance(
            field.dataType,
            ArrayType
        ):

            array_columns.append(
                field.name
            )

            print(
                field.name,
                "->",
                field.dataType
            )


    if len(array_columns) == 0:

        print("No ARRAY columns found.")

    else:

        print(
            "Total ARRAY columns:",
            len(array_columns)
        )


# ============================================================
# 8. EXPLODE ARRAY COLUMNS
# ============================================================
#
# IMPORTANT:
#
# Exploding an array can create multiple rows for one
# inventory record.
#
# Example:
#
# inventory_id = 123
# array = [A, B]
#
# becomes:
#
# 123 | A
# 123 | B
#
# Therefore this is enabled because you specifically asked
# to explode the columns.
#
# ============================================================

def explode_arrays(df):

    while True:

        array_columns = []


        # Find ARRAY columns

        for field in df.schema.fields:

            if isinstance(
                field.dataType,
                ArrayType
            ):

                array_columns.append(
                    field.name
                )


        # Stop if no ARRAY columns remain

        if not array_columns:

            break


        # Take first ARRAY column

        array_column = array_columns[0]


        print(
            "Exploding ARRAY column:",
            array_column
        )


        # Explode array

        df = df.withColumn(
            array_column,
            F.explode_outer(
                F.col(array_column)
            )
        )


        # The exploded value may now be a STRUCT.
        # Flatten it.

        df = flatten_structs(
            df
        )


    return df


# ============================================================
# 9. EXPLODE ALL ARRAY COLUMNS
# ============================================================

if df_inventory is not None:

    df_inventory = explode_arrays(
        df_inventory
    )


    print()
    print("============================================")
    print("ARRAY EXPLOSION COMPLETED")
    print("============================================")


# ============================================================
# 10. CONVERT TIMESTAMP COLUMNS
# ============================================================

if df_inventory is not None:

    print()
    print("============================================")
    print("CONVERTING TIMESTAMP COLUMNS")
    print("============================================")


    timestamp_keywords = [
        "created_at",
        "updated_at",
        "calculated_at",
        "occurred_at",
        "sold_at",
        "received_at",
        "updated_at"
    ]


    for column in df_inventory.columns:

        column_lower = column.lower()


        for keyword in timestamp_keywords:

            if keyword in column_lower:

                df_inventory = df_inventory.withColumn(
                    column,
                    F.to_timestamp(
                        F.col(column)
                    )
                )

                break


# ============================================================
# 11. SHOW FINAL SCHEMA
# ============================================================

if df_inventory is not None:

    print()
    print("============================================")
    print("FINAL FLATTENED SCHEMA")
    print("============================================")

    df_inventory.printSchema()


# ============================================================
# 12. SHOW FINAL DATA
# ============================================================

if df_inventory is not None:

    print()
    print("============================================")
    print("FINAL DATA")
    print("============================================")

    df_inventory.show(
        20,
        truncate=False
    )


# ============================================================
# 13. RECORD COUNT
# ============================================================

if df_inventory is not None:

    final_count = df_inventory.count()

    print()
    print("============================================")
    print("FINAL RECORD COUNT:", final_count)
    print("============================================")


# ============================================================
# 14. SAVE TO FABRIC LAKEHOUSE
# ============================================================

if df_inventory is not None:

    table_name = "SquareInventory"


    df_inventory.write \
        .format("delta") \
        .mode("overwrite") \
        .option(
            "overwriteSchema",
            "true"
        ) \
        .saveAsTable(
            table_name
        )


    print()
    print("============================================")
    print("TABLE SAVED SUCCESSFULLY")
    print("Table:", table_name)
    print("============================================")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
