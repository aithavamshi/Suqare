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

# ****gold_order_date_mapping****

# CELL ********************

from pyspark.sql import functions as F

print("=" * 100)
print("GOLD ORDER DATE MAPPING")
print("=" * 100)

# ============================================================
# READ SILVER
# ============================================================

order_date_mapping = spark.table("silver.order_date_mapping")

print("Source rows:", order_date_mapping.count())

order_date_mapping.printSchema()

# ============================================================
# SELECT GOLD COLUMNS
# ============================================================

gold_order_date_mapping = (
    order_date_mapping
    .select(
        F.col("BusinessOrderID"),
        F.col("SquareOrderID"),
        F.col("ReportingOrderDate").cast("date")
            .alias("ReportingOrderDate")
    )
)

# ============================================================
# VALIDATION
# ============================================================

print()
print("=" * 100)
print("GOLD ORDER DATE MAPPING VALIDATION")
print("=" * 100)

total_rows = gold_order_date_mapping.count()

duplicate_business_ids = (
    gold_order_date_mapping
    .groupBy("BusinessOrderID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_business_ids = (
    gold_order_date_mapping
    .filter(F.col("BusinessOrderID").isNull())
    .count()
)

null_square_ids = (
    gold_order_date_mapping
    .filter(F.col("SquareOrderID").isNull())
    .count()
)

null_dates = (
    gold_order_date_mapping
    .filter(F.col("ReportingOrderDate").isNull())
    .count()
)

print("Total mappings         :", total_rows)
print("Duplicate Business IDs :", duplicate_business_ids)
print("NULL Business IDs      :", null_business_ids)
print("NULL Square IDs        :", null_square_ids)
print("NULL Reporting Dates   :", null_dates)

# ============================================================
# SAVE GOLD
# ============================================================

(
    gold_order_date_mapping
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.gold_order_date_mapping")
)

print()
print("=" * 100)
print("✅ GOLD ORDER DATE MAPPING SAVED")
print("=" * 100)

print("Table: gold.gold_order_date_mapping")
print("Rows :", gold_order_date_mapping.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ****dim_date****

# CELL ********************

from pyspark.sql import functions as F

print("=" * 100)
print("GOLD DIM DATE")
print("=" * 100)

# ============================================================
# READ ORDER REPORTING DATES
# ============================================================

order_dates = (
    spark.table("gold.gold_order_date_mapping")
    .select("ReportingOrderDate")
    .filter(F.col("ReportingOrderDate").isNotNull())
)

# ============================================================
# READ INVENTORY SNAPSHOT DATES
# ============================================================

inventory_dates = (
    spark.table("silver.inventory")
    .select(
        F.to_date("CalculatedAt").alias("InventoryDate")
    )
    .filter(F.col("InventoryDate").isNotNull())
)

# ============================================================
# FIND COMPLETE DATE RANGE
# ============================================================

order_min = order_dates.agg(
    F.min("ReportingOrderDate")
).first()[0]

order_max = order_dates.agg(
    F.max("ReportingOrderDate")
).first()[0]

inventory_min = inventory_dates.agg(
    F.min("InventoryDate")
).first()[0]

inventory_max = inventory_dates.agg(
    F.max("InventoryDate")
).first()[0]

print("Order minimum date     :", order_min)
print("Order maximum date     :", order_max)
print("Inventory minimum date :", inventory_min)
print("Inventory maximum date :", inventory_max)

# ============================================================
# DETERMINE MASTER DATE RANGE
# ============================================================

dates = [
    d for d in [
        order_min,
        order_max,
        inventory_min,
        inventory_max
    ]
    if d is not None
]

min_date = min(dates)
max_date = max(dates)

print("Dim Date minimum       :", min_date)
print("Dim Date maximum       :", max_date)

# ============================================================
# CREATE DATE RANGE
# ============================================================

dim_date = (
    spark.sql(
        f"""
        SELECT explode(
            sequence(
                to_date('{min_date}'),
                to_date('{max_date}'),
                interval 1 day
            )
        ) AS Date
        """
    )
)

# ============================================================
# CREATE DATE ATTRIBUTES
# ============================================================

dim_date = (
    dim_date
    .withColumn(
        "DateKey",
        F.date_format("Date", "yyyyMMdd").cast("int")
    )

    .withColumn(
        "Year",
        F.year("Date")
    )

    .withColumn(
        "QuarterNumber",
        F.quarter("Date")
    )

    .withColumn(
        "Quarter",
        F.concat(
            F.lit("Q"),
            F.quarter("Date")
        )
    )

    .withColumn(
        "MonthNumber",
        F.month("Date")
    )

    .withColumn(
        "MonthName",
        F.date_format("Date", "MMMM")
    )

    .withColumn(
        "MonthShortName",
        F.date_format("Date", "MMM")
    )

    .withColumn(
        "WeekOfYear",
        F.weekofyear("Date")
    )

    .withColumn(
        "DayOfMonth",
        F.dayofmonth("Date")
    )

    .withColumn(
        "DayName",
        F.date_format("Date", "EEEE")
    )

    .withColumn(
        "DayShortName",
        F.date_format("Date", "EEE")
    )

    .withColumn(
        "DayOfWeekNumber",
        F.dayofweek("Date")
    )

    .withColumn(
        "IsWeekend",
        F.dayofweek("Date").isin([1, 7])
    )

    .withColumn(
        "YearMonth",
        F.date_format("Date", "yyyy-MM")
    )

    .withColumn(
        "YearMonthNumber",
        F.year("Date") * 100 + F.month("Date")
    )
)

# ============================================================
# ORDER COLUMNS
# ============================================================

dim_date = dim_date.select(
    "DateKey",
    "Date",
    "Year",
    "QuarterNumber",
    "Quarter",
    "MonthNumber",
    "MonthName",
    "MonthShortName",
    "WeekOfYear",
    "DayOfMonth",
    "DayName",
    "DayShortName",
    "DayOfWeekNumber",
    "IsWeekend",
    "YearMonth",
    "YearMonthNumber"
)

# ============================================================
# VALIDATION
# ============================================================

print()
print("=" * 100)
print("DIM DATE VALIDATION")
print("=" * 100)

print("Total dates:", dim_date.count())

print(
    "Duplicate DateKeys:",
    dim_date
    .groupBy("DateKey")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    "NULL DateKeys:",
    dim_date
    .filter(F.col("DateKey").isNull())
    .count()
)

print(
    "NULL Dates:",
    dim_date
    .filter(F.col("Date").isNull())
    .count()
)

dim_date.orderBy("Date").show(10, truncate=False)

# ============================================================
# SAVE GOLD DIM DATE
# ============================================================

(
    dim_date
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.dim_date")
)

print()
print("=" * 100)
print("✅ GOLD DIM DATE SAVED")
print("=" * 100)

print("Table: gold.dim_date")
print("Rows :", dim_date.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ****dim_customer****

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("=" * 100)
print("GOLD DIM CUSTOMER")
print("=" * 100)

# ============================================================
# READ SILVER CUSTOMER
# ============================================================

silver_customer = spark.table("silver.customer")

print("Silver customer rows:", silver_customer.count())

# ============================================================
# REMOVE 100% NULL COLUMNS
# ============================================================

columns_to_keep = []

for column_name in silver_customer.columns:

    non_null_count = (
        silver_customer
        .filter(F.col(column_name).isNotNull())
        .limit(1)
        .count()
    )

    if non_null_count > 0:
        columns_to_keep.append(column_name)

silver_customer = silver_customer.select(*columns_to_keep)

print()
print("Columns retained:")
print(columns_to_keep)

# ============================================================
# CREATE CUSTOMER NAME
# ============================================================

dim_customer = (
    silver_customer
    .select(
        "SquareCustomerID",
        "ReferenceID",
        "GivenName",
        "FamilyName",
        "EmailAddress",
        "PhoneNumber",
        "CreatedAt",
        "UpdatedAt",
        "AddressDistrict",
        "AddressLocality",
        "AddressPostalCode",
        "AddressCountry"
    )
    .withColumn(
        "CustomerName",
        F.trim(
            F.concat_ws(
                " ",
                F.col("GivenName"),
                F.col("FamilyName")
            )
        )
    )
)

# ============================================================
# CREATE SURROGATE KEY
# ============================================================

window_spec = (
    Window
    .orderBy("SquareCustomerID")
)

dim_customer = (
    dim_customer
    .withColumn(
        "CustomerKey",
        F.row_number().over(window_spec)
    )
)

# ============================================================
# REORDER COLUMNS
# ============================================================

dim_customer = dim_customer.select(
    "CustomerKey",
    "SquareCustomerID",
    "ReferenceID",
    "CustomerName",
    "GivenName",
    "FamilyName",
    "EmailAddress",
    "PhoneNumber",
    "CreatedAt",
    "UpdatedAt",
    "AddressDistrict",
    "AddressLocality",
    "AddressPostalCode",
    "AddressCountry"
)

# ============================================================
# VALIDATION
# ============================================================

print()
print("=" * 100)
print("DIM CUSTOMER VALIDATION")
print("=" * 100)

total_customers = dim_customer.count()

duplicate_square_ids = (
    dim_customer
    .groupBy("SquareCustomerID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_square_ids = (
    dim_customer
    .filter(F.col("SquareCustomerID").isNull())
    .count()
)

null_customer_keys = (
    dim_customer
    .filter(F.col("CustomerKey").isNull())
    .count()
)

duplicate_customer_keys = (
    dim_customer
    .groupBy("CustomerKey")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("Total customers        :", total_customers)
print("Duplicate Square IDs   :", duplicate_square_ids)
print("NULL Square IDs        :", null_square_ids)
print("Duplicate CustomerKeys :", duplicate_customer_keys)
print("NULL CustomerKeys      :", null_customer_keys)

print()
print("Sample:")
dim_customer.show(10, truncate=False)

# ============================================================
# SAVE GOLD
# ============================================================

(
    dim_customer
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.dim_customer")
)

print()
print("=" * 100)
print("✅ GOLD DIM CUSTOMER SAVED")
print("=" * 100)

print("Table: gold.dim_customer")
print("Rows :", dim_customer.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ****dim_product****

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("=" * 100)
print("GOLD DIM PRODUCT")
print("=" * 100)

# ============================================================
# READ SILVER TABLES
# ============================================================

silver_item = spark.table("silver.catalog_item")
silver_variation = spark.table("silver.catalog_variation")

print("Silver catalog items      :", silver_item.count())
print("Silver catalog variations :", silver_variation.count())

# ============================================================
# JOIN ITEM + VARIATION
#
# One row = one catalog variation
# ============================================================

product_df = (
    silver_variation.alias("v")
    .join(
        silver_item.alias("i"),
        F.col("v.SquareItemID") == F.col("i.SquareItemID"),
        "left"
    )
)

print()
print("=" * 100)
print("CATALOG ITEM → VARIATION JOIN")
print("=" * 100)

print("Joined rows:", product_df.count())

# ============================================================
# SELECT GOLD COLUMNS
# ============================================================

dim_product = (
    product_df
    .select(
        F.col("v.SquareVariationID").alias("SquareVariationID"),
        F.col("v.SquareItemID").alias("SquareItemID"),

        F.col("i.ItemName").alias("ItemName"),
        F.col("i.Description").alias("Description"),
        F.col("i.ProductType").alias("ProductType"),

        F.col("v.VariationName").alias("VariationName"),
        F.col("v.SKU").alias("SKU"),

        F.col("v.Ordinal").alias("Ordinal"),

        F.col("v.PriceAmount").alias("PriceAmount"),
        F.col("v.Currency").alias("Currency"),
        F.col("v.PricingType").alias("PricingType"),

        F.col("v.Sellable").alias("Sellable"),
        F.col("v.Stockable").alias("Stockable"),

        F.col("i.IsArchived").alias("IsArchived"),
        F.col("i.IsTaxable").alias("IsTaxable"),

        F.col("v.IsDeleted").alias("IsVariationDeleted"),
        F.col("i.IsDeleted").alias("IsItemDeleted"),

        F.col("v.PresentAtAllLocations").alias(
            "PresentAtAllLocations"
        ),

        F.col("v.CreatedAt").alias("CreatedAt"),
        F.col("v.UpdatedAt").alias("UpdatedAt"),
        F.col("v.Version").alias("Version")
    )
)

# ============================================================
# REMOVE 100% NULL COLUMNS
# ============================================================

columns_to_keep = []

for column_name in dim_product.columns:

    non_null_count = (
        dim_product
        .filter(F.col(column_name).isNotNull())
        .limit(1)
        .count()
    )

    if non_null_count > 0:
        columns_to_keep.append(column_name)

dim_product = dim_product.select(*columns_to_keep)

print()
print("=" * 100)
print("COLUMNS RETAINED")
print("=" * 100)

print(columns_to_keep)

# ============================================================
# CREATE PRODUCT SURROGATE KEY
# ============================================================

window_spec = (
    Window
    .orderBy("SquareVariationID")
)

dim_product = (
    dim_product
    .withColumn(
        "ProductKey",
        F.row_number().over(window_spec)
    )
)

# ============================================================
# REORDER COLUMNS
# ============================================================

dim_product = dim_product.select(
    "ProductKey",
    "SquareVariationID",
    "SquareItemID",
    "ItemName",
    "Description",
    "ProductType",
    "VariationName",
    "SKU",
    "Ordinal",
    "PriceAmount",
    "Currency",
    "PricingType",
    "Sellable",
    "Stockable",
    "IsArchived",
    "IsTaxable",
    "IsVariationDeleted",
    "IsItemDeleted",
    "PresentAtAllLocations",
    "CreatedAt",
    "UpdatedAt",
    "Version"
)

# ============================================================
# VALIDATION
# ============================================================

print()
print("=" * 100)
print("GOLD DIM PRODUCT VALIDATION")
print("=" * 100)

total_products = dim_product.count()

duplicate_product_keys = (
    dim_product
    .groupBy("ProductKey")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_product_keys = (
    dim_product
    .filter(F.col("ProductKey").isNull())
    .count()
)

duplicate_variation_ids = (
    dim_product
    .groupBy("SquareVariationID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_variation_ids = (
    dim_product
    .filter(F.col("SquareVariationID").isNull())
    .count()
)

null_item_ids = (
    dim_product
    .filter(F.col("SquareItemID").isNull())
    .count()
)

null_skus = (
    dim_product
    .filter(F.col("SKU").isNull())
    .count()
)

# Check whether any variation failed to find its parent item
orphan_variations = (
    dim_product
    .filter(F.col("ItemName").isNull())
    .count()
)

print("Total products           :", total_products)
print("Duplicate ProductKeys    :", duplicate_product_keys)
print("NULL ProductKeys         :", null_product_keys)
print("Duplicate Variation IDs  :", duplicate_variation_ids)
print("NULL Variation IDs       :", null_variation_ids)
print("NULL Item IDs            :", null_item_ids)
print("NULL SKUs                :", null_skus)
print("Orphan variations        :", orphan_variations)

print()
print("Sample products:")
dim_product.show(10, truncate=False)

# ============================================================
# SAVE GOLD
# ============================================================

(
    dim_product
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.dim_product")
)

print()
print("=" * 100)
print("✅ GOLD DIM PRODUCT SAVED")
print("=" * 100)

print("Table: gold.dim_product")
print("Rows :", dim_product.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ****dim_location****

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("=" * 100)
print("GOLD DIM LOCATION")
print("=" * 100)

# ============================================================
# READ SILVER LOCATION
# ============================================================

silver_location = spark.table("silver.location")

print("Silver location rows:", silver_location.count())

print()
print("Silver location schema:")
silver_location.printSchema()

# ============================================================
# REMOVE 100% NULL COLUMNS
# ============================================================

columns_to_keep = []

for column_name in silver_location.columns:

    non_null_count = (
        silver_location
        .filter(F.col(column_name).isNotNull())
        .limit(1)
        .count()
    )

    if non_null_count > 0:
        columns_to_keep.append(column_name)

silver_location = silver_location.select(*columns_to_keep)

print()
print("=" * 100)
print("COLUMNS RETAINED")
print("=" * 100)

print(columns_to_keep)

# ============================================================
# CREATE SURROGATE KEY
# ============================================================

window_spec = (
    Window
    .orderBy("SquareLocationID")
)

dim_location = (
    silver_location
    .withColumn(
        "LocationKey",
        F.row_number().over(window_spec)
    )
)

# ============================================================
# PUT SURROGATE KEY FIRST
# ============================================================

dim_location = dim_location.select(
    "LocationKey",
    *[
        c for c in dim_location.columns
        if c != "LocationKey"
    ]
)

# ============================================================
# VALIDATION
# ============================================================

print()
print("=" * 100)
print("GOLD DIM LOCATION VALIDATION")
print("=" * 100)

total_locations = dim_location.count()

duplicate_location_keys = (
    dim_location
    .groupBy("LocationKey")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_location_keys = (
    dim_location
    .filter(F.col("LocationKey").isNull())
    .count()
)

duplicate_square_location_ids = (
    dim_location
    .groupBy("SquareLocationID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_square_location_ids = (
    dim_location
    .filter(F.col("SquareLocationID").isNull())
    .count()
)

print("Total locations              :", total_locations)
print("Duplicate LocationKeys       :", duplicate_location_keys)
print("NULL LocationKeys            :", null_location_keys)
print("Duplicate Square Location IDs:", duplicate_square_location_ids)
print("NULL Square Location IDs     :", null_square_location_ids)

print()
print("Location data:")
dim_location.show(truncate=False)

# ============================================================
# SAVE GOLD
# ============================================================

(
    dim_location
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.dim_location")
)

print()
print("=" * 100)
print("✅ GOLD DIM LOCATION SAVED")
print("=" * 100)

print("Table: gold.dim_location")
print("Rows :", dim_location.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ****fact_orders****

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("=" * 100)
print("GOLD FACT ORDER")
print("=" * 100)

# ============================================================
# READ TABLES
# ============================================================

order_header = spark.table("silver.order_header")

date_mapping = spark.table("gold.gold_order_date_mapping")

dim_date = spark.table("gold.dim_date")

dim_customer = spark.table("gold.dim_customer")

dim_location = spark.table("gold.dim_location")

print("Silver orders :", order_header.count())
print("Date mappings :", date_mapping.count())
print("Dim date      :", dim_date.count())
print("Dim customer  :", dim_customer.count())
print("Dim location  :", dim_location.count())


# ============================================================
# JOIN ORDER → REPORTING DATE MAPPING
# ============================================================

fact_order = (
    order_header.alias("o")
    .join(
        date_mapping.alias("m"),
        F.col("o.SquareOrderID") == F.col("m.SquareOrderID"),
        "left"
    )
)


# ============================================================
# JOIN CUSTOMER
# ============================================================

fact_order = (
    fact_order
    .join(
        dim_customer.alias("c"),
        F.col("o.SquareCustomerID") ==
        F.col("c.SquareCustomerID"),
        "left"
    )
)


# ============================================================
# JOIN LOCATION
#
# IMPORTANT:
# Silver column = LocationID
# Gold dimension column = SquareLocationID
# ============================================================

fact_order = (
    fact_order
    .join(
        dim_location.alias("l"),
        F.col("o.LocationID") ==
        F.col("l.SquareLocationID"),
        "left"
    )
)


# ============================================================
# JOIN DATE DIMENSION
# ============================================================

fact_order = (
    fact_order
    .join(
        dim_date.alias("d"),
        F.col("m.ReportingOrderDate") ==
        F.col("d.Date"),
        "left"
    )
)


# ============================================================
# CREATE FACT ORDER
# ============================================================

fact_order = (
    fact_order
    .select(

        # ----------------------------------------------------
        # BUSINESS IDENTIFIERS
        # ----------------------------------------------------

        F.col("o.SquareOrderID")
            .alias("SquareOrderID"),

        F.col("o.BusinessOrderID")
            .alias("BusinessOrderID"),

        # ----------------------------------------------------
        # FOREIGN KEYS
        # ----------------------------------------------------

        F.col("d.DateKey")
            .alias("DateKey"),

        F.col("c.CustomerKey")
            .alias("CustomerKey"),

        F.col("l.LocationKey")
            .alias("LocationKey"),

        # ----------------------------------------------------
        # ORDER ATTRIBUTES
        # ----------------------------------------------------

        F.col("o.OrderState")
            .alias("OrderState"),

        F.col("o.OrderVersion")
            .alias("OrderVersion"),

        F.col("o.Currency")
            .alias("Currency"),

        F.col("o.CreatedAt")
            .alias("CreatedAt"),

        F.col("o.UpdatedAt")
            .alias("UpdatedAt"),

        F.col("o.ClosedAt")
            .alias("ClosedAt"),

        # ----------------------------------------------------
        # ORDER AMOUNTS
        # ----------------------------------------------------

        F.col("o.TotalAmount")
            .alias("TotalAmount"),

        F.col("o.TotalTaxAmount")
            .alias("TotalTaxAmount"),

        F.col("o.TotalDiscountAmount")
            .alias("TotalDiscountAmount"),

        F.col("o.TotalTipAmount")
            .alias("TotalTipAmount"),

        F.col("o.TotalServiceChargeAmount")
            .alias("TotalServiceChargeAmount"),

        F.col("o.TotalCardSurchargeAmount")
            .alias("TotalCardSurchargeAmount"),

        # ----------------------------------------------------
        # NET AMOUNTS
        # ----------------------------------------------------

        F.col("o.NetTotalAmount")
            .alias("NetTotalAmount"),

        F.col("o.NetTaxAmount")
            .alias("NetTaxAmount"),

        F.col("o.NetDiscountAmount")
            .alias("NetDiscountAmount"),

        F.col("o.NetTipAmount")
            .alias("NetTipAmount"),

        F.col("o.NetServiceChargeAmount")
            .alias("NetServiceChargeAmount"),

        F.col("o.NetCardSurchargeAmount")
            .alias("NetCardSurchargeAmount"),

        F.col("o.NetAmountDue")
            .alias("NetAmountDue")
    )
)


# ============================================================
# CREATE ORDER SURROGATE KEY
# ============================================================

window_spec = Window.orderBy("SquareOrderID")

fact_order = (
    fact_order
    .withColumn(
        "OrderKey",
        F.row_number().over(window_spec)
    )
)


# ============================================================
# PUT OrderKey FIRST
# ============================================================

fact_order = fact_order.select(
    "OrderKey",
    *[
        c for c in fact_order.columns
        if c != "OrderKey"
    ]
)


# ============================================================
# VALIDATION
# ============================================================

print()
print("=" * 100)
print("GOLD FACT ORDER VALIDATION")
print("=" * 100)

total_orders = fact_order.count()

duplicate_order_keys = (
    fact_order
    .groupBy("OrderKey")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

duplicate_square_orders = (
    fact_order
    .groupBy("SquareOrderID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_order_keys = (
    fact_order
    .filter(F.col("OrderKey").isNull())
    .count()
)

null_square_orders = (
    fact_order
    .filter(F.col("SquareOrderID").isNull())
    .count()
)

null_business_orders = (
    fact_order
    .filter(F.col("BusinessOrderID").isNull())
    .count()
)

null_date_keys = (
    fact_order
    .filter(F.col("DateKey").isNull())
    .count()
)

null_customer_keys = (
    fact_order
    .filter(F.col("CustomerKey").isNull())
    .count()
)

null_location_keys = (
    fact_order
    .filter(F.col("LocationKey").isNull())
    .count()
)

print("Total orders            :", total_orders)
print("Duplicate OrderKeys     :", duplicate_order_keys)
print("Duplicate Square IDs    :", duplicate_square_orders)
print("NULL OrderKeys          :", null_order_keys)
print("NULL Square Order IDs   :", null_square_orders)
print("NULL Business Order IDs :", null_business_orders)
print("NULL DateKeys           :", null_date_keys)
print("NULL CustomerKeys       :", null_customer_keys)
print("NULL LocationKeys       :", null_location_keys)


# ============================================================
# SHOW SAMPLE
# ============================================================

print()
print("=" * 100)
print("FACT ORDER SAMPLE")
print("=" * 100)

fact_order.show(10, truncate=False)


# ============================================================
# SAVE GOLD
# ============================================================

(
    fact_order
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.fact_order")
)

print()
print("=" * 100)
print("✅ GOLD FACT ORDER SAVED")
print("=" * 100)

print("Table: gold.fact_order")
print("Rows :", fact_order.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ****fact_sales****

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("=" * 100)
print("GOLD FACT SALES")
print("=" * 100)

# ============================================================
# READ SOURCE / GOLD TABLES
# ============================================================

order_line = spark.table("silver.order_line")

fact_order = spark.table("gold.fact_order")

dim_product = spark.table("gold.dim_product")

print("Silver order lines :", order_line.count())
print("Gold fact orders   :", fact_order.count())
print("Gold products      :", dim_product.count())


# ============================================================
# SHOW SOURCE SCHEMA
# ============================================================

print()
print("=" * 100)
print("ORDER LINE SCHEMA")
print("=" * 100)

order_line.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("=" * 100)
print("GOLD FACT SALES")
print("=" * 100)

# ============================================================
# READ TABLES
# ============================================================

order_line = spark.table("silver.order_line")

fact_order = spark.table("gold.fact_order")

dim_product = spark.table("gold.dim_product")

print("Silver order lines :", order_line.count())
print("Gold fact orders   :", fact_order.count())
print("Gold products      :", dim_product.count())


# ============================================================
# JOIN ORDER LINE → FACT ORDER
#
# This gives us:
# OrderKey
# DateKey
# CustomerKey
# LocationKey
# ============================================================

sales_df = (
    order_line.alias("ol")
    .join(
        fact_order.alias("fo"),
        F.col("ol.SquareOrderID") ==
        F.col("fo.SquareOrderID"),
        "left"
    )
)


# ============================================================
# JOIN ORDER LINE → PRODUCT DIMENSION
#
# SquareVariationID → ProductKey
# ============================================================

sales_df = (
    sales_df
    .join(
        dim_product.alias("p"),
        F.col("ol.SquareVariationID") ==
        F.col("p.SquareVariationID"),
        "left"
    )
)


# ============================================================
# CREATE FACT SALES
# ============================================================

fact_sales = (
    sales_df
    .select(

        # ----------------------------------------------------
        # FOREIGN KEYS
        # ----------------------------------------------------

        F.col("fo.OrderKey")
            .alias("OrderKey"),

        F.col("fo.DateKey")
            .alias("DateKey"),

        F.col("fo.CustomerKey")
            .alias("CustomerKey"),

        F.col("p.ProductKey")
            .alias("ProductKey"),

        F.col("fo.LocationKey")
            .alias("LocationKey"),

        # ----------------------------------------------------
        # BUSINESS IDENTIFIERS
        # ----------------------------------------------------

        F.col("ol.SquareOrderID")
            .alias("SquareOrderID"),

        F.col("ol.BusinessOrderID")
            .alias("BusinessOrderID"),

        F.col("ol.LineItemUID")
            .alias("SquareOrderLineID"),

        # ----------------------------------------------------
        # PRODUCT INFORMATION
        # ----------------------------------------------------

        F.col("ol.SquareVariationID")
            .alias("SquareVariationID"),

        F.col("ol.ItemName")
            .alias("ItemName"),

        F.col("ol.VariationName")
            .alias("VariationName"),

        F.col("ol.ItemType")
            .alias("ItemType"),

        # ----------------------------------------------------
        # QUANTITY
        # ----------------------------------------------------

        F.col("ol.Quantity")
            .alias("Quantity"),

        # ----------------------------------------------------
        # SALES AMOUNTS
        # ----------------------------------------------------

        F.col("ol.BasePriceAmount")
            .alias("UnitPrice"),

        F.col("ol.GrossSalesAmount")
            .alias("GrossSalesAmount"),

        F.col("ol.DiscountAmount")
            .alias("DiscountAmount"),

        F.col("ol.TaxAmount")
            .alias("TaxAmount"),

        F.col("ol.ServiceChargeAmount")
            .alias("ServiceChargeAmount"),

        F.col("ol.CardSurchargeAmount")
            .alias("CardSurchargeAmount"),

        F.col("ol.TotalAmount")
            .alias("TotalAmount"),

        F.col("ol.VariationTotalPriceAmount")
            .alias("VariationTotalPriceAmount"),

        F.col("ol.Currency")
            .alias("Currency")
    )
)


# ============================================================
# CREATE SALES SURROGATE KEY
#
# One row = one order line
# ============================================================

window_spec = (
    Window
    .orderBy(
        "SquareOrderID",
        "SquareOrderLineID"
    )
)

fact_sales = (
    fact_sales
    .withColumn(
        "SalesKey",
        F.row_number().over(window_spec)
    )
)


# ============================================================
# PUT SalesKey FIRST
# ============================================================

fact_sales = fact_sales.select(
    "SalesKey",
    *[
        c for c in fact_sales.columns
        if c != "SalesKey"
    ]
)


# ============================================================
# VALIDATION
# ============================================================

print()
print("=" * 100)
print("GOLD FACT SALES VALIDATION")
print("=" * 100)

# ------------------------------------------------------------
# Row count
# ------------------------------------------------------------

total_sales_lines = fact_sales.count()


# ------------------------------------------------------------
# Duplicate Sales Keys
# ------------------------------------------------------------

duplicate_sales_keys = (
    fact_sales
    .groupBy("SalesKey")
    .count()
    .filter(F.col("count") > 1)
    .count()
)


# ------------------------------------------------------------
# Duplicate Order + Line
# ------------------------------------------------------------

duplicate_order_lines = (
    fact_sales
    .groupBy(
        "SquareOrderID",
        "SquareOrderLineID"
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)


# ------------------------------------------------------------
# NULL CHECKS
# ------------------------------------------------------------

null_sales_keys = (
    fact_sales
    .filter(F.col("SalesKey").isNull())
    .count()
)

null_order_keys = (
    fact_sales
    .filter(F.col("OrderKey").isNull())
    .count()
)

null_date_keys = (
    fact_sales
    .filter(F.col("DateKey").isNull())
    .count()
)

null_customer_keys = (
    fact_sales
    .filter(F.col("CustomerKey").isNull())
    .count()
)

null_product_keys = (
    fact_sales
    .filter(F.col("ProductKey").isNull())
    .count()
)

null_location_keys = (
    fact_sales
    .filter(F.col("LocationKey").isNull())
    .count()
)

null_order_ids = (
    fact_sales
    .filter(F.col("SquareOrderID").isNull())
    .count()
)

null_line_ids = (
    fact_sales
    .filter(F.col("SquareOrderLineID").isNull())
    .count()
)

null_variation_ids = (
    fact_sales
    .filter(F.col("SquareVariationID").isNull())
    .count()
)

null_quantities = (
    fact_sales
    .filter(F.col("Quantity").isNull())
    .count()
)


# ============================================================
# PRINT VALIDATION
# ============================================================

print("Total sales lines       :", total_sales_lines)
print("Duplicate SalesKeys     :", duplicate_sales_keys)
print("Duplicate Order + Line  :", duplicate_order_lines)

print("NULL SalesKeys          :", null_sales_keys)
print("NULL OrderKeys          :", null_order_keys)
print("NULL DateKeys           :", null_date_keys)
print("NULL CustomerKeys       :", null_customer_keys)
print("NULL ProductKeys        :", null_product_keys)
print("NULL LocationKeys       :", null_location_keys)

print("NULL Square Order IDs   :", null_order_ids)
print("NULL Line Item IDs      :", null_line_ids)
print("NULL Variation IDs      :", null_variation_ids)
print("NULL Quantities         :", null_quantities)


# ============================================================
# SHOW SAMPLE
# ============================================================

print()
print("=" * 100)
print("FACT SALES SAMPLE")
print("=" * 100)

fact_sales.show(10, truncate=False)


# ============================================================
# SAVE GOLD
# ============================================================

(
    fact_sales
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.fact_sales")
)


print()
print("=" * 100)
print("✅ GOLD FACT SALES SAVED")
print("=" * 100)

print("Table: gold.fact_sales")
print("Rows :", fact_sales.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# validation

# CELL ********************

from pyspark.sql import functions as F

print("=" * 100)
print("SILVER → GOLD SALES RECONCILIATION")
print("=" * 100)

# ============================================================
# READ TABLES
# ============================================================

silver_sales = spark.table("silver.order_line")

gold_sales = spark.table("gold.fact_sales")


# ============================================================
# SILVER TOTALS
# ============================================================

silver_summary = (
    silver_sales
    .agg(
        F.count("*").alias("RowCount"),

        F.sum("Quantity").alias("TotalQuantity"),

        F.sum("GrossSalesAmount").alias("GrossSalesAmount"),

        F.sum("DiscountAmount").alias("DiscountAmount"),

        F.sum("TaxAmount").alias("TaxAmount"),

        F.sum("ServiceChargeAmount").alias(
            "ServiceChargeAmount"
        ),

        F.sum("CardSurchargeAmount").alias(
            "CardSurchargeAmount"
        ),

        F.sum("TotalAmount").alias("TotalAmount"),

        F.sum("VariationTotalPriceAmount").alias(
            "VariationTotalPriceAmount"
        )
    )
)


# ============================================================
# GOLD TOTALS
# ============================================================

gold_summary = (
    gold_sales
    .agg(
        F.count("*").alias("RowCount"),

        F.sum("Quantity").alias("TotalQuantity"),

        F.sum("GrossSalesAmount").alias("GrossSalesAmount"),

        F.sum("DiscountAmount").alias("DiscountAmount"),

        F.sum("TaxAmount").alias("TaxAmount"),

        F.sum("ServiceChargeAmount").alias(
            "ServiceChargeAmount"
        ),

        F.sum("CardSurchargeAmount").alias(
            "CardSurchargeAmount"
        ),

        F.sum("TotalAmount").alias("TotalAmount"),

        F.sum("VariationTotalPriceAmount").alias(
            "VariationTotalPriceAmount"
        )
    )
)


# ============================================================
# DISPLAY SILVER
# ============================================================

print()
print("=" * 100)
print("SILVER TOTALS")
print("=" * 100)

silver_summary.show(truncate=False)


# ============================================================
# DISPLAY GOLD
# ============================================================

print()
print("=" * 100)
print("GOLD TOTALS")
print("=" * 100)

gold_summary.show(truncate=False)


# ============================================================
# COMPARE
# ============================================================

comparison = (
    silver_summary.alias("s")
    .crossJoin(gold_summary.alias("g"))
    .select(

        F.col("s.RowCount").alias("SilverRows"),
        F.col("g.RowCount").alias("GoldRows"),

        F.col("s.TotalQuantity").alias("SilverQuantity"),
        F.col("g.TotalQuantity").alias("GoldQuantity"),

        F.col("s.GrossSalesAmount").alias(
            "SilverGrossSales"
        ),
        F.col("g.GrossSalesAmount").alias(
            "GoldGrossSales"
        ),

        F.col("s.DiscountAmount").alias(
            "SilverDiscount"
        ),
        F.col("g.DiscountAmount").alias(
            "GoldDiscount"
        ),

        F.col("s.TaxAmount").alias(
            "SilverTax"
        ),
        F.col("g.TaxAmount").alias(
            "GoldTax"
        ),

        F.col("s.ServiceChargeAmount").alias(
            "SilverServiceCharge"
        ),
        F.col("g.ServiceChargeAmount").alias(
            "GoldServiceCharge"
        ),

        F.col("s.CardSurchargeAmount").alias(
            "SilverCardSurcharge"
        ),
        F.col("g.CardSurchargeAmount").alias(
            "GoldCardSurcharge"
        ),

        F.col("s.TotalAmount").alias(
            "SilverTotalAmount"
        ),
        F.col("g.TotalAmount").alias(
            "GoldTotalAmount"
        ),

        F.col("s.VariationTotalPriceAmount").alias(
            "SilverVariationTotal"
        ),
        F.col("g.VariationTotalPriceAmount").alias(
            "GoldVariationTotal"
        )
    )
)

print()
print("=" * 100)
print("SILVER → GOLD COMPARISON")
print("=" * 100)

comparison.show(truncate=False)


# ============================================================
# DIFFERENCE CHECK
# ============================================================

difference = (
    silver_summary.alias("s")
    .crossJoin(gold_summary.alias("g"))
    .select(
        (F.col("s.RowCount") -
         F.col("g.RowCount")).alias(
             "RowDifference"
         ),

        (F.col("s.TotalQuantity") -
         F.col("g.TotalQuantity")).alias(
             "QuantityDifference"
         ),

        (F.col("s.GrossSalesAmount") -
         F.col("g.GrossSalesAmount")).alias(
             "GrossSalesDifference"
         ),

        (F.col("s.DiscountAmount") -
         F.col("g.DiscountAmount")).alias(
             "DiscountDifference"
         ),

        (F.col("s.TaxAmount") -
         F.col("g.TaxAmount")).alias(
             "TaxDifference"
         ),

        (F.col("s.ServiceChargeAmount") -
         F.col("g.ServiceChargeAmount")).alias(
             "ServiceChargeDifference"
         ),

        (F.col("s.CardSurchargeAmount") -
         F.col("g.CardSurchargeAmount")).alias(
             "CardSurchargeDifference"
         ),

        (F.col("s.TotalAmount") -
         F.col("g.TotalAmount")).alias(
             "TotalAmountDifference"
         ),

        (F.col("s.VariationTotalPriceAmount") -
         F.col("g.VariationTotalPriceAmount")).alias(
             "VariationTotalDifference"
         )
    )
)

print()
print("=" * 100)
print("RECONCILIATION DIFFERENCES")
print("=" * 100)

difference.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ****fact_payment****

# CELL ********************

payment = spark.table("silver.payment")

print("=" * 100)
print("SILVER PAYMENT SCHEMA")
print("=" * 100)

print("Payment rows:", payment.count())

payment.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("=" * 100)
print("GOLD FACT PAYMENT")
print("=" * 100)

# ============================================================
# READ TABLES
# ============================================================

payment = spark.table("silver.payment")

fact_order = spark.table("gold.fact_order")

print("Silver payments :", payment.count())
print("Gold fact orders:", fact_order.count())


# ============================================================
# JOIN PAYMENT → FACT ORDER
#
# This gives us:
# OrderKey
# DateKey
# CustomerKey
# LocationKey
# ============================================================

payment_df = (
    payment.alias("p")
    .join(
        fact_order.alias("fo"),
        F.col("p.SquareOrderID") ==
        F.col("fo.SquareOrderID"),
        "left"
    )
)


# ============================================================
# CREATE FACT PAYMENT
# ============================================================

fact_payment = (
    payment_df
    .select(

        # ----------------------------------------------------
        # FOREIGN KEYS
        # ----------------------------------------------------

        F.col("fo.OrderKey")
            .alias("OrderKey"),

        F.col("fo.DateKey")
            .alias("DateKey"),

        F.col("fo.CustomerKey")
            .alias("CustomerKey"),

        F.col("fo.LocationKey")
            .alias("LocationKey"),

        # ----------------------------------------------------
        # PAYMENT IDENTIFIERS
        # ----------------------------------------------------

        F.col("p.SquarePaymentID")
            .alias("SquarePaymentID"),

        F.col("p.SquareOrderID")
            .alias("SquareOrderID"),

        F.col("p.SquareCustomerID")
            .alias("SquareCustomerID"),

        # ----------------------------------------------------
        # PAYMENT AMOUNTS
        # ----------------------------------------------------

        F.col("p.Amount")
            .alias("Amount"),

        F.col("p.Currency")
            .alias("Currency"),

        F.col("p.TotalAmount")
            .alias("TotalAmount"),

        F.col("p.TotalCurrency")
            .alias("TotalCurrency"),

        # ----------------------------------------------------
        # PAYMENT ATTRIBUTES
        # ----------------------------------------------------

        F.col("p.PaymentStatus")
            .alias("PaymentStatus"),

        F.col("p.PaymentSourceType")
            .alias("PaymentSourceType"),

        F.col("p.CreatedAt")
            .alias("CreatedAt"),

        F.col("p.UpdatedAt")
            .alias("UpdatedAt"),

        F.col("p.ReceiptNumber")
            .alias("ReceiptNumber"),

        # ----------------------------------------------------
        # EXTERNAL PAYMENT DETAILS
        # ----------------------------------------------------

        F.col("p.ExternalSource")
            .alias("ExternalSource"),

        F.col("p.ExternalType")
            .alias("ExternalType"),

        # ----------------------------------------------------
        # CARD DETAILS
        # ----------------------------------------------------

        F.col("p.CardBrand")
            .alias("CardBrand"),

        F.col("p.CardType")
            .alias("CardType"),

        F.col("p.CardLast4")
            .alias("CardLast4"),

        F.col("p.CardEntryMethod")
            .alias("CardEntryMethod"),

        F.col("p.CardStatus")
            .alias("CardStatus"),

        F.col("p.AVSStatus")
            .alias("AVSStatus"),

        F.col("p.CVVStatus")
            .alias("CVVStatus"),

        # ----------------------------------------------------
        # CASH DETAILS
        # ----------------------------------------------------

        F.col("p.CashBuyerSuppliedAmount")
            .alias("CashBuyerSuppliedAmount"),

        F.col("p.CashChangeBackAmount")
            .alias("CashChangeBackAmount"),

        # ----------------------------------------------------
        # RISK / APPLICATION
        # ----------------------------------------------------

        F.col("p.RiskLevel")
            .alias("RiskLevel"),

        F.col("p.ApplicationID")
            .alias("ApplicationID"),

        F.col("p.SquareProduct")
            .alias("SquareProduct"),

        F.col("p.VersionToken")
            .alias("VersionToken")
    )
)


# ============================================================
# CREATE PAYMENT SURROGATE KEY
#
# One row = one payment
# ============================================================

window_spec = (
    Window
    .orderBy("SquarePaymentID")
)

fact_payment = (
    fact_payment
    .withColumn(
        "PaymentKey",
        F.row_number().over(window_spec)
    )
)


# ============================================================
# PUT PaymentKey FIRST
# ============================================================

fact_payment = fact_payment.select(
    "PaymentKey",
    *[
        c for c in fact_payment.columns
        if c != "PaymentKey"
    ]
)


# ============================================================
# VALIDATION
# ============================================================

print()
print("=" * 100)
print("GOLD FACT PAYMENT VALIDATION")
print("=" * 100)

# ------------------------------------------------------------
# Row count
# ------------------------------------------------------------

total_payments = fact_payment.count()


# ------------------------------------------------------------
# Duplicate Payment Keys
# ------------------------------------------------------------

duplicate_payment_keys = (
    fact_payment
    .groupBy("PaymentKey")
    .count()
    .filter(F.col("count") > 1)
    .count()
)


# ------------------------------------------------------------
# Duplicate Square Payment IDs
# ------------------------------------------------------------

duplicate_payment_ids = (
    fact_payment
    .groupBy("SquarePaymentID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)


# ------------------------------------------------------------
# NULL CHECKS
# ------------------------------------------------------------

null_payment_keys = (
    fact_payment
    .filter(F.col("PaymentKey").isNull())
    .count()
)

null_payment_ids = (
    fact_payment
    .filter(F.col("SquarePaymentID").isNull())
    .count()
)

null_order_ids = (
    fact_payment
    .filter(F.col("SquareOrderID").isNull())
    .count()
)

null_amounts = (
    fact_payment
    .filter(F.col("Amount").isNull())
    .count()
)

null_order_keys = (
    fact_payment
    .filter(F.col("OrderKey").isNull())
    .count()
)

null_date_keys = (
    fact_payment
    .filter(F.col("DateKey").isNull())
    .count()
)

null_customer_keys = (
    fact_payment
    .filter(F.col("CustomerKey").isNull())
    .count()
)

null_location_keys = (
    fact_payment
    .filter(F.col("LocationKey").isNull())
    .count()
)


print("Total payments         :", total_payments)
print("Duplicate PaymentKeys  :", duplicate_payment_keys)
print("Duplicate Payment IDs  :", duplicate_payment_ids)

print("NULL PaymentKeys       :", null_payment_keys)
print("NULL Payment IDs       :", null_payment_ids)
print("NULL Order IDs         :", null_order_ids)
print("NULL Amounts           :", null_amounts)

print("NULL OrderKeys         :", null_order_keys)
print("NULL DateKeys          :", null_date_keys)
print("NULL CustomerKeys      :", null_customer_keys)
print("NULL LocationKeys      :", null_location_keys)


# ============================================================
# PAYMENT STATUS
# ============================================================

print()
print("=" * 100)
print("PAYMENT STATUS")
print("=" * 100)

fact_payment.groupBy(
    "PaymentStatus"
).count().show()


# ============================================================
# PAYMENT SOURCE TYPE
# ============================================================

print()
print("=" * 100)
print("PAYMENT SOURCE TYPE")
print("=" * 100)

fact_payment.groupBy(
    "PaymentSourceType"
).count().show()


# ============================================================
# SAMPLE
# ============================================================

print()
print("=" * 100)
print("FACT PAYMENT SAMPLE")
print("=" * 100)

fact_payment.show(
    10,
    truncate=False
)


# ============================================================
# SAVE GOLD
# ============================================================

(
    fact_payment
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.fact_payment")
)


print()
print("=" * 100)
print("✅ GOLD FACT PAYMENT SAVED")
print("=" * 100)

print("Table: gold.fact_payment")
print("Rows :", fact_payment.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# validation payment --> tender

# CELL ********************

from pyspark.sql import functions as F

print("=" * 100)
print("GOLD PAYMENT → TENDER VALIDATION")
print("=" * 100)

# ============================================================
# READ TABLES
# ============================================================

fact_payment = spark.table("gold.fact_payment")

order_tender = spark.table("silver.order_tender")

print("Gold payments :", fact_payment.count())
print("Tender rows   :", order_tender.count())


# ============================================================
# PAYMENT → TENDER MATCH
# ============================================================

payment_tender_check = (
    fact_payment.alias("p")
    .join(
        order_tender.alias("t"),
        F.col("p.SquarePaymentID") ==
        F.col("t.PaymentID"),
        "left"
    )
)


# ============================================================
# ORPHAN PAYMENTS
# ============================================================

orphan_payments = (
    payment_tender_check
    .filter(F.col("t.PaymentID").isNull())
    .select("p.SquarePaymentID")
    .distinct()
    .count()
)


# ============================================================
# ORPHAN TENDERS
# ============================================================

orphan_tenders = (
    order_tender.alias("t")
    .join(
        fact_payment.alias("p"),
        F.col("t.PaymentID") ==
        F.col("p.SquarePaymentID"),
        "left"
    )
    .filter(F.col("p.SquarePaymentID").isNull())
    .select("t.PaymentID")
    .distinct()
    .count()
)


# ============================================================
# AMOUNT RECONCILIATION
# ============================================================

payment_amounts = (
    fact_payment
    .select(
        F.col("SquarePaymentID"),
        F.col("Amount").alias("PaymentAmount")
    )
)

tender_amounts = (
    order_tender
    .select(
        F.col("PaymentID"),
        F.col("Amount").alias("TenderAmount")
    )
)

amount_check = (
    payment_amounts.alias("p")
    .join(
        tender_amounts.alias("t"),
        F.col("p.SquarePaymentID") ==
        F.col("t.PaymentID"),
        "inner"
    )
    .withColumn(
        "AmountDifference",
        F.col("p.PaymentAmount") -
        F.col("t.TenderAmount")
    )
)


matched_records = amount_check.count()

amount_matches = (
    amount_check
    .filter(F.col("AmountDifference") == 0)
    .count()
)

amount_differences = (
    amount_check
    .filter(F.col("AmountDifference") != 0)
    .count()
)


# ============================================================
# RESULTS
# ============================================================

print()
print("=" * 100)
print("PAYMENT → TENDER VALIDATION")
print("=" * 100)

print("Gold payments   :", fact_payment.count())
print("Tender rows     :", order_tender.count())
print("Orphan payments :", orphan_payments)
print("Orphan tenders  :", orphan_tenders)

print()
print("=" * 100)
print("PAYMENT → TENDER AMOUNT RECONCILIATION")
print("=" * 100)

print("Matched records     :", matched_records)
print("Amount matches      :", amount_matches)
print("Amount differences  :", amount_differences)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ****fact_inventory****

# CELL ********************

inventory = spark.table("silver.inventory")

print("=" * 100)
print("SILVER INVENTORY SCHEMA")
print("=" * 100)

print("Inventory rows:", inventory.count())

inventory.printSchema()

print()
print("=" * 100)
print("INVENTORY SAMPLE")
print("=" * 100)

inventory.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window


print("=" * 100)
print("GOLD FACT INVENTORY")
print("=" * 100)

# ============================================================
# READ TABLES
# ============================================================

inventory = spark.table("silver.inventory")

dim_product = spark.table("gold.dim_product")

dim_location = spark.table("gold.dim_location")

dim_date = spark.table("gold.dim_date")


print("Silver inventory :", inventory.count())
print("Dim product      :", dim_product.count())
print("Dim location     :", dim_location.count())
print("Dim date         :", dim_date.count())


# ============================================================
# PREPARE INVENTORY DATE
# ============================================================
#
# CalculatedAt is a timestamp.
# dim_date.Date is a date.
#
# Therefore convert CalculatedAt → InventoryDate.
# ============================================================

inventory = (
    inventory
    .withColumn(
        "InventoryDate",
        F.to_date(F.col("CalculatedAt"))
    )
)


# ============================================================
# JOIN INVENTORY → PRODUCT
# ============================================================

inventory_df = (
    inventory.alias("i")
    .join(
        dim_product.alias("p"),
        F.col("i.SquareVariationID") ==
        F.col("p.SquareVariationID"),
        "left"
    )
)


# ============================================================
# JOIN INVENTORY → LOCATION
# ============================================================

inventory_df = (
    inventory_df
    .join(
        dim_location.alias("l"),
        F.col("i.LocationID") ==
        F.col("l.SquareLocationID"),
        "left"
    )
)


# ============================================================
# JOIN INVENTORY → DATE
# ============================================================

inventory_df = (
    inventory_df
    .join(
        dim_date.alias("d"),
        F.col("i.InventoryDate") ==
        F.col("d.Date"),
        "left"
    )
)


# ============================================================
# CREATE FACT INVENTORY
# ============================================================

fact_inventory = (
    inventory_df
    .select(

        # ----------------------------------------------------
        # DIMENSION FOREIGN KEYS
        # ----------------------------------------------------

        F.col("d.DateKey")
            .alias("DateKey"),

        F.col("p.ProductKey")
            .alias("ProductKey"),

        F.col("l.LocationKey")
            .alias("LocationKey"),

        # ----------------------------------------------------
        # SOURCE IDENTIFIERS
        # ----------------------------------------------------

        F.col("i.SquareVariationID")
            .alias("SquareVariationID"),

        F.col("i.LocationID")
            .alias("SquareLocationID"),

        F.col("i.CatalogObjectType")
            .alias("CatalogObjectType"),

        # ----------------------------------------------------
        # INVENTORY ATTRIBUTES
        # ----------------------------------------------------

        F.col("i.InventoryState")
            .alias("InventoryState"),

        F.col("i.Quantity")
            .alias("Quantity"),

        # ----------------------------------------------------
        # SNAPSHOT INFORMATION
        # ----------------------------------------------------

        F.col("i.InventoryDate")
            .alias("InventoryDate"),

        F.col("i.CalculatedAt")
            .alias("CalculatedAt")
    )
)


# ============================================================
# CREATE INVENTORY SURROGATE KEY
# ============================================================
#
# Grain:
# Product + Location + Snapshot Date
# ============================================================

fact_inventory = (
    fact_inventory
    .withColumn(
        "InventoryKey",
        F.row_number().over(
            Window.orderBy(
                "SquareVariationID",
                "SquareLocationID",
                "InventoryDate"
            )
        )
    )
)


# ============================================================
# PUT InventoryKey FIRST
# ============================================================

fact_inventory = fact_inventory.select(
    "InventoryKey",
    *[
        c for c in fact_inventory.columns
        if c != "InventoryKey"
    ]
)


# ============================================================
# VALIDATION
# ============================================================

print()
print("=" * 100)
print("GOLD FACT INVENTORY VALIDATION")
print("=" * 100)


# ------------------------------------------------------------
# Total rows
# ------------------------------------------------------------

total_inventory = fact_inventory.count()


# ------------------------------------------------------------
# Duplicate Inventory Keys
# ------------------------------------------------------------

duplicate_inventory_keys = (
    fact_inventory
    .groupBy("InventoryKey")
    .count()
    .filter(F.col("count") > 1)
    .count()
)


# ------------------------------------------------------------
# Duplicate business grain
#
# Product + Location + Date
# ------------------------------------------------------------

duplicate_inventory_grain = (
    fact_inventory
    .groupBy(
        "SquareVariationID",
        "SquareLocationID",
        "InventoryDate"
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)


# ------------------------------------------------------------
# NULL CHECKS
# ------------------------------------------------------------

null_inventory_keys = (
    fact_inventory
    .filter(F.col("InventoryKey").isNull())
    .count()
)

null_product_keys = (
    fact_inventory
    .filter(F.col("ProductKey").isNull())
    .count()
)

null_location_keys = (
    fact_inventory
    .filter(F.col("LocationKey").isNull())
    .count()
)

null_date_keys = (
    fact_inventory
    .filter(F.col("DateKey").isNull())
    .count()
)

null_variation_ids = (
    fact_inventory
    .filter(F.col("SquareVariationID").isNull())
    .count()
)

null_location_ids = (
    fact_inventory
    .filter(F.col("SquareLocationID").isNull())
    .count()
)

null_quantities = (
    fact_inventory
    .filter(F.col("Quantity").isNull())
    .count()
)

null_inventory_dates = (
    fact_inventory
    .filter(F.col("InventoryDate").isNull())
    .count()
)


# ============================================================
# PRINT VALIDATION
# ============================================================

print("Total inventory rows        :", total_inventory)
print("Duplicate InventoryKeys     :", duplicate_inventory_keys)
print("Duplicate Product+Location+Date:",
      duplicate_inventory_grain)

print("NULL InventoryKeys          :", null_inventory_keys)
print("NULL ProductKeys            :", null_product_keys)
print("NULL LocationKeys           :", null_location_keys)
print("NULL DateKeys               :", null_date_keys)

print("NULL Variation IDs          :", null_variation_ids)
print("NULL Location IDs           :", null_location_ids)
print("NULL Quantities             :", null_quantities)
print("NULL Inventory Dates        :", null_inventory_dates)


# ============================================================
# INVENTORY STATE VALIDATION
# ============================================================

print()
print("=" * 100)
print("INVENTORY STATE")
print("=" * 100)

fact_inventory.groupBy(
    "InventoryState"
).count().show()


# ============================================================
# INVENTORY DATE VALIDATION
# ============================================================

print()
print("=" * 100)
print("INVENTORY SNAPSHOT DATE")
print("=" * 100)

fact_inventory.groupBy(
    "InventoryDate"
).count().show()


# ============================================================
# SAMPLE
# ============================================================

print()
print("=" * 100)
print("FACT INVENTORY SAMPLE")
print("=" * 100)

fact_inventory.show(
    10,
    truncate=False
)


# ============================================================
# SAVE GOLD
# ============================================================

(
    fact_inventory
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.fact_inventory")
)


print()
print("=" * 100)
print("✅ GOLD FACT INVENTORY SAVED")
print("=" * 100)

print("Table: gold.fact_inventory")
print("Rows :", fact_inventory.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# validation silver inventory --> gold inventory

# CELL ********************

from pyspark.sql import functions as F

print("=" * 100)
print("SILVER → GOLD INVENTORY RECONCILIATION")
print("=" * 100)

# ============================================================
# READ TABLES
# ============================================================

silver_inventory = spark.table("silver.inventory")

gold_inventory = spark.table("gold.fact_inventory")

print("Silver inventory rows :", silver_inventory.count())
print("Gold inventory rows   :", gold_inventory.count())


# ============================================================
# SILVER TOTALS
# ============================================================

silver_summary = (
    silver_inventory
    .agg(
        F.count("*").alias("RowCount"),

        F.sum("Quantity").alias("TotalQuantity")
    )
)


# ============================================================
# GOLD TOTALS
# ============================================================

gold_summary = (
    gold_inventory
    .agg(
        F.count("*").alias("RowCount"),

        F.sum("Quantity").alias("TotalQuantity")
    )
)


# ============================================================
# SILVER TOTALS
# ============================================================

print()
print("=" * 100)
print("SILVER INVENTORY TOTALS")
print("=" * 100)

silver_summary.show(truncate=False)


# ============================================================
# GOLD TOTALS
# ============================================================

print()
print("=" * 100)
print("GOLD INVENTORY TOTALS")
print("=" * 100)

gold_summary.show(truncate=False)


# ============================================================
# SILVER → GOLD COMPARISON
# ============================================================

comparison = (
    silver_summary.alias("s")
    .crossJoin(gold_summary.alias("g"))
    .select(
        F.col("s.RowCount").alias("SilverRows"),
        F.col("g.RowCount").alias("GoldRows"),

        F.col("s.TotalQuantity").alias(
            "SilverTotalQuantity"
        ),

        F.col("g.TotalQuantity").alias(
            "GoldTotalQuantity"
        )
    )
)

print()
print("=" * 100)
print("SILVER → GOLD INVENTORY COMPARISON")
print("=" * 100)

comparison.show(truncate=False)


# ============================================================
# DIFFERENCE CHECK
# ============================================================

difference = (
    silver_summary.alias("s")
    .crossJoin(gold_summary.alias("g"))
    .select(

        (
            F.col("s.RowCount") -
            F.col("g.RowCount")
        ).alias("RowDifference"),

        (
            F.col("s.TotalQuantity") -
            F.col("g.TotalQuantity")
        ).alias("QuantityDifference")
    )
)

print()
print("=" * 100)
print("INVENTORY RECONCILIATION DIFFERENCES")
print("=" * 100)

difference.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
