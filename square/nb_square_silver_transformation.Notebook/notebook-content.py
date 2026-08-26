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

# ****Silver configuration****

# CELL ********************

# ============================================================
# SQUARE SILVER TRANSFORMATION
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timezone

print("========================================")
print("SQUARE SILVER TRANSFORMATION")
print("========================================")

# ------------------------------------------------------------
# RAW SOURCE
# ------------------------------------------------------------

ORDERS_RAW_PATH = (
#Files/square/orders/"
    "file:/lakehouse/default/Files/square/orders/"#orders_20260820_145407.json
)

# ------------------------------------------------------------
# SILVER TABLE NAMES
# ------------------------------------------------------------

SILVER_ORDER_HEADER = "silver_order_header"
SILVER_ORDER_LINE = "silver_order_line"
SILVER_ORDER_TENDER = "silver_order_tender"

print("Orders source :", ORDERS_RAW_PATH)

print("\nSilver targets:")
print("-", SILVER_ORDER_HEADER)
print("-", SILVER_ORDER_LINE)
print("-", SILVER_ORDER_TENDER)

print("\nConfiguration loaded successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Silver_order_header**

# CELL ********************

# ============================================================
# DISCOVER RAW ORDER FILES
# ============================================================

orders_files = mssparkutils.fs.ls(
    ORDERS_RAW_PATH
)

print("========================================")
print("RAW ORDER FILES")
print("========================================")

for file in orders_files:
    print(file.path)

print("\nTotal files:", len(orders_files))
# ============================================================
# READ RAW ORDERS
# ============================================================

raw_orders_df = (
    spark.read
    .option("multiline", "true")
    .json(ORDERS_RAW_PATH)
)

print("========================================")
print("RAW ORDERS")
print("========================================")

print(
    "Raw files loaded from:",
    ORDERS_RAW_PATH
)

print(
    "Raw order array count:",
    raw_orders_df
    .select(F.size("orders"))
    .first()[0]
)

print("\nRaw schema:")
#raw_orders_df.printSchema()
# ============================================================
# EXPLODE ORDERS
# ============================================================

orders_df = (
    raw_orders_df
    .select(
        F.explode("orders").alias("order")
    )
)

print("========================================")
print("EXPLODED ORDERS")
print("========================================")

print(
    "Order rows:",
    orders_df.count()
)

#orders_df.printSchema()
# ============================================================
# SILVER ORDER HEADER
# Grain: 1 row = 1 order
# ============================================================

silver_order_header_df = orders_df.select(

    # --------------------------------------------------------
    # IDENTIFIERS
    # --------------------------------------------------------

    F.col("order.id").alias("SquareOrderID"),

    F.col("order.reference_id").alias("BusinessOrderID"),

    F.col("order.location_id").alias("LocationID"),

    F.col("order.customer_id").alias("SquareCustomerID"),

    # --------------------------------------------------------
    # ORDER INFORMATION
    # --------------------------------------------------------

    F.col("order.state").alias("OrderState"),

    F.col("order.version").cast("long").alias("OrderVersion"),

    # --------------------------------------------------------
    # TIMESTAMPS
    # --------------------------------------------------------

    F.to_timestamp(
        "order.created_at"
    ).alias("CreatedAt"),

    F.to_timestamp(
        "order.updated_at"
    ).alias("UpdatedAt"),

    F.to_timestamp(
        "order.closed_at"
    ).alias("ClosedAt"),

    # --------------------------------------------------------
    # TOTAL AMOUNTS
    # Square stores money in minor units (cents)
    # --------------------------------------------------------

    F.col(
        "order.total_money.amount"
    ).cast("long").alias("TotalAmount"),

    F.col(
        "order.total_money.currency"
    ).alias("Currency"),

    F.col(
        "order.total_tax_money.amount"
    ).cast("long").alias("TotalTaxAmount"),

    F.col(
        "order.total_discount_money.amount"
    ).cast("long").alias("TotalDiscountAmount"),

    F.col(
        "order.total_tip_money.amount"
    ).cast("long").alias("TotalTipAmount"),

    F.col(
        "order.total_service_charge_money.amount"
    ).cast("long").alias("TotalServiceChargeAmount"),

    F.col(
        "order.total_card_surcharge_money.amount"
    ).cast("long").alias("TotalCardSurchargeAmount"),

    # --------------------------------------------------------
    # NET AMOUNTS
    # --------------------------------------------------------

    F.col(
        "order.net_amounts.total_money.amount"
    ).cast("long").alias("NetTotalAmount"),

    F.col(
        "order.net_amounts.tax_money.amount"
    ).cast("long").alias("NetTaxAmount"),

    F.col(
        "order.net_amounts.discount_money.amount"
    ).cast("long").alias("NetDiscountAmount"),

    F.col(
        "order.net_amounts.tip_money.amount"
    ).cast("long").alias("NetTipAmount"),

    F.col(
        "order.net_amounts.service_charge_money.amount"
    ).cast("long").alias("NetServiceChargeAmount"),

    F.col(
        "order.net_amounts.card_surcharge_money.amount"
    ).cast("long").alias("NetCardSurchargeAmount"),

    # --------------------------------------------------------
    # AMOUNT STILL DUE
    # --------------------------------------------------------

    F.col(
        "order.net_amount_due_money.amount"
    ).cast("long").alias("NetAmountDue"),

    # --------------------------------------------------------
    # SOURCE
    # --------------------------------------------------------

    F.col(
        "order.source.name"
    ).alias("SourceName")
)

# ============================================================
# ORDER HEADER VALIDATION
# ============================================================

print("========================================")
print("ORDER HEADER VALIDATION")
print("========================================")

duplicate_square = (
    silver_order_header_df
    .groupBy("SquareOrderID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

duplicate_business = (
    silver_order_header_df
    .groupBy("BusinessOrderID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_square = (
    silver_order_header_df
    .filter(F.col("SquareOrderID").isNull())
    .count()
)

null_business = (
    silver_order_header_df
    .filter(F.col("BusinessOrderID").isNull())
    .count()
)

print("Total orders          :", silver_order_header_df.count())
print("Duplicate Square IDs  :", duplicate_square)
print("Duplicate Business IDs:", duplicate_business)
print("NULL Square IDs       :", null_square)
print("NULL Business IDs     :", null_business)

# ============================================================
# SAVE SILVER ORDER HEADER
# ============================================================

(
    silver_order_header_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.order_header")
)

print("========================================")
print("✅ SILVER ORDER HEADER SAVED")
print("========================================")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **silver_oreder_line**

# CELL ********************

# ============================================================
# SILVER ORDER LINE
# Step 1: Explode line_items
# ============================================================

order_lines_df = (
    orders_df
    .select(
        F.col("order.id").alias("SquareOrderID"),
        F.col("order.reference_id").alias("BusinessOrderID"),
        F.col("order.location_id").alias("LocationID"),
        F.explode_outer("order.line_items").alias("line")
    )
)

print("========================================")
print("ORDER LINE EXPLOSION")
print("========================================")

print(
    "Order line rows:",
    order_lines_df.count()
)

#order_lines_df.printSchema()
# ============================================================
# SILVER ORDER LINE
# Step 2: Select and standardize columns
# ============================================================

silver_order_line_df = order_lines_df.select(

    # --------------------------------------------------------
    # ORDER RELATIONSHIP
    # --------------------------------------------------------

    F.col("SquareOrderID"),

    F.col("BusinessOrderID"),

    F.col("LocationID"),

    # --------------------------------------------------------
    # LINE IDENTIFIER
    # --------------------------------------------------------

    F.col("line.uid").alias("LineItemUID"),

    # --------------------------------------------------------
    # CATALOG RELATIONSHIP
    # --------------------------------------------------------

    F.col(
        "line.catalog_object_id"
    ).alias("SquareVariationID"),

    F.col(
        "line.catalog_version"
    ).cast("long").alias("CatalogVersion"),

    # --------------------------------------------------------
    # PRODUCT INFORMATION
    # --------------------------------------------------------

    F.col("line.name").alias("ItemName"),

    F.col(
        "line.variation_name"
    ).alias("VariationName"),

    F.col(
        "line.item_type"
    ).alias("ItemType"),

    # --------------------------------------------------------
    # QUANTITY
    # --------------------------------------------------------

    F.col(
        "line.quantity"
    ).cast("decimal(18,3)").alias("Quantity"),

    # --------------------------------------------------------
    # BASE PRICE
    # --------------------------------------------------------

    F.col(
        "line.base_price_money.amount"
    ).cast("long").alias("BasePriceAmount"),

    F.col(
        "line.base_price_money.currency"
    ).alias("Currency"),

    # --------------------------------------------------------
    # SALES AMOUNTS
    # --------------------------------------------------------

    F.col(
        "line.gross_sales_money.amount"
    ).cast("long").alias("GrossSalesAmount"),

    F.col(
        "line.total_discount_money.amount"
    ).cast("long").alias("DiscountAmount"),

    F.col(
        "line.total_tax_money.amount"
    ).cast("long").alias("TaxAmount"),

    F.col(
        "line.total_service_charge_money.amount"
    ).cast("long").alias("ServiceChargeAmount"),

    F.col(
        "line.total_card_surcharge_money.amount"
    ).cast("long").alias("CardSurchargeAmount"),

    F.col(
        "line.total_money.amount"
    ).cast("long").alias("TotalAmount"),

    F.col(
        "line.variation_total_price_money.amount"
    ).cast("long").alias("VariationTotalPriceAmount")
)
# ============================================================
# ORDER LINE VALIDATION
# ============================================================

print("========================================")
print("ORDER LINE VALIDATION")
print("========================================")

total_lines = silver_order_line_df.count()

duplicate_lines = (
    silver_order_line_df
    .groupBy(
        "SquareOrderID",
        "LineItemUID"
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_order_ids = (
    silver_order_line_df
    .filter(
        F.col("SquareOrderID").isNull()
    )
    .count()
)

null_line_ids = (
    silver_order_line_df
    .filter(
        F.col("LineItemUID").isNull()
    )
    .count()
)

null_variation_ids = (
    silver_order_line_df
    .filter(
        F.col("SquareVariationID").isNull()
    )
    .count()
)

null_quantity = (
    silver_order_line_df
    .filter(
        F.col("Quantity").isNull()
    )
    .count()
)

print("Total order lines       :", total_lines)
print("Duplicate Order + Line :", duplicate_lines)
print("NULL Square Order IDs  :", null_order_ids)
print("NULL Line Item IDs     :", null_line_ids)
print("NULL Variation IDs     :", null_variation_ids)
print("NULL Quantities        :", null_quantity)
# ============================================================
# SAVE SILVER ORDER LINE
# ============================================================

(
    silver_order_line_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.order_line")
)

print("========================================")
print("✅ SILVER ORDER LINE SAVED")
print("========================================")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **silver_tender**

# CELL ********************

# ============================================================
# SILVER ORDER TENDER
# Step 1: Explode tenders
# ============================================================

order_tenders_df = (
    orders_df
    .select(
        F.col("order.id").alias("SquareOrderID"),
        F.col("order.reference_id").alias("BusinessOrderID"),
        F.col("order.location_id").alias("OrderLocationID"),
        F.explode_outer("order.tenders").alias("tender")
    )
)

print("========================================")
print("ORDER TENDER EXPLOSION")
print("========================================")

print(
    "Tender rows:",
    order_tenders_df.count()
)

#order_tenders_df.printSchema()

# ============================================================
# SILVER ORDER TENDER
# Step 2: Select and standardize columns
# ============================================================

silver_order_tender_df = order_tenders_df.select(

    # --------------------------------------------------------
    # ORDER RELATIONSHIP
    # --------------------------------------------------------

    F.col("SquareOrderID"),

    F.col("BusinessOrderID"),

    F.col("OrderLocationID"),

    # --------------------------------------------------------
    # TENDER IDENTIFIERS
    # --------------------------------------------------------

    F.col(
        "tender.id"
    ).alias("TenderID"),

    F.col(
        "tender.payment_id"
    ).alias("PaymentID"),

    F.col(
        "tender.transaction_id"
    ).alias("TransactionID"),

    # --------------------------------------------------------
    # TENDER TIMESTAMP
    # --------------------------------------------------------

    F.to_timestamp(
        "tender.created_at"
    ).alias("CreatedAt"),

    # --------------------------------------------------------
    # PAYMENT AMOUNT
    # --------------------------------------------------------

    F.col(
        "tender.amount_money.amount"
    ).cast("long").alias("Amount"),

    F.col(
        "tender.amount_money.currency"
    ).alias("Currency"),

    # --------------------------------------------------------
    # TENDER TYPE
    # --------------------------------------------------------

    F.col(
        "tender.type"
    ).alias("TenderType"),

    # --------------------------------------------------------
    # OTHER PAYMENT DETAILS
    # --------------------------------------------------------

    F.col(
        "tender.other_details.source"
    ).alias("PaymentSource"),

    F.col(
        "tender.other_details.status"
    ).alias("TenderStatus"),

    # --------------------------------------------------------
    # CARD DETAILS
    # --------------------------------------------------------

    F.col(
        "tender.card_details.card.card_brand"
    ).alias("CardBrand"),

    F.col(
        "tender.card_details.card.card_type"
    ).alias("CardType"),

    F.col(
        "tender.card_details.card.last_4"
    ).alias("CardLast4"),

    F.col(
        "tender.card_details.entry_method"
    ).alias("CardEntryMethod"),

    F.col(
        "tender.card_details.status"
    ).alias("CardStatus"),

    # --------------------------------------------------------
    # CASH DETAILS
    # --------------------------------------------------------

    F.col(
        "tender.cash_details.buyer_tendered_money.amount"
    ).cast("long").alias("CashBuyerTenderedAmount"),

    F.col(
        "tender.cash_details.change_back_money.amount"
    ).cast("long").alias("CashChangeBackAmount")
)

print(
    "Columns:",
    len(silver_order_tender_df.columns)
)


# ============================================================
# TENDER VALIDATION
# ============================================================

print("========================================")
print("ORDER TENDER VALIDATION")
print("========================================")

total_tenders = (
    silver_order_tender_df.count()
)

duplicate_tenders = (
    silver_order_tender_df
    .groupBy("TenderID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_tender_ids = (
    silver_order_tender_df
    .filter(F.col("TenderID").isNull())
    .count()
)

null_order_ids = (
    silver_order_tender_df
    .filter(F.col("SquareOrderID").isNull())
    .count()
)

null_payment_ids = (
    silver_order_tender_df
    .filter(F.col("PaymentID").isNull())
    .count()
)

null_amounts = (
    silver_order_tender_df
    .filter(F.col("Amount").isNull())
    .count()
)

print("Total tenders        :", total_tenders)
print("Duplicate Tender IDs :", duplicate_tenders)
print("NULL Tender IDs      :", null_tender_ids)
print("NULL Order IDs       :", null_order_ids)
print("NULL Payment IDs     :", null_payment_ids)
print("NULL Amounts         :", null_amounts)
# ============================================================
# SAVE SILVER ORDER TENDER
# ============================================================

(
    silver_order_tender_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.order_tender")
)

print("========================================")
print("✅ SILVER ORDER TENDER SAVED")
print("========================================")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **silver_payment**

# CELL ********************

# ============================================================
# PAYMENT SOURCE
# ============================================================

PAYMENTS_RAW_PATH = (
    "file:/lakehouse/default/Files/square/payments/"
    
)

SILVER_PAYMENT = "silver_payment"

print("========================================")
print("PAYMENT SILVER TRANSFORMATION")
print("========================================")

print("Payments source :", PAYMENTS_RAW_PATH)
print("Silver target   :", SILVER_PAYMENT)

# ============================================================
# READ RAW PAYMENTS
# ============================================================

raw_payments_df = (
    spark.read
    .option("multiline", "true")
    .json(PAYMENTS_RAW_PATH)
)

print("========================================")
print("RAW PAYMENTS")
print("========================================")

print(
    "Payment array count:",
    raw_payments_df
    .select(F.size("payments"))
    .first()[0]
)

#raw_payments_df.printSchema()
# ============================================================
# EXPLODE PAYMENTS
# ============================================================

payments_df = (
    raw_payments_df
    .select(
        F.explode("payments").alias("payment")
    )
)

print("========================================")
print("EXPLODED PAYMENTS")
print("========================================")

print(
    "Payment rows:",
    payments_df.count()
)

#payments_df.printSchema()

# ============================================================
# SILVER PAYMENT
# ============================================================

silver_payment_df = payments_df.select(

    # --------------------------------------------------------
    # PAYMENT IDENTIFIERS
    # --------------------------------------------------------

    F.col("payment.id")
        .alias("SquarePaymentID"),

    F.col("payment.order_id")
        .alias("SquareOrderID"),

    F.col("payment.customer_id")
        .alias("SquareCustomerID"),

    F.col("payment.location_id")
        .alias("LocationID"),

    # --------------------------------------------------------
    # PAYMENT AMOUNT
    # --------------------------------------------------------

    F.col("payment.amount_money.amount")
        .cast("long")
        .alias("Amount"),

    F.col("payment.amount_money.currency")
        .alias("Currency"),

    # --------------------------------------------------------
    # TOTAL AMOUNT
    # --------------------------------------------------------

    F.col("payment.total_money.amount")
        .cast("long")
        .alias("TotalAmount"),

    F.col("payment.total_money.currency")
        .alias("TotalCurrency"),

    # --------------------------------------------------------
    # PAYMENT STATUS
    # --------------------------------------------------------

    F.col("payment.status")
        .alias("PaymentStatus"),

    F.col("payment.source_type")
        .alias("PaymentSourceType"),

    # --------------------------------------------------------
    # TIMESTAMPS
    # --------------------------------------------------------

    F.to_timestamp(
        "payment.created_at"
    ).alias("CreatedAt"),

    F.to_timestamp(
        "payment.updated_at"
    ).alias("UpdatedAt"),

    # --------------------------------------------------------
    # RECEIPT
    # --------------------------------------------------------

    F.col("payment.receipt_number")
        .alias("ReceiptNumber"),

    # --------------------------------------------------------
    # EXTERNAL PAYMENT
    # --------------------------------------------------------

    F.col("payment.external_details.source")
        .alias("ExternalSource"),

    F.col("payment.external_details.type")
        .alias("ExternalType"),

    # --------------------------------------------------------
    # CARD DETAILS
    # --------------------------------------------------------

    F.col("payment.card_details.card.card_brand")
        .alias("CardBrand"),

    F.col("payment.card_details.card.card_type")
        .alias("CardType"),

    F.col("payment.card_details.card.last_4")
        .alias("CardLast4"),

    F.col("payment.card_details.entry_method")
        .alias("CardEntryMethod"),

    F.col("payment.card_details.status")
        .alias("CardStatus"),

    F.col("payment.card_details.avs_status")
        .alias("AVSStatus"),

    F.col("payment.card_details.cvv_status")
        .alias("CVVStatus"),

    # --------------------------------------------------------
    # CASH DETAILS
    # --------------------------------------------------------

    F.col(
        "payment.cash_details.buyer_supplied_money.amount"
    )
        .cast("long")
        .alias("CashBuyerSuppliedAmount"),

    F.col(
        "payment.cash_details.change_back_money.amount"
    )
        .cast("long")
        .alias("CashChangeBackAmount"),

    # --------------------------------------------------------
    # RISK
    # --------------------------------------------------------

    F.col(
        "payment.risk_evaluation.risk_level"
    )
        .alias("RiskLevel"),

    # --------------------------------------------------------
    # APPLICATION
    # --------------------------------------------------------

    F.col(
        "payment.application_details.application_id"
    )
        .alias("ApplicationID"),

    F.col(
        "payment.application_details.square_product"
    )
        .alias("SquareProduct"),

    # --------------------------------------------------------
    # VERSION
    # --------------------------------------------------------

    F.col(
        "payment.version_token"
    )
        .alias("VersionToken")
)
# ============================================================
# PAYMENT VALIDATION
# ============================================================

print("========================================")
print("PAYMENT VALIDATION")
print("========================================")

total_payments = (
    silver_payment_df.count()
)

duplicate_payment_ids = (
    silver_payment_df
    .groupBy("SquarePaymentID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_payment_ids = (
    silver_payment_df
    .filter(
        F.col("SquarePaymentID").isNull()
    )
    .count()
)

null_amounts = (
    silver_payment_df
    .filter(
        F.col("Amount").isNull()
    )
    .count()
)

null_order_ids = (
    silver_payment_df
    .filter(
        F.col("SquareOrderID").isNull()
    )
    .count()
)

print("Total payments       :", total_payments)
print("Duplicate Payment IDs:", duplicate_payment_ids)
print("NULL Payment IDs     :", null_payment_ids)
print("NULL Amounts         :", null_amounts)
print("NULL Order IDs       :", null_order_ids)

# ============================================================
# SAVE SILVER PAYMENT
# ============================================================

(
    silver_payment_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.payment")
)

print("========================================")
print("✅ SILVER PAYMENT SAVED")
print("========================================")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# TENDER → PAYMENT RELATIONSHIP VALIDATION
# ============================================================

orphan_tenders = (
    silver_order_tender_df.alias("t")
    .join(
        silver_payment_df.alias("p"),
        F.col("t.PaymentID") ==
        F.col("p.SquarePaymentID"),
        "left_anti"
    )
    .count()
)

print("========================================")
print("TENDER → PAYMENT VALIDATION")
print("========================================")

print("Tender rows :", silver_order_tender_df.count())
print("Payment rows:", silver_payment_df.count())
print("Orphan tenders:", orphan_tenders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# TENDER → PAYMENT AMOUNT RECONCILIATION
# ============================================================

tender_payment_check = (
    silver_order_tender_df.alias("t")
    .join(
        silver_payment_df.alias("p"),
        F.col("t.PaymentID") ==
        F.col("p.SquarePaymentID"),
        "inner"
    )
    .select(
        F.col("t.TenderID"),
        F.col("t.SquareOrderID"),
        F.col("t.PaymentID"),
        F.col("t.Amount").alias("TenderAmount"),
        F.col("p.Amount").alias("PaymentAmount")
    )
    .withColumn(
        "Difference",
        F.col("TenderAmount") -
        F.col("PaymentAmount")
    )
)

print("========================================")
print("TENDER → PAYMENT AMOUNT RECONCILIATION")
print("========================================")

total = tender_payment_check.count()

matches = (
    tender_payment_check
    .filter(F.col("Difference") == 0)
    .count()
)

differences = (
    tender_payment_check
    .filter(F.col("Difference") != 0)
    .count()
)

print("Total matched records :", total)
print("Amount matches        :", matches)
print("Amount differences    :", differences)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **silver_customer**

# CELL ********************


from pyspark.sql import functions as F
# ============================================================
# CUSTOMER SILVER TRANSFORMATION
# ============================================================

CUSTOMERS_RAW_PATH = (
    "file:/lakehouse/default/Files/square/customers/"
)

print("========================================")
print("CUSTOMER SILVER TRANSFORMATION")
print("========================================")

print("Customers source:", CUSTOMERS_RAW_PATH)

# ============================================================
# READ RAW CUSTOMERS
# ============================================================

raw_customers_df = (
    spark.read
    .option("multiline", "true")
    .json(CUSTOMERS_RAW_PATH)
)

print("========================================")
print("RAW CUSTOMERS")
print("========================================")

print(
    "Customer array count:",
    raw_customers_df
    .select(F.size("customers"))
    .first()[0]
)

raw_customers_df.printSchema()

# ============================================================
# EXPLODE CUSTOMERS
# ============================================================

customers_df = (
    raw_customers_df
    .select(
        F.explode("customers").alias("customer")
    )
)

print("========================================")
print("EXPLODED CUSTOMERS")
print("========================================")

print(
    "Customer rows:",
    customers_df.count()
)

#customers_df.printSchema()

# ============================================================
# SILVER CUSTOMER
# ============================================================

silver_customer_df = customers_df.select(

    # --------------------------------------------------------
    # CUSTOMER IDENTIFIERS
    # --------------------------------------------------------

    F.col("customer.id")
        .alias("SquareCustomerID"),

    F.col("customer.reference_id")
        .alias("ReferenceID"),

    # --------------------------------------------------------
    # CUSTOMER NAME
    # --------------------------------------------------------

    F.col("customer.given_name")
        .alias("GivenName"),

    F.col("customer.family_name")
        .alias("FamilyName"),

    # --------------------------------------------------------
    # CONTACT INFORMATION
    # --------------------------------------------------------

    F.col("customer.email_address")
        .alias("EmailAddress"),

    F.col("customer.phone_number")
        .alias("PhoneNumber"),

    # --------------------------------------------------------
    # CUSTOMER DATES
    # --------------------------------------------------------

    F.to_timestamp(
        "customer.created_at"
    ).alias("CreatedAt"),

    F.to_timestamp(
        "customer.updated_at"
    ).alias("UpdatedAt"),

    # --------------------------------------------------------
    # CUSTOMER SOURCE
    # --------------------------------------------------------

    F.col("customer.creation_source")
        .alias("CreationSource"),

    # --------------------------------------------------------
    # ADDRESS
    # --------------------------------------------------------

    F.col(
        "customer.address.administrative_district_level_1"
    ).alias("AddressDistrict"),

    F.col(
        "customer.address.locality"
    ).alias("AddressLocality"),

    F.col(
        "customer.address.postal_code"
    ).alias("AddressPostalCode"),

    F.col(
        "customer.address.country"
    ).alias("AddressCountry"),

    # --------------------------------------------------------
    # PREFERENCES
    # --------------------------------------------------------

    F.col(
        "customer.preferences.email_unsubscribed"
    ).alias("EmailUnsubscribed"),

    # --------------------------------------------------------
    # SEGMENTS
    # Keep as array because one customer can have many
    # segments.
    # --------------------------------------------------------

    F.col("customer.segment_ids")[0].alias("SegmentIDs"),

    # --------------------------------------------------------
    # VERSION
    # --------------------------------------------------------

    F.col(
        "customer.version"
    ).cast("long")
    .alias("Version")
)

print("========================================")
print("SILVER CUSTOMER")
print("========================================")

print(
    "Customer rows:",
    silver_customer_df.count()
)

print(
    "Columns:",
    len(silver_customer_df.columns)
)

silver_customer_df.printSchema()

# ============================================================
# CUSTOMER VALIDATION
# ============================================================

print("========================================")
print("CUSTOMER VALIDATION")
print("========================================")

total_customers = (
    silver_customer_df.count()
)

duplicate_customer_ids = (
    silver_customer_df
    .groupBy("SquareCustomerID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_customer_ids = (
    silver_customer_df
    .filter(
        F.col("SquareCustomerID").isNull()
    )
    .count()
)

print("Total customers        :", total_customers)
print("Duplicate Customer IDs :", duplicate_customer_ids)
print("NULL Customer IDs      :", null_customer_ids)

# ============================================================
# SAVE SILVER CUSTOMER
# ============================================================

(
    silver_customer_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.customer")
)

print("========================================")
print("✅ SILVER CUSTOMER SAVED")
print("========================================")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# ORDER → CUSTOMER RELATIONSHIP VALIDATION
# ============================================================

orders_with_customers = (
    silver_order_header_df
    .filter(
        F.col("SquareCustomerID").isNotNull()
    )
)

orphan_orders = (
    orders_with_customers.alias("o")
    .join(
        silver_customer_df.alias("c"),
        F.col("o.SquareCustomerID") ==
        F.col("c.SquareCustomerID"),
        "left_anti"
    )
    .count()
)

print("========================================")
print("ORDER → CUSTOMER VALIDATION")
print("========================================")

print(
    "Orders with CustomerID :",
    orders_with_customers.count()
)

print(
    "Orders without CustomerID:",
    silver_order_header_df
    .filter(F.col("SquareCustomerID").isNull())
    .count()
)

print(
    "Orphan orders           :",
    orphan_orders
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **silver_catalog**

# CELL ********************

# ============================================================
# CATALOG SILVER TRANSFORMATION
# ============================================================

CATALOG_RAW_PATH = (
    "file:/lakehouse/default/Files/square/catalog/"
)

print("========================================")
print("CATALOG SILVER TRANSFORMATION")
print("========================================")

print("Catalog source:", CATALOG_RAW_PATH)
# ============================================================
# READ RAW CATALOG
# ============================================================

raw_catalog_df = (
    spark.read
    .option("multiline", "true")
    .json(CATALOG_RAW_PATH)
)

print("========================================")
print("RAW CATALOG")
print("========================================")

raw_catalog_df.printSchema()

# ============================================================
# EXPLODE CATALOG OBJECTS
# ============================================================

catalog_objects_df = (
    raw_catalog_df
    .select(
        F.explode("objects").alias("catalog_object")
    )
)

print("========================================")
print("CATALOG OBJECTS")
print("========================================")

print(
    "Total catalog objects:",
    catalog_objects_df.count()
)

catalog_objects_df.groupBy(
    "catalog_object.type"
).count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# SILVER CATALOG ITEM
# ============================================================

silver_catalog_item_df = (
    catalog_objects_df
    .filter(
        F.col("catalog_object.type") == "ITEM"
    )
    .select(

        # ----------------------------------------------------
        # ITEM IDENTIFIER
        # ----------------------------------------------------

        F.col(
            "catalog_object.id"
        ).alias("SquareItemID"),

        # ----------------------------------------------------
        # ITEM INFORMATION
        # ----------------------------------------------------

        F.col(
            "catalog_object.item_data.name"
        ).alias("ItemName"),

        F.col(
            "catalog_object.item_data.description"
        ).alias("Description"),

        F.col(
            "catalog_object.item_data.product_type"
        ).alias("ProductType"),

        # ----------------------------------------------------
        # ITEM FLAGS
        # ----------------------------------------------------

        F.col(
            "catalog_object.item_data.is_archived"
        ).alias("IsArchived"),

        F.col(
            "catalog_object.item_data.is_taxable"
        ).alias("IsTaxable"),

        F.col(
            "catalog_object.is_deleted"
        ).alias("IsDeleted"),

        # ----------------------------------------------------
        # DATES
        # ----------------------------------------------------

        F.to_timestamp(
            "catalog_object.created_at"
        ).alias("CreatedAt"),

        F.to_timestamp(
            "catalog_object.updated_at"
        ).alias("UpdatedAt"),

        # ----------------------------------------------------
        # VERSION
        # ----------------------------------------------------

        F.col(
            "catalog_object.version"
        ).cast("long").alias("Version")
    )
)

print("========================================")
print("SILVER CATALOG ITEM")
print("========================================")

print(
    "Catalog item rows:",
    silver_catalog_item_df.count()
)

silver_catalog_item_df.printSchema()

# ============================================================
# CATALOG ITEM VALIDATION
# ============================================================

print("========================================")
print("CATALOG ITEM VALIDATION")
print("========================================")

total_items = (
    silver_catalog_item_df.count()
)

duplicate_items = (
    silver_catalog_item_df
    .groupBy("SquareItemID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_item_ids = (
    silver_catalog_item_df
    .filter(
        F.col("SquareItemID").isNull()
    )
    .count()
)

null_item_names = (
    silver_catalog_item_df
    .filter(
        F.col("ItemName").isNull()
    )
    .count()
)

print("Total catalog items :", total_items)
print("Duplicate Item IDs  :", duplicate_items)
print("NULL Item IDs       :", null_item_ids)
print("NULL Item Names     :", null_item_names)

# ============================================================
# SILVER CATALOG VARIATION
# ============================================================

silver_catalog_variation_df = (
    catalog_objects_df
    .filter(
        F.col("catalog_object.type") ==
        "ITEM_VARIATION"
    )
    .select(

        # ----------------------------------------------------
        # VARIATION IDENTIFIER
        # ----------------------------------------------------

        F.col(
            "catalog_object.id"
        ).alias("SquareVariationID"),

        # ----------------------------------------------------
        # PARENT ITEM
        # ----------------------------------------------------

        F.col(
            "catalog_object.item_variation_data.item_id"
        ).alias("SquareItemID"),

        # ----------------------------------------------------
        # VARIATION INFORMATION
        # ----------------------------------------------------

        F.col(
            "catalog_object.item_variation_data.name"
        ).alias("VariationName"),

        F.col(
            "catalog_object.item_variation_data.sku"
        ).alias("SKU"),

        F.col(
            "catalog_object.item_variation_data.ordinal"
        ).cast("long").alias("Ordinal"),

        # ----------------------------------------------------
        # PRICE
        # ----------------------------------------------------

        F.col(
            "catalog_object.item_variation_data.price_money.amount"
        ).cast("long").alias("PriceAmount"),

        F.col(
            "catalog_object.item_variation_data.price_money.currency"
        ).alias("Currency"),

        # ----------------------------------------------------
        # PRICING
        # ----------------------------------------------------

        F.col(
            "catalog_object.item_variation_data.pricing_type"
        ).alias("PricingType"),

        # ----------------------------------------------------
        # FLAGS
        # ----------------------------------------------------

        F.col(
            "catalog_object.item_variation_data.sellable"
        ).alias("Sellable"),

        F.col(
            "catalog_object.item_variation_data.stockable"
        ).alias("Stockable"),

        F.col(
            "catalog_object.is_deleted"
        ).alias("IsDeleted"),

        F.col(
            "catalog_object.present_at_all_locations"
        ).alias("PresentAtAllLocations"),

        # ----------------------------------------------------
        # DATES
        # ----------------------------------------------------

        F.to_timestamp(
            "catalog_object.created_at"
        ).alias("CreatedAt"),

        F.to_timestamp(
            "catalog_object.updated_at"
        ).alias("UpdatedAt"),

        # ----------------------------------------------------
        # VERSION
        # ----------------------------------------------------

        F.col(
            "catalog_object.version"
        ).cast("long").alias("Version")
    )
)

print("========================================")
print("SILVER CATALOG VARIATION")
print("========================================")

print(
    "Catalog variation rows:",
    silver_catalog_variation_df.count()
)

silver_catalog_variation_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CATALOG VARIATION VALIDATION
# ============================================================

print("========================================")
print("CATALOG VARIATION VALIDATION")
print("========================================")

total_variations = (
    silver_catalog_variation_df.count()
)

duplicate_variations = (
    silver_catalog_variation_df
    .groupBy("SquareVariationID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_variation_ids = (
    silver_catalog_variation_df
    .filter(
        F.col("SquareVariationID").isNull()
    )
    .count()
)

null_item_ids = (
    silver_catalog_variation_df
    .filter(
        F.col("SquareItemID").isNull()
    )
    .count()
)

null_skus = (
    silver_catalog_variation_df
    .filter(
        F.col("SKU").isNull()
    )
    .count()
)

print("Total variations       :", total_variations)
print("Duplicate Variation IDs:", duplicate_variations)
print("NULL Variation IDs     :", null_variation_ids)
print("NULL Item IDs          :", null_item_ids)
print("NULL SKUs              :", null_skus)

# ============================================================
# CATALOG VARIATION → ITEM VALIDATION
# ============================================================

orphan_variations = (
    silver_catalog_variation_df.alias("v")
    .join(
        silver_catalog_item_df.alias("i"),
        F.col("v.SquareItemID") ==
        F.col("i.SquareItemID"),
        "left_anti"
    )
    .count()
)

print("========================================")
print("CATALOG VARIATION → ITEM VALIDATION")
print("========================================")

print(
    "Catalog variations :",
    silver_catalog_variation_df.count()
)

print(
    "Catalog items      :",
    silver_catalog_item_df.count()
)

print(
    "Orphan variations  :",
    orphan_variations
)

# ============================================================
# ORDER LINE → CATALOG VARIATION VALIDATION
# ============================================================

orphan_order_lines = (
    silver_order_line_df.alias("ol")
    .join(
        silver_catalog_variation_df.alias("v"),
        F.col("ol.SquareVariationID") ==
        F.col("v.SquareVariationID"),
        "left_anti"
    )
    .count()
)

print("========================================")
print("ORDER LINE → CATALOG VALIDATION")
print("========================================")

print(
    "Order lines       :",
    silver_order_line_df.count()
)

print(
    "Orphan order lines:",
    orphan_order_lines
)

# ============================================================
# SAVE SILVER CATALOG ITEM
# ============================================================

(
    silver_catalog_item_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.catalog_item")
)

print("✅ silver_catalog_item saved")

# ============================================================
# SAVE SILVER CATALOG VARIATION
# ============================================================

(
    silver_catalog_variation_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.catalog_variation")
)

print("✅ silver_catalog_variation saved")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "silver_catalog_item:",
    spark.table("silver.catalog_item").count()
)

print(
    "silver_catalog_variation:",
    spark.table("silver.catalog_variation").count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **silver_inventory**

# CELL ********************

# ============================================================
# INVENTORY SILVER TRANSFORMATION
# ============================================================

INVENTORY_RAW_PATH = (
    "file:/lakehouse/default/Files/square/inventory/"
)

print("========================================")
print("INVENTORY SILVER TRANSFORMATION")
print("========================================")

print("Inventory source:", INVENTORY_RAW_PATH)

raw_inventory_df = (
    spark.read
    .option("multiline", "true")
    .json(INVENTORY_RAW_PATH)
)

print("========================================")
print("RAW INVENTORY")
print("========================================")

raw_inventory_df.printSchema()

# ============================================================
# EXPLODE INVENTORY COUNTS
# ============================================================

inventory_counts_df = (
    raw_inventory_df
    .select(
        F.explode("counts").alias("inventory")
    )
)

print("========================================")
print("EXPLODED INVENTORY")
print("========================================")

print(
    "Inventory rows:",
    inventory_counts_df.count()
)

inventory_counts_df.printSchema()


# ============================================================
# SILVER INVENTORY
# ============================================================

silver_inventory_df = (
    inventory_counts_df
    .select(

        # ----------------------------------------------------
        # CATALOG VARIATION
        # ----------------------------------------------------

        F.col(
            "inventory.catalog_object_id"
        ).alias("SquareVariationID"),

        F.col(
            "inventory.catalog_object_type"
        ).alias("CatalogObjectType"),

        # ----------------------------------------------------
        # INVENTORY STATE
        # ----------------------------------------------------

        F.col(
            "inventory.state"
        ).alias("InventoryState"),

        # ----------------------------------------------------
        # LOCATION
        # ----------------------------------------------------

        F.col(
            "inventory.location_id"
        ).alias("LocationID"),

        # ----------------------------------------------------
        # QUANTITY
        # ----------------------------------------------------

        F.col(
            "inventory.quantity"
        ).cast("long").alias("Quantity"),

        # ----------------------------------------------------
        # CALCULATED TIME
        # ----------------------------------------------------

        F.to_timestamp(
            "inventory.calculated_at"
        ).alias("CalculatedAt")
    )
)

print("========================================")
print("SILVER INVENTORY")
print("========================================")

print(
    "Inventory rows:",
    silver_inventory_df.count()
)

silver_inventory_df.printSchema()

# ============================================================
# INVENTORY VALIDATION
# ============================================================

print("========================================")
print("INVENTORY VALIDATION")
print("========================================")

total_inventory = (
    silver_inventory_df.count()
)

duplicate_variations = (
    silver_inventory_df
    .groupBy(
        "SquareVariationID",
        "LocationID"
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_variation_ids = (
    silver_inventory_df
    .filter(
        F.col("SquareVariationID").isNull()
    )
    .count()
)

null_locations = (
    silver_inventory_df
    .filter(
        F.col("LocationID").isNull()
    )
    .count()
)

null_quantities = (
    silver_inventory_df
    .filter(
        F.col("Quantity").isNull()
    )
    .count()
)

null_states = (
    silver_inventory_df
    .filter(
        F.col("InventoryState").isNull()
    )
    .count()
)

print("Total inventory rows      :", total_inventory)
print("Duplicate Variation/Loc  :", duplicate_variations)
print("NULL Variation IDs       :", null_variation_ids)
print("NULL Locations           :", null_locations)
print("NULL Quantities          :", null_quantities)
print("NULL Inventory States    :", null_states)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# INVENTORY → CATALOG VARIATION VALIDATION
# ============================================================

orphan_inventory = (
    silver_inventory_df.alias("inv")
    .join(
        silver_catalog_variation_df.alias("v"),
        F.col("inv.SquareVariationID") ==
        F.col("v.SquareVariationID"),
        "left_anti"
    )
    .count()
)

print("========================================")
print("INVENTORY → CATALOG VALIDATION")
print("========================================")

print(
    "Inventory rows      :",
    silver_inventory_df.count()
)

print(
    "Catalog variations  :",
    silver_catalog_variation_df.count()
)

print(
    "Orphan inventory    :",
    orphan_inventory
)

# ============================================================
# SAVE SILVER INVENTORY
# ============================================================

(
    silver_inventory_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.inventory")
)

print("========================================")
print("✅ SILVER INVENTORY SAVED")
print("========================================")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "silver_inventory count:",
    spark.table("silver.inventory").count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ****Employes****

# CELL ********************

# ============================================================
# EMPLOYEE SILVER TRANSFORMATION
# ============================================================

EMPLOYEE_RAW_PATH = (
    "file:/lakehouse/default/Files/square/employees/"
)

print("========================================")
print("EMPLOYEE SILVER TRANSFORMATION")
print("========================================")

print("Employee source:", EMPLOYEE_RAW_PATH)

raw_employee_df = (
    spark.read
    .option("multiline", "true")
    .json(EMPLOYEE_RAW_PATH)
)

print("========================================")
print("RAW EMPLOYEES")
print("========================================")

raw_employee_df.printSchema()

# ============================================================
# EXPLODE EMPLOYEES
# ============================================================

employee_df = (
    raw_employee_df
    .select(
        F.explode("employees").alias("employee")
    )
)

print("========================================")
print("EXPLODED EMPLOYEES")
print("========================================")

print(
    "Employee rows:",
    employee_df.count()
)

employee_df.printSchema()

# ============================================================
# SILVER EMPLOYEE
# ============================================================

silver_employee_df = (
    employee_df
    .select(

        # ----------------------------------------------------
        # EMPLOYEE IDENTIFIER
        # ----------------------------------------------------

        F.col(
            "employee.id"
        ).alias("SquareEmployeeID"),

        # ----------------------------------------------------
        # NAME
        # ----------------------------------------------------

        F.col(
            "employee.first_name"
        ).alias("FirstName"),

        F.col(
            "employee.last_name"
        ).alias("LastName"),

        # ----------------------------------------------------
        # CONTACT
        # ----------------------------------------------------

        F.col(
            "employee.email"
        ).alias("Email"),

        F.col(
            "employee.phone_number"
        ).alias("PhoneNumber"),

        # ----------------------------------------------------
        # LOCATION
        # Keep as array — one employee can have
        # multiple locations.
        # ----------------------------------------------------

        F.col("employee.location_ids")[0].alias("LocationIDs"),

        # ----------------------------------------------------
        # STATUS
        # ----------------------------------------------------

        F.col(
            "employee.status"
        ).alias("EmployeeStatus"),

        # ----------------------------------------------------
        # DATES
        # ----------------------------------------------------

        F.to_timestamp(
            "employee.created_at"
        ).alias("CreatedAt"),

        F.to_timestamp(
            "employee.updated_at"
        ).alias("UpdatedAt")
    )
)

print("========================================")
print("SILVER EMPLOYEE")
print("========================================")

print(
    "Employee rows:",
    silver_employee_df.count()
)

print(
    "Columns:",
    len(silver_employee_df.columns)
)

silver_employee_df.printSchema()

# ============================================================
# EMPLOYEE VALIDATION
# ============================================================

print("========================================")
print("EMPLOYEE VALIDATION")
print("========================================")

total_employees = (
    silver_employee_df.count()
)

duplicate_employee_ids = (
    silver_employee_df
    .groupBy("SquareEmployeeID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_employee_ids = (
    silver_employee_df
    .filter(
        F.col("SquareEmployeeID").isNull()
    )
    .count()
)

null_status = (
    silver_employee_df
    .filter(
        F.col("EmployeeStatus").isNull()
    )
    .count()
)

print("Total employees        :", total_employees)
print("Duplicate Employee IDs :", duplicate_employee_ids)
print("NULL Employee IDs      :", null_employee_ids)
print("NULL Employee Status   :", null_status)


# ============================================================
# SAVE SILVER EMPLOYEE
# ============================================================

(
    silver_employee_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.employee")
)

print("========================================")
print("✅ SILVER EMPLOYEE SAVED")
print("========================================")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "silver_employee count:",
    spark.table("silver.employee").count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **silver_order_date_mapping**

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================================
# ORDER DATE MAPPING
# ============================================================

print("========================================")
print("ORDER DATE MAPPING")
print("========================================")

orders_df = spark.table("silver_order_header")

print("Orders available:", orders_df.count())

# ------------------------------------------------------------
# Keep only the columns required for the mapping
# ------------------------------------------------------------

order_ids_df = (
    orders_df
    .select(
        "BusinessOrderID",
        "SquareOrderID"
    )
    .dropDuplicates(["BusinessOrderID"])
)

# ------------------------------------------------------------
# Create a deterministic reporting date
#
# POC reporting period:
# 2026-07-01 → 2026-08-19
#
# 50-day reporting window
# ------------------------------------------------------------

start_date = "2026-07-01"

order_date_mapping_df = (
    order_ids_df
    .withColumn(
        "_order_number",
        F.regexp_extract(
            F.col("BusinessOrderID"),
            r"(\d+)$",
            1
        ).cast("int")
    )
    .withColumn(
        "ReportingOrderDate",
        F.date_add(
            F.to_date(F.lit(start_date)),
            F.pmod(
                F.col("_order_number") - 1,
                F.lit(50)
            )
        )
    )
    .select(
        "BusinessOrderID",
        "SquareOrderID",
        "ReportingOrderDate"
    )
)

print("========================================")
print("MAPPING CREATED")
print("========================================")

print(
    "Mapping rows:",
    order_date_mapping_df.count()
)

print(
    "Distinct reporting dates:",
    order_date_mapping_df
    .select("ReportingOrderDate")
    .distinct()
    .count()
)

order_date_mapping_df.orderBy(
    "ReportingOrderDate",
    "BusinessOrderID"
).show(20, truncate=False)

# ============================================================
# ORDER DATE MAPPING VALIDATION
# ============================================================

print("========================================")
print("ORDER DATE MAPPING VALIDATION")
print("========================================")

total_orders = orders_df.count()

total_mappings = (
    order_date_mapping_df.count()
)

duplicate_business_ids = (
    order_date_mapping_df
    .groupBy("BusinessOrderID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_business_ids = (
    order_date_mapping_df
    .filter(
        F.col("BusinessOrderID").isNull()
    )
    .count()
)

null_reporting_dates = (
    order_date_mapping_df
    .filter(
        F.col("ReportingOrderDate").isNull()
    )
    .count()
)

print("Total orders           :", total_orders)
print("Total mappings         :", total_mappings)
print("Duplicate Business IDs :", duplicate_business_ids)
print("NULL Business IDs      :", null_business_ids)
print("NULL Reporting Dates   :", null_reporting_dates)

# ============================================================
# ORDER → REPORTING DATE VALIDATION
# ============================================================

unmapped_orders = (
    orders_df
    .select("BusinessOrderID")
    .join(
        order_date_mapping_df.select(
            "BusinessOrderID"
        ),
        on="BusinessOrderID",
        how="left_anti"
    )
    .count()
)

print("========================================")
print("ORDER → REPORTING DATE VALIDATION")
print("========================================")

print("Orders in Silver :", total_orders)
print("Unmapped orders  :", unmapped_orders)

# ============================================================
# REPORTING DATE DISTRIBUTION
# ============================================================

print("========================================")
print("REPORTING DATE DISTRIBUTION")
print("========================================")

(
    order_date_mapping_df
    .groupBy("ReportingOrderDate")
    .count()
    .orderBy("ReportingOrderDate")
    .show(100, truncate=False)
)

# ============================================================
# SAVE ORDER DATE MAPPING
# ============================================================

(
    order_date_mapping_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.order_date_mapping")
)

print("========================================")
print("✅ ORDER DATE MAPPING SAVED")
print("========================================")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "order_date_mapping rows:",
    spark.table("order_date_mapping").count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **silver_location**

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("========================================")
print("LOCATION SILVER TRANSFORMATION")
print("========================================")

# ============================================================
# SOURCE / TARGET
# ============================================================

locations_path = "file:/lakehouse/default/Files/square/location/"

print("Location source:", locations_path)
print("Silver target  : silver.location")

# ============================================================
# READ RAW LOCATION JSON
# ============================================================

raw_locations = (
    spark.read
    .option("multiLine", "true")
    .json(locations_path)
)

print()
print("========================================")
print("RAW LOCATIONS")
print("========================================")

raw_locations.printSchema()

# ============================================================
# EXPLODE LOCATIONS ARRAY
# ============================================================

locations_df = (
    raw_locations
    .select(
        "ingested_at",
        F.explode_outer("locations").alias("location")
    )
)

print()
print("========================================")
print("EXPLODED LOCATIONS")
print("========================================")

print("Location rows:", locations_df.count())

# ============================================================
# FLATTEN LOCATION (safe against missing optional fields)
# ============================================================

def safe_col(df, path, alias, cast_type=None):
    """Return the column if it exists in the schema, otherwise a NULL column."""
    try:
        df.select(path)
        col = F.col(path)
        if cast_type:
            col = col.cast(cast_type)
        return col.alias(alias)
    except Exception:
        return F.lit(None).cast(cast_type or "string").alias(alias)

silver_location = (
    locations_df
    .select(
        safe_col(locations_df, "location.id", "SquareLocationID"),
        safe_col(locations_df, "location.name", "LocationName"),
        safe_col(locations_df, "location.business_name", "BusinessName"),
        safe_col(locations_df, "location.status", "LocationStatus"),
        safe_col(locations_df, "location.type", "LocationType"),
        safe_col(locations_df, "location.address.address_line_1", "AddressLine1"),
        safe_col(locations_df, "location.address.address_line_2", "AddressLine2"),
        safe_col(locations_df, "location.address.locality", "City"),
        safe_col(locations_df, "location.address.administrative_district_level_1", "State"),
        safe_col(locations_df, "location.address.postal_code", "PostalCode"),
        safe_col(locations_df, "location.address.country", "Country"),
        safe_col(locations_df, "location.timezone", "Timezone"),
        safe_col(locations_df, "location.currency", "Currency"),
        safe_col(locations_df, "location.language_code", "LanguageCode"),
        safe_col(locations_df, "location.created_at", "CreatedAt", "timestamp"),
        safe_col(locations_df, "location.updated_at", "UpdatedAt", "timestamp"),
        safe_col(locations_df, "ingested_at", "IngestedAt", "timestamp")
    )
)

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
print("========================================")
print("SILVER LOCATION")
print("========================================")

print("Location rows:", silver_location.count())
print("Columns:", len(silver_location.columns))

silver_location.printSchema()

# ============================================================
# DEDUPLICATE LOCATIONS
# ============================================================

dedup_order_col = "IngestedAt" if "IngestedAt" in silver_location.columns else "UpdatedAt"

window_spec = (
    Window
    .partitionBy("SquareLocationID")
    .orderBy(F.col(dedup_order_col).desc())
)

silver_location = (
    silver_location
    .withColumn("_rn", F.row_number().over(window_spec))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# ============================================================
# VALIDATION
# ============================================================

print()
print("========================================")
print("LOCATION VALIDATION")
print("========================================")

total_locations = silver_location.count()

duplicate_location_ids = (
    silver_location
    .groupBy("SquareLocationID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_location_ids = (
    silver_location
    .filter(F.col("SquareLocationID").isNull())
    .count()
)

null_location_names = (
    silver_location
    .filter(F.col("LocationName").isNull())
    .count()
)

print("Total locations        :", total_locations)
print("Duplicate Location IDs :", duplicate_location_ids)
print("NULL Location IDs      :", null_location_ids)
print("NULL Location Names    :", null_location_names)

# ============================================================
# SAVE TO SILVER SCHEMA
# ============================================================

(
    silver_location
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.location")
)

print()
print("========================================")
print("✅ SILVER LOCATION SAVED")
print("========================================")

print("Table: silver.location")
print("Rows :", silver_location.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
