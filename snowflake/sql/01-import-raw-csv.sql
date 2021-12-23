CREATE OR REPLACE TABLE purchases (
    website VARCHAR,
    order_id VARCHAR,
    order_dt VARCHAR,
    purchase_order_num VARCHAR,
    currency VARCHAR,
    unit_price VARCHAR,
    unit_price_tax VARCHAR,
    shipping_charge VARCHAR,
    total_discounts VARCHAR,
    total_owed VARCHAR,
    shipment_item_subtotal VARCHAR,
    shipment_item_subtotal_tax VARCHAR,
    asin VARCHAR,
    product_condition VARCHAR,
    quantity VARCHAR,
    payment_instrument_type VARCHAR,
    order_status VARCHAR,
    shipment_status VARCHAR,
    ship_dt VARCHAR,
    shipping_option VARCHAR,
    shipping_addr VARCHAR,
    billing_addr VARCHAR,
    carrier_nm_and_tracking_num VARCHAR,
    product_nm VARCHAR,
    gift_message VARCHAR,
    gift_sender_nm VARCHAR,
    gift_recipient_contact_details VARCHAR
)
;

COPY INTO purchases
  FROM @purchases_stage/Retail.OrderHistory.1.csv
  ON_ERROR = 'skip_file'

  -- Test first using the following option
  --VALIDATION_MODE = RETURN_10_ROWS
;
