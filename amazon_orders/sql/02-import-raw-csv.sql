-- 01.  Copy the data as raw strings into the staging table: purchases_raw
COPY INTO purchases_raw
  FROM @purchases_stage/Retail.OrderHistory.1.csv
  ON_ERROR = 'skip_file'

  -- Test first using the following option
  --VALIDATION_MODE = RETURN_10_ROWS
;

-- 02.  Transform and import the data into the data model
ALTER SESSION SET TIMEZONE = 'UTC';

MERGE INTO address addr_old
USING (
  SELECT DISTINCT
      addr_raw,
      REGEXP_SUBSTR(addr_raw, '([^0-9]+)', 1, 1, '', 1) AS name,
      REGEXP_SUBSTR(REGEXP_REPLACE(addr_raw, ' Primary Phone.*', ''), '\\d{3,}.+') AS address,
      REGEXP_REPLACE(REGEXP_SUBSTR(addr_raw, 'Primary Phone: (.*)', 1, 1, '', 1), '[^0-9]', '') AS phone
  FROM (
      SELECT DISTINCT LOWER(shipping_addr) AS addr_raw FROM purchases_raw WHERE TRIM(shipping_addr) <> ''
      UNION
      SELECT DISTINCT LOWER(billing_addr) AS addr_raw FROM purchases_raw WHERE TRIM(billing_addr) <> ''
  )
) addr_new
ON LOWER(addr_old.addr_raw) = LOWER(addr_new.addr_raw)
WHEN NOT MATCHED THEN 
    INSERT (addr_raw, name, address, phone) VALUES (
      addr_new.addr_raw,
      addr_new.name,
      addr_new.address,
      addr_new.phone
    )
;



INSERT INTO purchase
VALUES(
  SELECT
      website,
      order_id,
      TO_TIMESTAMP_TZ(REPLACE(order_dt, ' UTC', ''), 'mm/dd/yyyy hh24:mi:ss') AS order_dttm,
      -- purchase_order_num,   -- Column is always empty!
      currency,
      TO_DECIMAL(unit_price, '999,999.99', 8, 2) AS unit_price,
      TO_DECIMAL(unit_price_tax, '999,999.99', 8, 2) AS unit_price_tax,
      TO_DECIMAL(shipping_charge, '999,999.99', 8, 2) AS shipping_charge,
      total_discounts,
      TO_DECIMAL(total_owed, '999,999.99', 8, 2) AS total_owed,
      shipment_item_subtotal,
      shipment_item_subtotal_tax,
      asin,
      product_condition,
      quantity,
      payment_instrument_type,
      order_status,
      shipment_status,
      ship_dt,
      shipping_option,
      addr_shipping.address_id AS shipping_address_id,
      addr_billing.address_id AS billing_address_id,
      carrier_nm_and_tracking_num,
      product_nm,
      gift_message,
      gift_sender_nm,
      gift_recipient_contact_details
  FROM purchases_raw
  LEFT OUTER JOIN address addr_shipping
    ON LOWER(purchases_raw.shipping_addr) = addr_shipping.addr_raw
  LEFT OUTER JOIN address addr_billing
    ON LOWER(purchases_raw.billing_addr) = addr_billing.addr_raw
)
;