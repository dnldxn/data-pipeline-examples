/*
Table to store raw data straight from the CSV file.  All fields are stored as strings.
*/
CREATE OR REPLACE TABLE purchases_raw (
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
    gift_recipient_contact_details VARCHAR,
    load_dt DATE DEFAULT 
)
;

/*
Address table lookup table
*/
CREATE OR REPLACE TABLE address (
  address_id number autoincrement start 1 increment 1,  -- primary key
  addr_raw varchar,
  name varchar,
  address varchar,
  phone varchar
);

/*
Order status lookup table
*/
CREATE OR REPLACE TABLE order_status (
  order_status_id number autoincrement start 1 increment 1,  -- primary key
  order_status varchar,
);

/*
Main purchase fact table
*/
CREATE OR REPLACE TABLE purchase (
    website VARCHAR,
    order_id VARCHAR,
    order_dttm TIMESTAMP_TZ(9),
    currency VARCHAR,
    unit_price NUMBER(8,2),
    unit_price_tax NUMBER(8,2),
    shipping_charge NUMBER(8,2),
    total_discounts VARCHAR,
    TO_DECIMAL(total_owed, '999,999.99', 8, 2) AS total_owed,
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
    shipping_address_id NUMBER(38,0),  -- foreign key to address table
    billing_address_id NUMBER(38,0),   -- foreign key to address table
    carrier_nm_and_tracking_num VARCHAR,
    product_nm VARCHAR,
    gift_message VARCHAR,
    gift_sender_nm VARCHAR,
    gift_recipient_contact_details VARCHAR,
    load_dt DATE DEFAULT 
)
;