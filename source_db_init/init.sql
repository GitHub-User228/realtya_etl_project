CREATE TABLE addresses (
    address_id SERIAL PRIMARY KEY,
    address_info VARCHAR(100),
    latitude DECIMAL(8, 5),
    longitude DECIMAL(8, 5)
);

CREATE TABLE realty (
    offer_id BIGINT,
    date_parsed DATE,
    flat_type VARCHAR(50),
    main_info VARCHAR(50) ARRAY[6],
    fee_info VARCHAR(50) ARRAY[4],
    address_info VARCHAR(100),
    extra_features VARCHAR(100) ARRAY[35],
    PRIMARY KEY (offer_id, date_parsed)
);