CREATE TABLE IF NOT EXISTS "healthcare"."patients"(

    uuid UUID PRIMARY KEY,
    birth_date DATE NOT NULL,
    death_date DATE,
    social_security_number VARCHAR(16) NOT NULL,
    driver_license_number VARCHAR(16),
    passport_number VARCHAR(9),
    salutation VARCHAR(8),
    first_name VARCHAR(64) NOT NULL,
    middle_name VARCHAR(64),
    last_name VARCHAR(64),
    doctorate VARCHAR(32) DEFAULT 'No Doctorate',
    marital_status VARCHAR(16) DEFAULT 'Unknown',
    race VARCHAR(20),
    ethnicity VARCHAR(16),
    gender CHAR(1),
    patient_birthplace VARCHAR(255),
    current_address TEXT,
    geolocated_city VARCHAR(128),
    geolocated_state VARCHAR(128),
    geolocated_county VARCHAR(128),
    postal_code VARCHAR(5),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    family_income bigint DEFAULT 0 check (family_income >= 0),
    created_at TIMESTAMPTZ DEFAULT NOW() 
);

