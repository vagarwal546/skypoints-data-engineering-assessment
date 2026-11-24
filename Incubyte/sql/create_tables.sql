-- ============================================================================
-- SkyPoints Loyalty Program - Unity Catalog & Delta Lake DDL
-- ============================================================================

CREATE CATALOG IF NOT EXISTS skypoints_dev;

CREATE SCHEMA IF NOT EXISTS skypoints_dev.bronze;
CREATE SCHEMA IF NOT EXISTS skypoints_dev.silver;
CREATE SCHEMA IF NOT EXISTS skypoints_dev.gold;

-- ----------------------------------------------------------------------------
-- BRONZE LAYER (Raw / Landing)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS skypoints_dev.bronze.member_profiles_raw (
    member_id           STRING,
    member_name         STRING,
    dob                 STRING,
    enrollment_date     STRING,
    flight_date         STRING,
    tier_code           STRING,
    member_type         STRING,
    agent_name          STRING,
    state               STRING,
    country             STRING,
    is_active           STRING,
    source_file_name    STRING,
    source_country      STRING,
    batch_id            STRING,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
CLUSTER BY (source_country, batch_id)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS skypoints_dev.bronze.redemption_raw (
    raw_payload         STRING,
    source_file_name    STRING,
    batch_id            STRING,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
CLUSTER BY (batch_id);

-- ----------------------------------------------------------------------------
-- SILVER LAYER (Cleansed, Standardized & Quarantined)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS skypoints_dev.silver.member_profiles (
    member_id           STRING      NOT NULL,
    member_name         STRING      NOT NULL,
    dob                 DATE,
    enrollment_date     DATE        NOT NULL,
    flight_date         DATE,
    tier_code           STRING,
    member_type         STRING,
    agent_name          STRING,
    state               STRING,
    country             STRING      NOT NULL,
    is_active           STRING,
    age                 INT,
    stale_member        INT,
    dq_warnings         ARRAY<STRING>,
    source_file_name    STRING,
    batch_id            STRING,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
CLUSTER BY (country, member_id)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS skypoints_dev.silver.rejected_members (
    member_id           STRING,
    member_name         STRING,
    dob                 STRING,
    enrollment_date     STRING,
    flight_date         STRING,
    tier_code           STRING,
    country             STRING,
    rejection_reasons   ARRAY<STRING>,
    source_file_name    STRING,
    batch_id            STRING,
    rejected_at         TIMESTAMP
)
USING DELTA
CLUSTER BY (batch_id);

CREATE TABLE IF NOT EXISTS skypoints_dev.silver.redemption_transactions (
    transaction_id      STRING      NOT NULL,
    member_id           STRING      NOT NULL,
    feed_date           DATE,
    transaction_date    DATE,
    partner             STRING,
    miles_redeemed      BIGINT,
    status              STRING,
    batch_id            STRING,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
CLUSTER BY (transaction_date, member_id);

-- ----------------------------------------------------------------------------
-- GOLD LAYER (Master, Country-Specific Targets & Analytics)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS skypoints_dev.gold.member_master (
    member_id           STRING      NOT NULL,
    member_name         STRING      NOT NULL,
    dob                 DATE,
    enrollment_date     DATE        NOT NULL,
    flight_date         DATE,
    tier_code           STRING,
    member_type         STRING,
    agent_name          STRING,
    state               STRING,
    country             STRING      NOT NULL,
    is_active           STRING,
    age                 INT,
    stale_member        INT,
    last_updated_at     TIMESTAMP
)
USING DELTA
CLUSTER BY (country, member_id);

CREATE TABLE IF NOT EXISTS skypoints_dev.gold.member_usa        LIKE skypoints_dev.gold.member_master;
CREATE TABLE IF NOT EXISTS skypoints_dev.gold.member_india      LIKE skypoints_dev.gold.member_master;
CREATE TABLE IF NOT EXISTS skypoints_dev.gold.member_australia  LIKE skypoints_dev.gold.member_master;
CREATE TABLE IF NOT EXISTS skypoints_dev.gold.member_philippines LIKE skypoints_dev.gold.member_master;
CREATE TABLE IF NOT EXISTS skypoints_dev.gold.member_canada     LIKE skypoints_dev.gold.member_master;

CREATE TABLE IF NOT EXISTS skypoints_dev.gold.member_redemption_analytics (
    member_id           STRING,
    member_name         STRING,
    country             STRING,
    tier_code           STRING,
    age                 INT,
    stale_member        INT,
    total_redemptions   BIGINT,
    total_miles_redeemed BIGINT,
    completed_miles     BIGINT,
    pending_miles       BIGINT,
    last_redemption_date DATE,
    refreshed_at        TIMESTAMP
)
USING DELTA
CLUSTER BY (country, tier_code);