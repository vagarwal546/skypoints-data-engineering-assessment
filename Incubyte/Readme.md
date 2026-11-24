# SkyPoints Global Loyalty Program — Data Engineering Assessment

A production-ready Databricks Medallion Architecture (Bronze, Silver, Gold) built with **PySpark, Spark SQL, Delta Lake, Unity Catalog, and Databricks Asset Bundles (DABs)** to process member profile feeds and partner airline JSON redemption transactions at scale.

---

## Architecture Overview

```text
Source Files (USA.csv, Australia.csv, India.csv, Redemption.json)
        │
        ▼
┌─────────────────────────────────────────┐
│ BRONZE (Raw / Landing)                  │  Immutable raw strings + audit metadata
│ - bronze.member_profiles_raw            │  (source_file_name, batch_id, timestamp)
│ - bronze.redemption_raw                 │
└───────────────────┬─────────────────────┘
                    ▼
┌─────────────────────────────────────────┐
│ SILVER (Cleansed, Enriched & Validated) │  Multi-format date parsing, country normalization,
│ - silver.member_profiles                │  Age & Stale_Member (>90d), DQ Quarantine
│ - silver.rejected_members (Quarantine)  │  Flattened & idempotent JSON redemptions
│ - silver.redemption_transactions        │
└───────────────────┬─────────────────────┘
                    ▼
┌─────────────────────────────────────────┐
│ GOLD (Master, Country Tables & BI)      │  Latest-record-wins deduplication,
│ - gold.member_master                    │  country-movement delete + upsert sync,
│ - gold.member_{usa,india,australia,...} │  joined member-redemption analytics
│ - gold.member_redemption_analytics      │
└─────────────────────────────────────────┘