-- ════════════════════════════════════════════════════════════════
-- Unity Catalog Permissions — Phase 8 (Security Hardening)
-- ════════════════════════════════════════════════════════════════
--
-- Run these in the Databricks SQL Editor after setting up
-- your service principal and user groups.
--
-- PRINCIPLE OF LEAST PRIVILEGE:
-- Each role gets ONLY the access it needs. Nothing more.
-- ════════════════════════════════════════════════════════════════

-- ── Pipeline Service Principal ──
-- The CI/CD pipeline runs as this identity. It needs:
-- - CREATE TABLE in Bronze (writes raw data)
-- - MODIFY in Silver/Gold (updates tables)
-- - USAGE on all schemas
GRANT USAGE ON CATALOG `brazilian-ecommerce` TO `pipeline-service-principal`;
GRANT USAGE ON SCHEMA `brazilian-ecommerce`.`bronze` TO `pipeline-service-principal`;
GRANT USAGE ON SCHEMA `brazilian-ecommerce`.`silver` TO `pipeline-service-principal`;
GRANT USAGE ON SCHEMA `brazilian-ecommerce`.`gold` TO `pipeline-service-principal`;

GRANT CREATE TABLE ON SCHEMA `brazilian-ecommerce`.`bronze` TO `pipeline-service-principal`;
GRANT MODIFY ON SCHEMA `brazilian-ecommerce`.`silver` TO `pipeline-service-principal`;
GRANT MODIFY ON SCHEMA `brazilian-ecommerce`.`gold` TO `pipeline-service-principal`;

-- ── Analytics Team (Data Analysts, BI Developers) ──
-- Read-only access to Gold tables (business-ready data)
-- NO access to Bronze/Silver (raw/intermediate data)
GRANT USAGE ON CATALOG `brazilian-ecommerce` TO `analytics-team`;
GRANT USAGE ON SCHEMA `brazilian-ecommerce`.`gold` TO `analytics-team`;
GRANT SELECT ON SCHEMA `brazilian-ecommerce`.`gold` TO `analytics-team`;

-- ── Data Engineering Team ──
-- Full access for debugging and development
GRANT USAGE ON CATALOG `brazilian-ecommerce` TO `data-engineering-team`;
GRANT ALL PRIVILEGES ON SCHEMA `brazilian-ecommerce`.`bronze` TO `data-engineering-team`;
GRANT ALL PRIVILEGES ON SCHEMA `brazilian-ecommerce`.`silver` TO `data-engineering-team`;
GRANT ALL PRIVILEGES ON SCHEMA `brazilian-ecommerce`.`gold` TO `data-engineering-team`;

-- ── Volume Access (Source Data) ──
GRANT READ VOLUME ON VOLUME `brazilian-ecommerce`.`filestore`.`olis`
TO `pipeline-service-principal`;
