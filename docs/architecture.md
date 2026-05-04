# Architecture Overview

## Data Flow

```mermaid
graph LR
    CSV[CSV Files in<br/>Unity Catalog Volume] --> B[🥉 Bronze<br/>5 Raw Delta Tables]
    B --> S[🥈 Silver<br/>orders_master]
    S --> G1[🥇 revenue_by_category_monthly]
    S --> G2[🥇 customer_lifetime_value]
    S --> G3[🥇 seller_performance]
    G1 --> G4[🥇 category_mom_trend]

    style CSV fill:#e8e8e8,stroke:#666
    style B fill:#cd7f32,stroke:#8b5a2b,color:#fff
    style S fill:#c0c0c0,stroke:#808080,color:#000
    style G1 fill:#ffd700,stroke:#b8860b,color:#000
    style G2 fill:#ffd700,stroke:#b8860b,color:#000
    style G3 fill:#ffd700,stroke:#b8860b,color:#000
    style G4 fill:#ffd700,stroke:#b8860b,color:#000
```

## Unity Catalog Namespace

```
brazilian-ecommerce (catalog)
├── bronze (schema)
│   ├── orders
│   ├── order_items
│   ├── customers
│   ├── products
│   └── payments
├── silver (schema)
│   └── orders_master
├── gold (schema)
│   ├── revenue_by_category_monthly
│   ├── customer_lifetime_value
│   ├── seller_performance
│   └── category_mom_trend
└── delta_demos (schema)
    ├── orders_incremental
    ├── customer_segments_scd2
    ├── schema_evolution_demo
    └── cdf_demo
```

## Workflow DAG

```mermaid
graph TD
    A[bronze_ingestion] --> B[silver_transformation]
    B --> C[gold_aggregation]
    A --> D[dlt_pipeline]

    style A fill:#cd7f32,color:#fff
    style B fill:#c0c0c0
    style C fill:#ffd700
    style D fill:#4a90d9,color:#fff
```

## CI/CD Flow

```mermaid
graph LR
    PR[Pull Request] --> L[Lint<br/>ruff]
    L --> T[Test<br/>pytest]
    T --> BLD[Build<br/>wheel]
    BLD --> STG[Deploy<br/>Staging]
    STG --> SMOKE[Smoke<br/>Test]
    SMOKE --> APPROVE[Manual<br/>Approval]
    APPROVE --> PROD[Deploy<br/>Production]

    style PR fill:#6c757d,color:#fff
    style L fill:#17a2b8,color:#fff
    style T fill:#28a745,color:#fff
    style BLD fill:#ffc107,color:#000
    style STG fill:#fd7e14,color:#fff
    style SMOKE fill:#20c997,color:#fff
    style APPROVE fill:#dc3545,color:#fff
    style PROD fill:#6610f2,color:#fff
```
