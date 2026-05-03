# Project Intent: POC Data Ingestion Pipeline

## What

This project is a local proof of concept for a generic data ingestion pipeline that processes product files into a shared business domain and produces operational analytics for observability, audit, quality, and troubleshooting.

## Why

It exists to validate a simple, low-cost ingestion architecture that can support multiple products with low product-specific customization, traceable processing, rejected-record handling, and operational visibility before moving to a real AWS deployment.

## Current State

The project implements a local end-to-end pipeline that accepts file-ready events, validates configured products, copies source files to a raw zone, harmonizes product layouts into a shared transaction domain, enriches accepted records, records rejected rows, publishes downstream events, writes analytics facts, registers catalog tables, and runs local analytical queries.

The repository includes source scripts, configuration files, sample events and data, unit tests, an end-to-end test, local runtime output, and architecture documentation.

## Current Features

- Event-driven ingestion
- Config-driven product onboarding
- Business data lake processing
- Rejected-record handling
- Downstream event publication
- Operational analytics and audit
- Catalog and query layer
- Local execution and evidence generation

## Related

- [Decision: Tech Stack](../decisions/001-tech-stack.md)
- [Decision: Local AWS Emulation](../decisions/002-local-aws-emulation.md)
- [Decision: Event Orchestration](../decisions/003-event-orchestration.md)
- [Decision: Config Driven Product Mapping](../decisions/004-config-driven-product-mapping.md)
- [Decision: S3 Data Lake Layout](../decisions/005-s3-data-lake-layout.md)
- [Decision: Enriched Analytics Model](../decisions/006-enriched-analytics-model.md)
- [Decision: Rejection Policy And Downstream Events](../decisions/007-rejection-policy-and-downstream-events.md)
- [Decision: Local Query Simulation](../decisions/008-local-query-simulation.md)

## Status

- **Created**: 2026-05-02 (Phase: Intent)
- **Status**: Active
- **Note**: Generated from existing codebase analysis
