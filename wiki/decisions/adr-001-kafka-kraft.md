# ADR-001: Apache Kafka in KRaft Mode

**Date:** 2026-07-05
**Status:** Accepted

## Context

Kafka historically required ZooKeeper for broker coordination, controller election, and metadata storage. ZooKeeper is a separate process consuming ~256–512 MB additional RAM and adding operational complexity (own healthcheck, quorum, upgrade path). Since Kafka 3.7, KRaft mode (Kafka Raft Metadata) is the stable production default. As of Kafka 4.0, ZooKeeper mode is removed.

## Decision

Deploy Kafka using KRaft mode with a single combined broker/controller node (`KAFKA_CFG_PROCESS_ROLES=broker,controller`). No ZooKeeper service.

Image: `bitnami/kafka:3.9`
Node ID: 1
Quorum voters: `1@kafka:9093`

## Consequences

| | |
|---|---|
| ✅ | Saves ~512 MB RAM — meaningful on a 7–8 GB usable RAM machine |
| ✅ | Single service; simpler Docker Compose definition and healthcheck |
| ✅ | Aligns with Kafka's roadmap (ZooKeeper fully deprecated) |
| ✅ | Faster broker startup (~5 s vs ~20 s with ZooKeeper) |
| ⚠️ | Requires Kafka ≥3.3; we use 3.9 (well within support window) |
| ⚠️ | Single-node KRaft has no HA; acceptable for a dev/portfolio environment |
