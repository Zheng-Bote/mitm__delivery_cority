---
name: Feature Specification (Spec Kit)
about: Propose a new feature, collector, or transformation rule.
title: "Feature: "
labels: "feature, spec-kit"
assignees: ""
---

## Feature Intent

_Describe what the feature does and why it is needed. Keep it concise._

## Scope

_What components (Collectors, Transformation, Delivery) does this affect?_

## SpecDD Architecture Alignment (Drift Control)

Please confirm that this feature respects the global `mitm-2` constraints defined in `.sdd` files:

- [ ] **Architecture:** The layered architecture is maintained (e.g. no direct bypass from Collector to Delivery).
- [ ] **Architecture:** Feature affects architecture: SpecKit feature forces update of the SpecDD .sdd
- [ ] **Security:** Envelope Encryption (AES-GCM) is NOT bypassed for PII data.
- [ ] **Data Model:** Core PostgreSQL schemas remain intact (feature-specific tables are allowed).
- [ ] **Standards:** SPDX headers, English documentation, and independent `go.mod` per layer will be maintained.

## Acceptance Criteria

- [ ] Criterion 1
- [ ] Criterion 2
