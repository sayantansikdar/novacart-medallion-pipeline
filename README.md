# NovaCart Order Analytics: Medallion Pipeline

A bronze → silver → gold pipeline for NovaCart's September 2026 orders, built
for the "NovaCart Order Analytics: Data Engineering Lab". The pipeline must give
the right answer after batch 1, after batch 2, and when either batch is run twice.

Team: Bug Byts

## Status

Work in progress, built one module at a time. This README is completed in the
final module with run instructions, evidence and the AI-assistance disclosure.

## Repository rules

- The input files are never edited or committed. They are read from storage at runtime.
- No secret, token or key is ever committed.
