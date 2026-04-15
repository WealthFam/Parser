# 🔄 WealthFam Ingestion Parser

<div align="center">

[![Version](https://img.shields.io/badge/Version-2.2.15-4338ca?style=for-the-badge)](https://github.com/WealthFam/parser)
[![Framework](https://img.shields.io/badge/FastAPI-0.109+-009688?style=for-the-badge&logo=fastapi)](https://fastapi.tiangolo.com/)
[![Status](https://img.shields.io/badge/Docs-Technical_Hub-e11d48?style=for-the-badge)](https://wealthfam.github.io/docs)

**The high-performance ingestion backbone of the WealthFam ecosystem.**  
*Stateless, AI-augmented, and optimized for sub-second transaction parsing.*

</div>

---

## 🚀 Overview

The WealthFam Parser is a specialized microservice dedicated to transforming unstructured financial alerts (SMS, Email, CAS PDFs) into structured ledger entries. It employs a tiered methodology of static, learned, and AI-powered parsers to achieve 100% accuracy.

---

## 🛠️ Ingestion Engines at a Glance

For a detailed breakdown of our tiered parsing logic, see:  
[**📖 Deep Dive: Data Ingestion Flow**](../Docs/technical/architecture/ingestion.md)

| Tier | Engine | Description |
| :--- | :--- | :--- |
| **Tier 1** | **Static Parsers** | High-performance logic for major banks and UPI alerts. |
| **Tier 2** | **Pattern Engine** | User-trained regex patterns that evolve with usage. |
| **Tier 3** | **AI Fallback** | Gemini-powered parsing for complex or unknown formats. |

---

## 🏗️ Architecture

The parser is designed to be highly scalable and stateless, allowing for rapid horizontal scaling during peak ingestion periods (e.g., month-end).

- **[🏛️ System Overview](../Docs/technical/architecture/system_overview.md)**
- **[📐 Ingestion Engineering](../Docs/technical/architecture/ingestion.md)**

---

## 🏁 Development Setup

To setup the parser locally or extend the ingestion logic:

[**🛠️ Master Setup Guide**](../Docs/technical/getting_started.md)

---
<div align="center">
*Maintained by the WealthFam Parser Engineering Team*
</div>
