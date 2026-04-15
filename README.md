# 🔄 WealthFam Ingestion Parser

<div align="center">

[![Version](https://img.shields.io/badge/Version-2.2.15-1b5e20?style=for-the-badge)](https://github.com/oksbwn/wealthfam/parser)
[![Framework](https://img.shields.io/badge/FastAPI-0.109+-009688?style=for-the-badge&logo=fastapi)](https://fastapi.tiangolo.com/)
[![Logic](https://img.shields.io/badge/Parsing-Multi_Tier-0891b2?style=for-the-badge)](https://fastapi.tiangolo.com/)

**The high-performance ingestion backbone of the WealthFam ecosystem.**  
*Stateless, AI-augmented, and optimized for sub-second transaction parsing.*

</div>

---

## 🚀 Overview

The WealthFam Parser is a specialized microservice dedicated to transforming unstructured financial alerts (SMS, Email, CAS PDFs) into structured ledger entries. It employs a tiered methodology of static, learned, and AI-powered parsers to achieve 100% accuracy.

---

## 🛠️ Ingestion Engines

| Tier | Engine | Description |
| :--- | :--- | :--- |
| **Tier 1** | **Static Parsers** | High-performance logic for major banks and UPI alerts. |
| **Tier 2** | **Pattern Engine** | User-trained regex patterns that evolve with usage. |
| **Tier 3** | **AI Fallback** | Gemini-powered parsing for complex or unknown formats. |

---

## 🏁 Development

To setup the parser locally or extend the ingestion logic, please visit the unified guide:

[**🛠️ In-Depth Setup Guide**](../../Docs/technical/development/setup.md)

---

## 📖 Key Documentation
- **[📐 Ingestion Engineering](../../Docs/technical/architecture/ingestion.md)**: Master parsing logic details.
- **[🛠️ Technical Setup](../../Docs/technical/development/setup.md)**: Local installation guide.

---
<div align="center">
*Developed by the WealthFam Parser Team*
</div>
