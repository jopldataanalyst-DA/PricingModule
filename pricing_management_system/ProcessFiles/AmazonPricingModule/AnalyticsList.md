# Amazon B2B Seller Analytics — Business Question Framework

> **Dataset scope:** January 2026 | Multi-GSTIN seller | AFN + MFN fulfillment | Pan-India shipments  
> **How to use:** Each question maps to a KPI card, chart, or table in a dashboard.  
> Questions marked 🔑 are executive-priority. Questions marked ⚠️ flag operational risks.

---

## 1. Revenue & Sales Performance

*Goal: Understand true earnings after returns, and identify growth drivers.*

1. 🔑 What is **net revenue** after subtracting all refunds? *(Gross shipment revenue − refund amounts)*
2. What is the **gross-to-net revenue gap** — how much is lost to returns as a percentage?
3. What is the **average order value (AOV)** for shipments only (excluding refunds and cancellations)?
4. What is the **average selling price per unit** (net revenue ÷ shipped units)?
5. How is **daily revenue trending** — are there spikes or dips on specific dates?
6. Which **days of the week** generate the highest and lowest sales?
7. What is the **revenue contribution split** between AFN and MFN fulfillment channels?
8. Which **seller GSTIN** contributes the most and least net revenue?
9. What is the **revenue per fulfilled unit** (net revenue ÷ non-cancelled, non-refunded quantity)?
10. Is revenue **top-heavy** — do a small number of orders drive a disproportionate share of total revenue?

---

## 2. Transaction Quality & Health

*Goal: Diagnose how many orders actually result in completed, paid deliveries.*

11. 🔑 What is the **net shipment rate**? *(Shipments − refunds) ÷ gross shipments*
12. What is the **refund rate** by order count and by invoice value?
13. What is the **cancellation rate** by order count?
14. ⚠️ How many orders went through a **cancel → re-ship cycle** (same Order ID has both Cancel and Shipment)?
15. Are refunds happening **same-day or days after shipment** — indicating process issues vs customer returns?
16. ⚠️ Are refunds **concentrated in high-value orders** or distributed evenly across price points?
17. ⚠️ Are there **duplicate invoice numbers** across different GSTINs or the same GSTIN?
18. Which orders have **both a shipment and a refund** — completed return loop vs partial resolution?
19. What is the **cancel-to-ship ratio per seller GSTIN** — which entity has the worst transaction quality?
20. Are there orders with **more than one refund entry** — indicating data issues or double-processing?

---

## 3. Product & SKU Intelligence

*Goal: Know which products drive profits and which drain them.*

21. 🔑 Which SKUs generate the **highest net revenue** after refunds?
22. Which SKUs generate the **highest gross revenue** but also carry the most refunds?
23. Which SKUs sell the **most units** — volume leaders vs revenue leaders?
24. Which **ASINs appear across multiple seller GSTINs** — possible inter-GSTIN stock sharing?
25. Which **HSN codes** contribute the most to taxable revenue?
26. Which products are **exclusively sold via MFN vs AFN** — are there channel-specific products?
27. Which SKUs have a **zero refund record** — most reliable products for customer satisfaction?
28. ⚠️ Which SKUs have **high refund rates** relative to their shipment volume?
29. Which **size variants** within the same product underperform compared to others?
30. Which **SKUs contribute 80% of sales** (Pareto / ABC analysis)?
31. Which products have **high tax burden relative to revenue** — low-margin, high-tax items?
32. Which products should be **discontinued** based on combined signals: low revenue + high refund + low velocity?

---

## 4. Customer Geography

*Goal: Identify demand hotspots, underserved markets, and high-risk locations.*

33. 🔑 Which **states generate the highest net revenue** after refunds?
34. Which **cities account for the highest order volume** — top 10 concentration?
35. Which **states buy the most units** regardless of order value?
36. ⚠️ Which **states have refund rates above the national average**?
37. Which **postal codes** generate maximum revenue — hyperlocal hotspots?
38. Which **states or cities are completely absent** from the customer map — untapped markets?
39. What is the **interstate vs intrastate shipment ratio** — relevant for GST compliance (IGST vs CGST/SGST)?
40. Are **high-value orders concentrated in metro cities** or spread across Tier 2/3 towns?
41. Which **states show order growth** within the dataset period vs which are declining?

---

## 5. Shipping & Route Analysis

*Goal: Optimize logistics and identify costly or high-refund shipping lanes.*

42. Which **Ship_From cities** handle the most outgoing orders?
43. Which **Ship_From states** generate the maximum shipment volume?
44. What are the **top 10 shipping routes** (state-to-state) by volume?
45. ⚠️ Which **routes have high refund rates** — are certain corridors problematic?
46. What is the **average distance proxy** — are most shipments local, regional, or national?
47. Which routes represent **intrastate shipments** — relevant for tax categorization?
48. Are **inter-GSTIN routes** present — same seller shipping across state GSTIN boundaries?

---

## 6. Warehouse Performance

*Goal: Measure warehouse efficiency, utilization, and loss contribution.*

49. 🔑 Which **warehouse processes the most orders** and generates the most revenue?
50. What is the **revenue per warehouse** — normalized by order count?
51. ⚠️ Which **warehouse has the highest refund percentage** — is it a logistics/handling issue?
52. Which **warehouses are underutilized** — low order volume relative to others?
53. Is **workload distributed evenly** across warehouses or concentrated in one?
54. Which **warehouse + fulfillment channel combination** performs best?
55. Are **cancelled orders disproportionately linked to a specific warehouse**?

---

## 7. Fulfillment Channel Analysis

*Goal: Compare AFN vs MFN on revenue, returns, and efficiency.*

56. 🔑 Which **fulfillment channel (AFN vs MFN) generates higher net revenue**?
57. Which **channel has the higher refund rate** by count and by value?
58. Which **channel handles more order volume**?
59. Which **channel has higher AOV** — do customers order more expensive items via one channel?
60. ⚠️ Is MFN driving more cancellations than AFN — or vice versa?
61. What is the **revenue leakage per channel** — refund impact on each?

---

## 8. Tax & Compliance Analysis

*Goal: Ensure accurate GST tracking and flag potential compliance gaps.*

62. 🔑 What is the **total tax collected** across all shipments?
63. What is the **effective tax rate** (Total Tax ÷ Tax-Exclusive Gross)?
64. Which **states generate the highest GST collection** on delivered orders?
65. Which **HSN codes carry the highest tax burden** — are these correctly classified?
66. ⚠️ Are there shipments **with zero or null HSN codes** — compliance risk?
67. Is the **tax rate consistent across the same HSN code** — or are there discrepancies?
68. What is the **IGST vs CGST/SGST split** based on intrastate vs interstate routes?
69. Are **refunded transactions correctly reversing tax** amounts (negative tax on refunds)?

---

## 9. Order Quality & Data Integrity

*Goal: Detect anomalies, duplicates, and data gaps before they become financial errors.*

70. ⚠️ Are there **duplicate invoice numbers** across the dataset?
71. ⚠️ Are there **orders with missing Invoice Numbers** (blank or null)?
72. Are there **orders with missing Warehouse IDs** — fulfillment tracking gap?
73. What is the **average quantity per order** — are bulk orders driving volume?
74. Are there **orders with unusually large quantities** — possible data entry errors?
75. Are there **transactions with zero Invoice Amount** but Transaction_Type = Shipment?
76. ⚠️ Are there **negative invoice amounts on non-refund transactions** — data corruption?
77. Do all refunds have a **corresponding original shipment** in the dataset?

---

## 10. Operational & Restock Intelligence

*Goal: Drive inventory, logistics, and channel decisions from data.*

78. 🔑 Which **SKUs should be restocked first** based on high velocity + low refund rate?
79. Which **products need demand investigation** — low sales but no refunds (possibly out of stock)?
80. Which **states need targeted inventory allocation** based on unfulfilled demand signals?
81. Which **warehouse needs workload rebalancing** — too concentrated or too idle?
82. ⚠️ Which **fulfillment process is causing revenue leakage** — high refund channel?
83. Which **shipping routes should be consolidated** for cost efficiency?
84. Which **seller GSTIN needs compliance review** — high cancellation or duplicate invoice rates?

---

## 11. Executive Summary Questions

*Goal: Answer the CEO/owner's top-of-mind questions in under 60 seconds.*

85. 🔑 What is the **single biggest revenue driver** — top SKU, state, or channel?
86. 🔑 How much revenue was **lost to refunds and cancellations** this month?
87. What is the **overall business health score** — net revenue, refund rate, shipment rate combined?
88. Which **3 products deserve more inventory and marketing focus**?
89. Which **3 products are destroying margins** through high returns?
90. Which **states or cities should be targeted next** for growth?
91. ⚠️ What are the **top 3 operational risks** — data gaps, high refund routes, underperforming warehouses?
92. What **changed most compared to prior periods** — if comparative data exists?
93. Is the business **AFN-dependent or balanced** across fulfillment channels?
94. Which **seller GSTIN entity is the most and least efficient**?
95. If only **one action could be taken today**, what would deliver the most revenue impact?

---

## Appendix: Question Tags Legend

| Tag | Meaning |
|-----|---------|
| 🔑 | Executive-priority KPI — must appear in the top-level dashboard |
| ⚠️ | Operational risk indicator — flag for immediate review |
| *(no tag)* | Supporting metric — drill-down or secondary analysis |

---

*Framework covers: 11 dimensions | 95 questions | 4 new dimensions vs original (Transaction Quality, Data Integrity, Compliance, Executive Summary)*