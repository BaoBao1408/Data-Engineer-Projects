\# KNIME E-Commerce Data Processing Workflow



This repository contains a KNIME workflow that performs advanced data processing tasks on an e-commerce dataset consisting of 7+ tables such as customers, orders, products, shipping, payments, etc.



\## 🧩 Workflow Features



\- ✅ Email validation \& auto-filling invalid values (`String Manipulation`, `Column Expressions`)

\- ✅ Customer age calculation from birthdate

\- ✅ Product-level profit computation (`Math Formula`)

\- ✅ Rule-based logic for flags and classification (`Rule Engine`)

\- ✅ GroupBy aggregation by product/category/year



\## 📁 Dataset Tables



\- `customers\_upgraded.csv`

\- `orders\_upgraded.csv`

\- `products\_upgraded.csv`

\- `payment\_upgraded.csv`

\- `shipping\_upgraded.csv`

\- Additional derived tables



\## 🧠 KNIME Nodes Used



\- `String Manipulation`

\- `Rule Engine`

\- `Math Formula`

\- `Column Expressions (legacy)`

\- `GroupBy`

\- `Joiner`

\- `Date\&Time to String`, `String to Date\&Time`

\- `Missing Value`



\## 📝 Syntax Reference



See the `KNIME\_Cheatsheet\_TongHop\_Revised.docx` for all code expressions used, including:

```javascript

// Email autofill if invalid

if (column("check\_valid\_email") == "Invalid") {

&nbsp;   column("name") + "@default.com"

} else {

&nbsp;   column("email")

}

📊 Visualization

Data is prepared for loading into Power BI for KPI and dashboard reporting:



Monthly revenue, profit, and order count



Top products and customers



Delivery delay tracking



📦 How to Use

Open advanced\_ecommerce.knwf in KNIME Analytics Platform



Place all CSV files in the appropriate input folder



Execute the workflow



Export output tables to Power BI



👤 Author

Quoc Bao Nguyen | Data Engineer Candidate | 2025

