# Data dictionary

## Raw transactions

| Field | Type | Meaning |
|---|---|---|
| `transaction_id` | integer | Unique purchase record |
| `customer_id` | integer | Customer identifier |
| `product_name` | string | Product vocabulary used by the transaction system |
| `transaction_date` | date | Purchase date in `yyyy-MM-dd` format |

## Raw customers

| Field | Type | Meaning |
|---|---|---|
| `customer_id` | integer | Unique customer identifier |
| `customer_name` | string | Synthetic customer name |
| `join_date` | date | Date the customer joined |
| `location` | string | Synthetic US state |

## Prepared products

| Field | Type | Meaning |
|---|---|---|
| `product_id` | integer | Product identifier from the public product master |
| `product_name` | string | Standardised name used for transaction joins |
| `source_product_name` | string | Original value retained from the public CSV |
| `category` | string | Product category |
| `price` | double | Demonstration unit price |

The dataset is synthetic and too small for real market inference. Its purpose is to demonstrate pipeline design and Spark transformations.
