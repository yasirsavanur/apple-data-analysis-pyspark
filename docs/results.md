# Expected results

## Immediate iPhone to AirPods sequences

| Customer | Name | iPhone date | AirPods date | Days |
|---:|---|---|---|---:|
| 105 | Eva | 2022-02-01 | 2022-02-04 | 3 |
| 108 | Henry | 2022-02-05 | 2022-02-09 | 4 |

Average delay: **3.5 days**.

## Customers who bought only iPhone and AirPods

| Customer | Name | Products |
|---:|---|---|
| 107 | Grace | AirPods, iPhone |
| 108 | Henry | AirPods, iPhone |

This rule ignores order. It tests whether the complete distinct product set is exactly those two products.

## Purchase journeys

| Customer | First product | Later products in order |
|---:|---|---|
| 105 | iPhone | AirPods, MacBook |
| 106 | iPhone | MacBook, AirPods |
| 107 | AirPods | iPhone |
| 108 | iPhone | AirPods |

## Revenue ranking

| Rank | Product | Units | Demonstration revenue |
|---:|---|---:|---:|
| 1 | MacBook | 2 | 2000 |
| 2 | iPhone | 4 | 1800 |
| 3 | AirPods | 4 | 1000 |
