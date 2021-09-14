# Estimating Resource Costs

This repository shows a way to compute the associated person month resource cost estimates for *Software Developers and Software Quality Assurance Analysts and Testers* (`15-1256`) from 2008 - 2019.

It uses the Occupational Employment and Wage Statistics ([OEWS](https://www.bls.gov/oes/)) data and the [Components of Value Added](https://apps.bea.gov/iTable/iTable.cfm?reqid=150&step=2&isuri=1&categories=compbyind).

## Results

This table provides the person-month resource cost estimates in current dollars using two potential approaches.

| Year | BLS/SUT-IG | NIPA/KLEMS-EIPS |
|:----:|:----------:|:---------------:|
| 2008 |  $19,751   |     $21,411     |
| 2009 |  $20,447   |     $22,170     |
| 2010 |  $21,210   |     $22,554     |
| 2011 |  $21,378   |     $22,816     |
| 2012 |  $20,887   |     $22,949     |
| 2013 |  $21,166   |     $24,136     |
| 2014 |  $21,309   |     $24,992     |
| 2015 |  $20,062   |     $25,863     |
| 2016 |  $20,787   |     $26,479     |
| 2017 |  $20,928   |     $27,018     |
| 2018 |  $21,289   |     $27,289     |
| 2019 |  $21,025   |     $27,393     |

## Code Organization
- `scripts/OEWS.jl` => `data/oews_15-1256.csv`
- `scripts/BLS-series.jl` => `data/bls_salary_wages_to_total_compensation.csv`
- `scripts/ComponentsValueAdded.jl` => `data/comp_of_va.csv`
- `scripts/ResourceCosts.jl` => `data/person_monthly_resource_cost.csv`

## TODO

- Pull NIPA tables using the BEA API


## Similar ways to compute resource costs

- [Information Processing Equipment and Software in the National Accounts](https://www.bea.gov/research/papers/2002/information-processing-equipment-and-software-national-accounts) (Grimm, Moulton, and Wasshausen 2002)
