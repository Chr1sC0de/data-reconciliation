# Data Reconciler

- [Data Reconciler](#data-reconciler)
  - [Comparing Tables](#comparing-tables)
    - [The queries](#the-queries)
    - [The Imports](#the-imports)
    - [Table Comparison](#table-comparison)
      - [Without Index](#without-index)
      - [With Index](#with-index)
    - [TableReconciliationData Objects](#tablereconciliationdata-objects)
      - [Content](#content)
      - [Left and Right Rows](#left-and-right-rows)
    - [Summarizing Results](#summarizing-results)

A set of utilities which can used to validate data (tables only at the
moment) using polars LazyFrames.

## Comparing Tables

The following examples use dbqq to manage database login details and grab
table data

### The queries

Let's work with the two following queries, in our example we have a module `tests`
with the following queries `query1` and `query2`

```sql
select
    nvl (to_char (settlementdate, 'YYYY-MM-DD'), -1) as settlementdate,
    runno,
    participantid,
    periodid,
    marketfeeid,
    participantcategoryid,
    nvl (
        to_char (max(effective_from_timestamp), 'YYYY-MM-DD'),
        -1
    ) as max_effective_from_timestamp,
    nvl (sum(energy), -1) as sum_energy,
    nvl (sum(marketfeevalue), -1) as sum_marketfeevalue,
    nvl (sum(feerate), -1) as sum_feerate,
    nvl (sum(feeunits), -1) as sum_feeunits
from
    sds.info_g_setmarketfees
where
    1 = 1
    and effective_from_timestamp < sysdate - 2
    and effective_to_timestamp > sysdate
group by
    nvl (to_char (settlementdate, 'YYYY-MM-DD'), -1),
    runno,
    participantid,
    periodid,
    marketfeeid,
    participantcategoryid
order by
    settlementdate,
    runno,
    participantid,
    periodid,
    marketfeeid,
    participantcategoryid
fetch first 10000 rows only
```

```sql
select
    nvl (cast(date (settlementdate) as string), -1) as settlementdate,
    runno,
    participantid,
    periodid,
    marketfeeid,
    participantcategoryid,
    nvl (
        cast(date (max(effective_from_timestamp)) as string),
        -1
    ) as max_effective_from_timestamp,
    nvl (sum(energy), -1) as sum_energy,
    nvl (sum(marketfeevalue), -1) as sum_marketfeevalue,
    nvl (sum(feerate), -1) as sum_feerate,
    nvl (sum(feeunits), -1) as sum_feeunits
from
    ea_edp_prod_infoserver_generation_raw.setmarketfees
where
    1 = 1
    and active_ind = 'Y'
group by
    nvl (cast(date (settlementdate) as string), -1),
    runno,
    participantid,
    periodid,
    marketfeeid,
    participantcategoryid
order by
    nvl (cast(date (settlementdate) as string), -1),
    runno,
    participantid,
    periodid,
    marketfeeid,
    participantcategoryid
limit 10500
```

### The Imports

```python
from dbqq import connectors
from datarec import tables
from tests.queries import query1, query2
```

### Table Comparison

The datarec package provides two methods for comparing tables:

datarec.tables.is_equal: Compares data from two tables, ensuring that the values
are equal. datarec.tables.is_close_numeric: Compares numeric values from two
tables, ensuring that the values are close based on absolute tolerance a_tol and
relative tolerance r_tol. Both comparison methods share similar keyword
arguments, except for the a_tol and r_tol arguments exclusive to
is_close_numeric.

Index Arguments During testing, each table is indexed, and each testable entry
in the row is validated. Each column has a suffix:

- ~...~: Specifies the table location.
- ~_..._~: Specifies the test results.
- **...**: Specifies if the column was found in either the left, right, or both
  tables.
- No surrounding text for the suffix: Specifies columns used as indexes.

The most useful arguments for the comparison methods are columns_to_ignore and
column_indexes. When no column_indexes are provided, a row number is used as the
index.

Indexes can be defined using an iterable object. In the indexed example, we set
the range to [0,1,2,3,4,5]. These indexes are not validated and do not have a
suffix. The group of indexes must be distinct and match rows between tables.

Like indexes, ignored columns can be set. Ignored columns are not indexed or
validated, but their values can be compared visually.

Lastly, at the end of the table are the left, right, and both identifier
columns. These columns indicate if the matched columns via index are in the left
table, right table, or both tables.

#### Without Index

```python
connector1 = connectors.oracle.improd()
connector2 = connectors.databricks.prod()
p1 = connector1.cache()(query1)
p2 = connector2.cache()(query2)
validation = tables.is_equal(
p1,
p2,
pl1_name="left",
pl2_name="right",
show_both_first=True,
show_failed_first=True,
interlaced=True,
column_case="upper",
columns_to_ignore=None,
column_indexes=None,
)
validation.results.collect()
```

```python
shape: (10_500, 37)
┌────────────┬───────────────────────┬────────────────────────┬────────────────┬──────────────┬───────────────┬──────────────────────┬──────────────────────┬───────────────────────┬──────────────────────────────┬───┬────────────────────┬────────────────────┬─────────────────────┬────────────────────────────┬─────────────────────┬──────────────────────┬─────────────────────────────┬──────────┬───────────┬──────────┐
│ row_number ┆ SETTLEMENTDATE ~LEFT~ ┆ SETTLEMENTDATE ~RIGHT~ ┆ SETTLEMENTDATE ┆ RUNNO ~LEFT~ ┆ RUNNO ~RIGHT~ ┆ RUNNO ~*VALIDATION*~ ┆ PARTICIPANTID ~LEFT~ ┆ PARTICIPANTID ~RIGHT~ ┆ PARTICIPANTID ~*VALIDATION*~ ┆ … ┆ SUM_MARKETFEEVALUE ┆ SUM_FEERATE ~LEFT~ ┆ SUM_FEERATE ~RIGHT~ ┆ SUM_FEERATE ~*VALIDATION*~ ┆ SUM_FEEUNITS ~LEFT~ ┆ SUM_FEEUNITS ~RIGHT~ ┆ SUM_FEEUNITS ~*VALIDATION*~ ┆ **LEFT** ┆ **RIGHT** ┆ **BOTH** │
│ ---        ┆ ---                   ┆ ---                    ┆ ~*VALIDATION*~ ┆ ---          ┆ ---           ┆ ---                  ┆ ---                  ┆ ---                   ┆ ---                          ┆   ┆ ~*VALIDATION*~     ┆ ---                ┆ ---                 ┆ ---                        ┆ ---                 ┆ ---                  ┆ ---                         ┆ ---      ┆ ---       ┆ ---      │
│ u32        ┆ str                   ┆ str                    ┆ ---            ┆ i64          ┆ i64           ┆ bool                 ┆ str                  ┆ str                   ┆ bool                         ┆   ┆ ---                ┆ f64                ┆ f64                 ┆ bool                       ┆ f64                 ┆ f64                  ┆ bool                        ┆ bool     ┆ bool      ┆ bool     │
│            ┆                       ┆                        ┆ bool           ┆              ┆               ┆                      ┆                      ┆                       ┆                              ┆   ┆ bool               ┆                    ┆                     ┆                            ┆                     ┆                      ┆                             ┆          ┆           ┆          │
╞════════════╪═══════════════════════╪════════════════════════╪════════════════╪══════════════╪═══════════════╪══════════════════════╪══════════════════════╪═══════════════════════╪══════════════════════════════╪═══╪════════════════════╪════════════════════╪═════════════════════╪════════════════════════════╪═════════════════════╪══════════════════════╪═════════════════════════════╪══════════╪═══════════╪══════════╡
│ 0          ┆ 1998-07-12            ┆ 1998-07-12             ┆ true           ┆ 5            ┆ 5             ┆ true                 ┆ YALLOURN             ┆ YALLOURN              ┆ true                         ┆ … ┆ true               ┆ -1.0               ┆ -1.0                ┆ true                       ┆ -1.0                ┆ -1.0                 ┆ true                        ┆ true     ┆ true      ┆ true     │
│ 1          ┆ 1998-07-12            ┆ 1998-07-12             ┆ true           ┆ 5            ┆ 5             ┆ true                 ┆ YALLOURN             ┆ YALLOURN              ┆ true                         ┆ … ┆ true               ┆ -1.0               ┆ -1.0                ┆ true                       ┆ -1.0                ┆ -1.0                 ┆ true                        ┆ true     ┆ true      ┆ true     │
│ 2          ┆ 1998-07-12            ┆ 1998-07-12             ┆ true           ┆ 5            ┆ 5             ┆ true                 ┆ YALLOURN             ┆ YALLOURN              ┆ true                         ┆ … ┆ true               ┆ -1.0               ┆ -1.0                ┆ true                       ┆ -1.0                ┆ -1.0                 ┆ true                        ┆ true     ┆ true      ┆ true     │
│ 3          ┆ 1998-07-12            ┆ 1998-07-12             ┆ true           ┆ 5            ┆ 5             ┆ true                 ┆ YALLOURN             ┆ YALLOURN              ┆ true                         ┆ … ┆ true               ┆ -1.0               ┆ -1.0                ┆ true                       ┆ -1.0                ┆ -1.0                 ┆ true                        ┆ true     ┆ true      ┆ true     │
│ …          ┆ …                     ┆ …                      ┆ …              ┆ …            ┆ …             ┆ …                    ┆ …                    ┆ …                     ┆ …                            ┆ … ┆ …                  ┆ …                  ┆ …                   ┆ …                          ┆ …                   ┆ …                    ┆ …                           ┆ …        ┆ …         ┆ …        │
│ 10496      ┆ null                  ┆ 1998-08-10             ┆ false          ┆ null         ┆ 3             ┆ false                ┆ null                 ┆ YALLOURN              ┆ false                        ┆ … ┆ false              ┆ null               ┆ -1.0                ┆ false                      ┆ null                ┆ -1.0                 ┆ false                       ┆ false    ┆ true      ┆ false    │
│ 10497      ┆ null                  ┆ 1998-08-10             ┆ false          ┆ null         ┆ 3             ┆ false                ┆ null                 ┆ YALLOURN              ┆ false                        ┆ … ┆ false              ┆ null               ┆ -1.0                ┆ false                      ┆ null                ┆ -1.0                 ┆ false                       ┆ false    ┆ true      ┆ false    │
│ 10498      ┆ null                  ┆ 1998-08-10             ┆ false          ┆ null         ┆ 3             ┆ false                ┆ null                 ┆ YALLOURN              ┆ false                        ┆ … ┆ false              ┆ null               ┆ -1.0                ┆ false                      ┆ null                ┆ -1.0                 ┆ false                       ┆ false    ┆ true      ┆ false    │
│ 10499      ┆ null                  ┆ 1998-08-10             ┆ false          ┆ null         ┆ 3             ┆ false                ┆ null                 ┆ YALLOURN              ┆ false                        ┆ … ┆ false              ┆ null               ┆ -1.0                ┆ false                      ┆ null                ┆ -1.0                 ┆ false                       ┆ false    ┆ true      ┆ false    │
└────────────┴───────────────────────┴────────────────────────┴────────────────┴──────────────┴───────────────┴──────────────────────┴──────────────────────┴───────────────────────┴──────────────────────────────┴───┴────────────────────┴────────────────────┴─────────────────────┴────────────────────────────┴─────────────────────┴──────────────────────┴─────────────────────────────┴──────────┴───────────┴──────────┘
```

#### With Index

```python
connector1 = connectors.oracle.improd()
connector2 = connectors.databricks.prod()
p1 = connector1.cache()(query1)
p2 = connector2.cache()(query2)
validation = tables.is_close_numeric(
p1,
p2,
pl1_name="left",
pl2_name="right",
show_both_first=True,
show_failed_first=True,
interlaced=True,
column_case="upper",
column_indexes=range(0, 6),
columns_to_ignore=[0],
a_tol=10e-3,
r_tol=10e-4,
)
validation.results.collect()
```

```python
shape: (10_500, 23)
┌────────────────┬───────┬───────────────┬──────────┬─────────────┬───────────────────────┬─────────────────────────────────────┬──────────────────────────────────────┬───────────────────┬────────────────────┬───┬───────────────────────────────────┬────────────────────┬─────────────────────┬────────────────────────────┬─────────────────────┬──────────────────────┬─────────────────────────────┬──────────┬───────────┬──────────┐
│ SETTLEMENTDATE ┆ RUNNO ┆ PARTICIPANTID ┆ PERIODID ┆ MARKETFEEID ┆ PARTICIPANTCATEGORYID ┆ MAX_EFFECTIVE_FROM_TIMESTAMP ~LEFT~ ┆ MAX_EFFECTIVE_FROM_TIMESTAMP ~RIGHT~ ┆ SUM_ENERGY ~LEFT~ ┆ SUM_ENERGY ~RIGHT~ ┆ … ┆ SUM_MARKETFEEVALUE ~*VALIDATION*~ ┆ SUM_FEERATE ~LEFT~ ┆ SUM_FEERATE ~RIGHT~ ┆ SUM_FEERATE ~*VALIDATION*~ ┆ SUM_FEEUNITS ~LEFT~ ┆ SUM_FEEUNITS ~RIGHT~ ┆ SUM_FEEUNITS ~*VALIDATION*~ ┆ **LEFT** ┆ **RIGHT** ┆ **BOTH** │
│ ---            ┆ ---   ┆ ---           ┆ ---      ┆ ---         ┆ ---                   ┆ ---                                 ┆ ---                                  ┆ ---               ┆ ---                ┆   ┆ ---                               ┆ ---                ┆ ---                 ┆ ---                        ┆ ---                 ┆ ---                  ┆ ---                         ┆ ---      ┆ ---       ┆ ---      │
│ str            ┆ i64   ┆ str           ┆ i64      ┆ str         ┆ str                   ┆ str                                 ┆ str                                  ┆ f64               ┆ f64                ┆   ┆ bool                              ┆ f64                ┆ f64                 ┆ bool                       ┆ f64                 ┆ f64                  ┆ bool                        ┆ bool     ┆ bool      ┆ bool     │
╞════════════════╪═══════╪═══════════════╪══════════╪═════════════╪═══════════════════════╪═════════════════════════════════════╪══════════════════════════════════════╪═══════════════════╪════════════════════╪═══╪═══════════════════════════════════╪════════════════════╪═════════════════════╪════════════════════════════╪═════════════════════╪══════════════════════╪═════════════════════════════╪══════════╪═══════════╪══════════╡
│ 1998-07-12     ┆ 5     ┆ YALLOURN      ┆ 0        ┆ F_ADMIN     ┆ ALL                   ┆ 2019-08-01                          ┆ 2022-07-06                           ┆ 0.0               ┆ 0.0                ┆ … ┆ true                              ┆ -1.0               ┆ -1.0                ┆ true                       ┆ -1.0                ┆ -1.0                 ┆ true                        ┆ true     ┆ true      ┆ true     │
│ 1998-07-12     ┆ 5     ┆ YALLOURN      ┆ 1        ┆ V_ADMIN     ┆ ALL                   ┆ 2019-08-01                          ┆ 2022-07-06                           ┆ -4.66778          ┆ -4.66778           ┆ … ┆ true                              ┆ -1.0               ┆ -1.0                ┆ true                       ┆ -1.0                ┆ -1.0                 ┆ true                        ┆ true     ┆ true      ┆ true     │
│ 1998-07-12     ┆ 5     ┆ YALLOURN      ┆ 1        ┆ V_EST       ┆ ALL                   ┆ 2019-08-01                          ┆ 2022-07-06                           ┆ -4.66778          ┆ -4.66778           ┆ … ┆ true                              ┆ -1.0               ┆ -1.0                ┆ true                       ┆ -1.0                ┆ -1.0                 ┆ true                        ┆ true     ┆ true      ┆ true     │
│ 1998-07-12     ┆ 5     ┆ YALLOURN      ┆ 1        ┆ V_METERING  ┆ ALL                   ┆ 2019-08-01                          ┆ 2022-07-06                           ┆ -4.66778          ┆ -4.66778           ┆ … ┆ true                              ┆ -1.0               ┆ -1.0                ┆ true                       ┆ -1.0                ┆ -1.0                 ┆ true                        ┆ true     ┆ true      ┆ true     │
│ …              ┆ …     ┆ …             ┆ …        ┆ …           ┆ …                     ┆ …                                   ┆ …                                    ┆ …                 ┆ …                  ┆ … ┆ …                                 ┆ …                  ┆ …                   ┆ …                          ┆ …                   ┆ …                    ┆ …                           ┆ …        ┆ …         ┆ …        │
│ 1998-08-10     ┆ 3     ┆ YALLOURN      ┆ 17       ┆ V_SETT      ┆ ALL                   ┆ null                                ┆ 2022-07-06                           ┆ null              ┆ 0.0                ┆ … ┆ false                             ┆ null               ┆ -1.0                ┆ false                      ┆ null                ┆ -1.0                 ┆ false                       ┆ false    ┆ true      ┆ false    │
│ 1998-08-10     ┆ 3     ┆ YALLOURN      ┆ 18       ┆ V_ADMIN     ┆ ALL                   ┆ null                                ┆ 2022-07-06                           ┆ null              ┆ 0.0                ┆ … ┆ false                             ┆ null               ┆ -1.0                ┆ false                      ┆ null                ┆ -1.0                 ┆ false                       ┆ false    ┆ true      ┆ false    │
│ 1998-08-10     ┆ 3     ┆ YALLOURN      ┆ 18       ┆ V_EST       ┆ ALL                   ┆ null                                ┆ 2022-07-06                           ┆ null              ┆ 0.0                ┆ … ┆ false                             ┆ null               ┆ -1.0                ┆ false                      ┆ null                ┆ -1.0                 ┆ false                       ┆ false    ┆ true      ┆ false    │
│ 1998-08-10     ┆ 3     ┆ YALLOURN      ┆ 18       ┆ V_METERING  ┆ ALL                   ┆ null                                ┆ 2022-07-06                           ┆ null              ┆ 0.0                ┆ … ┆ false                             ┆ null               ┆ -1.0                ┆ false                      ┆ null                ┆ -1.0                 ┆ false                       ┆ false    ┆ true      ┆ false    │
└────────────────┴───────┴───────────────┴──────────┴─────────────┴───────────────────────┴─────────────────────────────────────┴──────────────────────────────────────┴───────────────────┴────────────────────┴───┴───────────────────────────────────┴────────────────────┴─────────────────────┴────────────────────────────┴─────────────────────┴──────────────────────┴─────────────────────────────┴──────────┴───────────┴──────────┘
```

### TableReconciliationData Objects

After a table reconciliation a `TableReconciliationData` object is returned, we
can view the content of the object as dictionary using the `dataclasses.asdict`
method. Using the solution from the `With Index` section

#### Content

```python
import dataclasses
from pprint import pprint

pprint(dataclasses.asdict(validation), width=20, sort_dicts=False)
```

```python
{'results': <polars.LazyFrame object at 0x25973615A30>,
'left': 'LEFT',
'right': 'RIGHT',
'columns_all': ['SETTLEMENTDATE',
                'RUNNO',
                'PARTICIPANTID',
                'PERIODID',
                'MARKETFEEID',
                'PARTICIPANTCATEGORYID',
                'MAX_EFFECTIVE_FROM_TIMESTAMP',
                'SUM_ENERGY',
                'SUM_MARKETFEEVALUE',
                'SUM_FEERATE',
                'SUM_FEEUNITS'],
'columns_indexes': ['SETTLEMENTDATE',
                    'RUNNO',
                    'PARTICIPANTID',
                    'PERIODID',
                    'MARKETFEEID',
                    'PARTICIPANTCATEGORYID'],
'columns_ignored': ['MAX_EFFECTIVE_FROM_TIMESTAMP'],
'columns_tested': ['SUM_ENERGY',
                'SUM_MARKETFEEVALUE',
                'SUM_FEERATE',
                'SUM_FEEUNITS']}
```

#### Left and Right Rows

We can also isolate the rows which are contained only in the left and right
tables.

> [!Note]
>
> When extracting rows only in the left and right tables, suffixed and location
> columns are not included

```python
validation.get_rows_left_only().collect()
shape: (0, 11)
┌────────────────┬───────┬───────────────┬──────────┬─────────────┬───────────────────────┬──────────────────────────────┬────────────┬────────────────────┬─────────────┬──────────────┐
│ SETTLEMENTDATE ┆ RUNNO ┆ PARTICIPANTID ┆ PERIODID ┆ MARKETFEEID ┆ PARTICIPANTCATEGORYID ┆ MAX_EFFECTIVE_FROM_TIMESTAMP ┆ SUM_ENERGY ┆ SUM_MARKETFEEVALUE ┆ SUM_FEERATE ┆ SUM_FEEUNITS │
│ ---            ┆ ---   ┆ ---           ┆ ---      ┆ ---         ┆ ---                   ┆ ---                          ┆ ---        ┆ ---                ┆ ---         ┆ ---          │
│ str            ┆ i64   ┆ str           ┆ i64      ┆ str         ┆ str                   ┆ str                          ┆ f64        ┆ f64                ┆ f64         ┆ f64          │
╞════════════════╪═══════╪═══════════════╪══════════╪═════════════╪═══════════════════════╪══════════════════════════════╪════════════╪════════════════════╪═════════════╪══════════════╡
└────────────────┴───────┴───────────────┴──────────┴─────────────┴───────────────────────┴──────────────────────────────┴────────────┴────────────────────┴─────────────┴──────────────┘
```

```python
validation.get_rows_right_only().collect()
shape: (500, 11)
┌────────────────┬───────┬───────────────┬──────────┬─────────────┬───────────────────────┬──────────────────────────────┬────────────┬────────────────────┬─────────────┬──────────────┐
│ SETTLEMENTDATE ┆ RUNNO ┆ PARTICIPANTID ┆ PERIODID ┆ MARKETFEEID ┆ PARTICIPANTCATEGORYID ┆ MAX_EFFECTIVE_FROM_TIMESTAMP ┆ SUM_ENERGY ┆ SUM_MARKETFEEVALUE ┆ SUM_FEERATE ┆ SUM_FEEUNITS │
│ ---            ┆ ---   ┆ ---           ┆ ---      ┆ ---         ┆ ---                   ┆ ---                          ┆ ---        ┆ ---                ┆ ---         ┆ ---          │
│ str            ┆ i64   ┆ str           ┆ i64      ┆ str         ┆ str                   ┆ str                          ┆ f64        ┆ f64                ┆ f64         ┆ f64          │
╞════════════════╪═══════╪═══════════════╪══════════╪═════════════╪═══════════════════════╪══════════════════════════════╪════════════╪════════════════════╪═════════════╪══════════════╡
│ 1998-08-08     ┆ 3     ┆ YALLOURN      ┆ 4        ┆ V_ADMIN     ┆ ALL                   ┆ 2022-07-06                   ┆ -5.08651   ┆ -0.55494           ┆ -1.0        ┆ -1.0         │
│ 1998-08-08     ┆ 3     ┆ YALLOURN      ┆ 4        ┆ V_EST       ┆ ALL                   ┆ 2022-07-06                   ┆ -5.08651   ┆ -0.35758           ┆ -1.0        ┆ -1.0         │
│ 1998-08-08     ┆ 3     ┆ YALLOURN      ┆ 4        ┆ V_METERING  ┆ ALL                   ┆ 2022-07-06                   ┆ -5.08651   ┆ -0.04018           ┆ -1.0        ┆ -1.0         │
│ 1998-08-08     ┆ 3     ┆ YALLOURN      ┆ 4        ┆ V_NECA      ┆ ALL                   ┆ 2022-07-06                   ┆ -5.08651   ┆ -0.20143           ┆ -1.0        ┆ -1.0         │
│ …              ┆ …     ┆ …             ┆ …        ┆ …           ┆ …                     ┆ …                            ┆ …          ┆ …                  ┆ …           ┆ …            │
│ 1998-08-10     ┆ 3     ┆ YALLOURN      ┆ 17       ┆ V_SETT      ┆ ALL                   ┆ 2022-07-06                   ┆ 0.0        ┆ 0.0                ┆ -1.0        ┆ -1.0         │
│ 1998-08-10     ┆ 3     ┆ YALLOURN      ┆ 18       ┆ V_ADMIN     ┆ ALL                   ┆ 2022-07-06                   ┆ 0.0        ┆ 0.0                ┆ -1.0        ┆ -1.0         │
│ 1998-08-10     ┆ 3     ┆ YALLOURN      ┆ 18       ┆ V_EST       ┆ ALL                   ┆ 2022-07-06                   ┆ 0.0        ┆ 0.0                ┆ -1.0        ┆ -1.0         │
│ 1998-08-10     ┆ 3     ┆ YALLOURN      ┆ 18       ┆ V_METERING  ┆ ALL                   ┆ 2022-07-06                   ┆ 0.0        ┆ 0.0                ┆ -1.0        ┆ -1.0         │
└────────────────┴───────┴───────────────┴──────────┴─────────────┴───────────────────────┴──────────────────────────────┴────────────┴────────────────────┴─────────────┴──────────────┘
```

### Summarizing Results

To summarize the results of a table validation we can call a
`summarize_reconciliation` object with a summarization `join` method. The join
method can be either `left`, `right`,`inner` and `outer`. The join method will
specify how the summarization of the results is performed

```python
import dataclasses
from pprint import pprint
pprint(
    dataclasses.asdict(tables.summarize_reconciliation(validation_no_index, "left")),
    width=20,
    sort_dicts=False
)

```

outputs

```python
{'n_tested_rows': 10000,
 'n_tested_cols': 4,
 'n_tested_entries': 40000,
 'n_tested_entries_passed': 40000,
 'n_tested_entries_failed': 0,
 'n_tested_rows_passed': 10000,
 'n_tested_rows_passed_partially': 0,
 'n_tested_rows_failed': 0,
 'validation_ratio_entries': 1.0,
 'validation_ratio_rows': 1.0,
 'stats_invalidations_per_row_avg': 0.0,
 'stats_invalidations_per_row_std': 0.0,
 'n_total_rows_left': 10000,
 'n_total_rows_right': 10500,
 'n_total_rows_intersecting': 10000,
 'n_total_rows_union': 10500,
 'pass_ratio': 1}
```

To check whether or not the test has passed

```python
tables.summarize_reconciliation(validation_no_index, "left").PASS
```

returns a boolean

```python
True
```

we can call flag a string describing the outcome of the test

```python
tables.summarize_reconciliation(validation_no_index, "left").flag
```

returns

```python
'PASSED'
```
