select
    marketfeeid,
    marketfeeperiod,
    marketfeetype,
    description,
    cast(date (lastchanged) as string) as lastchanged,
    fee_class
from
    ea_edp_prod_infoserver_retail_raw.marketfee
where
    active_ind = 'Y'
order by
    marketfeeid