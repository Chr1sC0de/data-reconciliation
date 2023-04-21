select
    marketfeeid,
    marketfeeperiod,
    marketfeetype,
    description,
    to_char (lastchanged, 'YYYY-MM-DD') as lastchanged,
    fee_class
from
    mms.marketfee
order by
    marketfeeid