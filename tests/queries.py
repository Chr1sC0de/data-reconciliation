import pathlib as pt
from triple_quote_clean import TripleQuoteCleaner


tqc = TripleQuoteCleaner(skip_top_lines=1)


query1 = (
    """--sql
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
"""
    >> tqc
)

query2 = (
    """--sql
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
"""
    >> tqc
)

pt.Path("reports").mkdir(exist_ok=True)
