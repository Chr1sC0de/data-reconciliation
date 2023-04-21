import polars as pl
import jinja2
from datetime import datetime, timedelta
from triple_quote_clean import TripleQuoteCleaner
import datarec
import dbqq

tqc_sql = TripleQuoteCleaner(skip_top_lines=1)
tqc = TripleQuoteCleaner()

schedule = dict(date_lower_bound=datetime.now() - timedelta(days=1))

jinja_environment = jinja2.Environment(trim_blocks=True, lstrip_blocks=True)

results_template = (
    tqc
    ** """
        ## {{title}} - {{flag}}

        ### Queries

        #### {{ left_name }}

        Time Taken: {{ left_query_tt }}

        ```sql
        {{ left_query }}
        ```

        #### {{right_name}}

        Time Taken: {{ right_query_tt }}

        ```sql
        {{right_query}}
        ```

        ### Results

        {{results}}
    """
    + "\n\n"
)

summary_template = (
    tqc
    << """
        #### Summary:
            {{summary}}

        #### Table (preview):

        {{table}}
    """
)

distinct_template = (
    tqc
    << """
        ## {{title}} - {{flag}}

        #### {{right_name}}

        Time Taken: {{ right_query_tt }}

        ```sql
        {{right_query}}
        ```

        ### Results (preview)

        {{results}}

    """
)


class TestMarketFeeInFile:
    source_connection = dbqq.connectors.oracle.infoserver_ret().cache(
        **schedule
    )
    target_connection = dbqq.connectors.databricks.prod().cache(**schedule)

    left_table_name = "INFOSERVER_RETAIL"
    right_table_name = "DATABRICKS_PRODUCTION"
    recon_kwargs = dict(pl1_name=left_table_name, pl2_name=right_table_name)

    def test_count(self):
        lf_1 = self.source_connection(
            tqc_sql
            << """--sql
            select
                count(*) as row_count
            from
                mms.marketfee
            """
        )
        lf_2 = self.target_connection(
            tqc_sql
            << """--sql
                select
                    count(*) as row_count
                from
                    ea_edp_prod_infoserver_retail_raw.marketfee
            """
        )

        reconciliation_data = datarec.tables.is_equal(
            lf_1, lf_2, **self.recon_kwargs
        )
        summary_data = datarec.tables.summarize_reconciliation(
            reconciliation_data
        )

        doc = jinja_environment.from_string(results_template).render(
            title="Row Count",
            flag=summary_data.flag,
            left_name=self.left_table_name,
            left_query_tt=self.source_connection.query_info.time_taken,
            left_query=self.source_connection.query_info.query,
            right_name=self.right_table_name,
            right_query_tt=self.target_connection.query_info.time_taken,
            right_query=self.target_connection.query_info.query,
            results=reconciliation_data.results.fetch()
            .select(
                pl.col(c)
                for c in reconciliation_data.results.columns
                if c.startswith(reconciliation_data.columns_tested[0])
            )
            .to_pandas()
            .to_markdown(index=False),
        )

        print(doc)

        assert summary_data.PASS, doc

    def test_is_equal(self):
        lf_1 = self.source_connection(
            tqc_sql
            << """--sql
            select
                marketfeeid,
                marketfeeperiod,
                marketfeetype,
                description,
                to_char(lastchanged, 'YYYY-MM-DD') as lastchanged,
                fee_class
            from mms.marketfee
            order by marketfeeid
            """
        )
        lf_2 = self.target_connection(
            tqc_sql
            << """--sql
            select
                marketfeeid,
                marketfeeperiod,
                marketfeetype,
                description,
                cast(date(lastchanged) as string) as lastchanged,
                fee_class
            from ea_edp_prod_infoserver_retail_raw.marketfee
            where active_ind = 'Y'
            order by marketfeeid
            """
        )

        reconciliation_data = datarec.tables.is_equal(
            lf_1, lf_2, **self.recon_kwargs
        )
        summary_data = datarec.tables.summarize_reconciliation(
            reconciliation_data
        )

        doc = jinja_environment.from_string(results_template).render(
            title="Direct Comparison",
            flag=summary_data.flag,
            left_name=self.left_table_name,
            left_query_tt=self.source_connection.query_info.time_taken,
            left_query=self.source_connection.query_info.query,
            right_name=self.right_table_name,
            right_query_tt=self.target_connection.query_info.time_taken,
            right_query=self.target_connection.query_info.query,
            results=jinja_environment.from_string(summary_template).render(
                summary=summary_data.get_string(),
                table=reconciliation_data.results.fetch(10)
                .to_pandas()
                .to_markdown(index=False),
            ),
        )

        print(doc)

        assert summary_data.PASS

    def test_distinct(self):
        columns = self.target_connection(
            "select * from ea_edp_prod_infoserver_retail_raw.marketfee limit 1"
        ).columns
        query = (
            jinja_environment.from_string(
                tqc_sql
                << """--sql
                {%macro get_columns(columns, stop="")%}
                    {% for col in columns %}
                    {% if loop.last%}
                    {{col}}{{stop}}
                {% else %}
                    {{col}},
                {% endif %}
                {% endfor %}
                {% endmacro %}
                select
                {{ get_columns(columns, stop=",") }}
                    count(*) as n_repetitions
                from
                    {{table_name}}
                group by
                {{ get_columns(columns) }}
                having
                    active_ind = 'Y'
                    and count(*) > 1
                """
            )
            .render(
                columns=columns,
                table_name="ea_edp_prod_infoserver_retail_raw.marketfee",
            )
            .replace("\n\n", "\n")
        )
        lf_1 = self.target_connection(query)
        PASSED = bool(len(lf_1.fetch(5)) == 0)

        doc = jinja_environment.from_string(distinct_template).render(
            title="Check Distinct",
            flag="PASS" if PASSED else "FAILED",
            right_name=self.right_table_name,
            right_query_tt=self.target_connection.query_info.time_taken,
            right_query=self.target_connection.query_info.query,
            results=lf_1.fetch(5).to_pandas().to_markdown(index=False),
        )
        print(doc)
        assert PASSED


if __name__ == "__main__":
    T = TestMarketFeeInFile()
    T.test_count()
    T.test_is_equal()
    T.test_distinct()
