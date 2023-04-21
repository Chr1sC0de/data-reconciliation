import pathlib as pt
import polars as pl
import jinja2
from datetime import datetime, timedelta
from triple_quote_clean import TripleQuoteCleaner
import datarec
import dbqq

tqc_sql = TripleQuoteCleaner(skip_top_lines=1)
tqc = TripleQuoteCleaner()

cwd = pt.Path(__file__).parent

jinja_environment = jinja2.Environment(
    loader=jinja2.FileSystemLoader(searchpath=[cwd / "../jinja_templates"]),
    trim_blocks=True,
    lstrip_blocks=True,
)

schedule = dict(date_lower_bound=datetime.now() - timedelta(days=1))

results_template = "results_template.md.j2"
summary_template = "summary_template.md.j2"
distinct_template = "distinct_template.md.j2"

distinct_sql_template = "distinct_template.sql.j2"


class TestMarketFeeExternal:
    source_connection = dbqq.connectors.oracle.infoserver_ret().cache(
        **schedule
    )
    target_connection = dbqq.connectors.databricks.prod().cache(**schedule)

    left_table_name = "INFOSERVER_RETAIL"
    right_table_name = "DATABRICKS_PRODUCTION"
    recon_kwargs = dict(pl1_name=left_table_name, pl2_name=right_table_name)

    def test_count(self):
        lf_1 = self.source_connection.from_file(cwd / "sql/row_count_q1.sql")
        lf_2 = self.target_connection.from_file(cwd / "sql/row_count_q2.sql")

        reconciliation_data = datarec.tables.is_equal(
            lf_1, lf_2, **self.recon_kwargs
        )
        summary_data = datarec.tables.summarize_reconciliation(
            reconciliation_data
        )

        doc = jinja_environment.get_template(results_template).render(
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
        lf_1 = self.source_connection.from_file(cwd / "sql/is_equal_q1.sql")
        lf_2 = self.target_connection.from_file(cwd / "sql/is_equal_q2.sql")

        reconciliation_data = datarec.tables.is_equal(
            lf_1, lf_2, **self.recon_kwargs
        )
        summary_data = datarec.tables.summarize_reconciliation(
            reconciliation_data
        )

        doc = jinja_environment.get_template(results_template).render(
            title="Direct Comparison",
            flag=summary_data.flag,
            left_name=self.left_table_name,
            left_query_tt=self.source_connection.query_info.time_taken,
            left_query=self.source_connection.query_info.query,
            right_name=self.right_table_name,
            right_query_tt=self.target_connection.query_info.time_taken,
            right_query=self.target_connection.query_info.query,
            results=jinja_environment.get_template(summary_template).render(
                summary=summary_data.get_string(),
                table=reconciliation_data.results.fetch(10)
                .to_pandas()
                .to_markdown(index=False),
            ),
        )

        print(doc)

        assert summary_data.PASS, doc

    def test_distinct(self):
        table_name = "ea_edp_prod_infoserver_retail_raw.marketfee"
        columns = self.target_connection(
            f"select * from {table_name} limit 1"
        ).columns

        query = (
            jinja_environment.get_template(distinct_sql_template)
            .render(table_name=table_name, columns=columns)
            .replace("\n\n", "\n")
        )

        lf_1 = self.target_connection(query)
        PASSED = bool(len(lf_1.fetch(5)) == 0)

        doc = jinja_environment.get_template(distinct_template).render(
            title="Check Distinct",
            flag="PASS" if PASSED else "FAILED",
            right_name=self.right_table_name,
            right_query_tt=self.target_connection.query_info.time_taken,
            right_query=self.target_connection.query_info.query,
            results=lf_1.fetch(5).to_pandas().to_markdown(index=False),
        )

        assert PASSED, doc


if __name__ == "__main__":
    T = TestMarketFeeExternal()
    T.test_distinct()
    T.test_count()
    T.test_is_equal()
