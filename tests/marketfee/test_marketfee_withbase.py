import dbqq
import datarec

try:
    from .base import Base, cwd, schedule
except ImportError:
    from base import Base, cwd, schedule


class TestMarketFeeWithInheritance(Base):
    source_connection = dbqq.connectors.oracle.infoserver_ret().cache(
        **schedule
    )
    target_connection = dbqq.connectors.databricks.prod().cache(**schedule)

    left_table_name = "INFOSERVER_RETAIL"
    right_table_name = "DATABRICKS_PRODUCTION"

    def test_count(self):
        lf_1 = self.source_connection.from_file(cwd / "sql/row_count_q1.sql")
        lf_2 = self.target_connection.from_file(cwd / "sql/row_count_q2.sql")

        self._test_count(
            datarec.tables.is_equal(lf_1, lf_2, **self.recon_kwargs)
        )

    def test_is_equal(self):
        lf_1 = self.source_connection.from_file(cwd / "sql/is_equal_q1.sql")
        lf_2 = self.target_connection.from_file(cwd / "sql/is_equal_q2.sql")

        self._test_comparison(
            datarec.tables.is_equal(lf_1, lf_2, **self.recon_kwargs)
        )

    def test_duplicates(self):
        self._test_duplicates(
            "ea_edp_prod_infoserver_retail_raw.marketfee",
            self.target_connection(
                "select * from ea_edp_prod_infoserver_retail_raw.marketfee limit 1"
            ).columns,
        )


if __name__ == "__main__":
    T = TestMarketFeeWithInheritance()
    T.test_count()
    T.test_is_equal()
    T.test_duplicates()
