class SetCase:
    def __init__(self, column_case: str):
        assert column_case in [
            "upper",
            "lower",
        ], "case is either upper or lower"
        self.column_case = column_case

    def __call__(self, string: str) -> str:
        return getattr(string, self.column_case)()
