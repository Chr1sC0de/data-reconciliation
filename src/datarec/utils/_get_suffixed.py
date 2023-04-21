from ._set_case import SetCase


class GetSuffixed:
    suffixed_template = "%s ~%s~"

    def __init__(self, setcase: SetCase, lf_name: str):
        self.setcase = setcase
        self.lf_name = lf_name

    def __call__(self, col: str) -> str:
        return self.setcase(self.suffixed_template % (col, self.lf_name))
