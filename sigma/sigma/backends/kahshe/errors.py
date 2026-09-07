"""One exception, and it is always raised rather than warned.

A detection that silently loses a clause is worse than one that will not convert: the rule
loads, reviews as correct, and never fires — the same failure the kahshe rule loader refuses
YAML booleans over (README, "Quote anything that looks like a boolean"). So every construct
kahshe cannot express stops the conversion and names itself.
"""

from sigma.exceptions import SigmaConversionError


class KahsheConversionError(SigmaConversionError):
    """A Sigma construct kahshe has no equivalent for.

    The message names the construct, the rule, and — where one exists — what to write instead,
    because the person reading it is holding a rule library they did not write.
    """

    def __init__(self, what: str, remedy: str = "", rule=None) -> None:
        message = f"kahshe cannot express {what}"
        if remedy:
            message += f"; {remedy}"
        super().__init__(message, source=getattr(rule, "source", None))
