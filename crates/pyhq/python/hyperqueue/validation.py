from typing import List


class ValidationException(BaseException):
    pass


def validate_args(args: List[str]):
    from .output import Output

    for arg in args:
        if not isinstance(arg, (str, Output)):
            raise ValidationException(
                "Each program argument must either be a string or an instance of `hq.Output`. "
                f"Argument `{arg}` has type `{type(arg)}`."
            )
