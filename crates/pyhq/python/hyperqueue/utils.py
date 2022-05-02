def pluralize(text: str, count: int) -> str:
    if count == 1:
        return text
    return f"{text}s"


class MissingPackageException(BaseException):
    def __init__(self, package: str):
        self.package = package

    def __str__(self):
        return (
            f"Unable to import `{self.package}`. You have to install the "
            f"`{self.package}` package."
        )
