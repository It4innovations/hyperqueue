class MissingPackageException(BaseException):
    def __init__(self, package: str):
        self.package = package

    def __str__(self):
        return f"Unable to import `{self.package}`. You have to install the `{self.package}` package."
