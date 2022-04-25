def pluralize(text: str, count: int) -> str:
    if count == 1:
        return text
    return f"{text}s"
