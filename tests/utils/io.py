def check_file_contents(path: str, content):
    with open(path) as f:
        assert f.read() == str(content)
