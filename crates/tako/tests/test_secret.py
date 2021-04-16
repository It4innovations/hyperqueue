import time


def test_set_secret_key(tako_env):
    tako_env.create_secret_file("secret", "f0" * 32)
    tako_env.create_secret_file("secret2", "a0" * 32)
    session = tako_env.start(secret_file="secret")

    tako_env.start_worker(1, secret_file="../secret2")
    tako_env.start_worker(1, secret_file="../secret")

    time.sleep(0.3)
    assert len(session.overview()["workers"]) == 1

    tako_env.expect_worker_fail(0)


def test_invalid_secret_key(tako_env):
    # Key has to be long 32 bytes, ie. 64 chars in hex
    # Let us provide only 31 bytes
    tako_env.create_secret_file("secret", "f0" * 31)
    tako_env.start_server(secret_file="secret")
    time.sleep(0.1)
    tako_env.expect_server_fail()


def test_server_no_auth_worker_auth(tako_env):
    tako_env.create_secret_file("secret", "f0" * 32)
    session = tako_env.start()
    tako_env.start_worker(1, secret_file="../secret")
    time.sleep(0.3)
    assert len(session.overview()["workers"]) == 0
    tako_env.expect_worker_fail(0)


def test_server_auth_worker_no_auth(tako_env):
    tako_env.create_secret_file("secret", "f0" * 32)
    session = tako_env.start(secret_file="secret")
    tako_env.start_worker(1)
    time.sleep(0.3)
    assert len(session.overview()["workers"]) == 0
    tako_env.expect_worker_fail(0)