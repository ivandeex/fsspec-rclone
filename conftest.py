def pytest_addoption(parser):
    parser.addoption("--remote", action="store")
    parser.addoption("--noclean", action="store_true")
