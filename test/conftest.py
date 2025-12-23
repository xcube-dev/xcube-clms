import os


def pytest_configure():
    os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
