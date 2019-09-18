from logging import Logger

from hypothesis import settings, HealthCheck


def register_profile():
    settings.register_profile("pyspark", deadline=None, suppress_health_check=HealthCheck.all(), max_examples=10)


def load_pyspark_profile():
    Logger("sparkle-hypothesis").warning("Switching hypothesis profile to 'pyspark'")
    settings.load_profile("pyspark")
