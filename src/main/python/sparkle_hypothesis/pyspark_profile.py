from hypothesis import settings, HealthCheck

settings.register_profile("pyspark", deadline=None, suppress_health_check=HealthCheck.all(), max_examples=10)


# noinspection PyProtectedMember
def load_pyspark_profile():
    if settings()._current_profile != 'pyspark':
        # Logger("sparkle-hypothesis").warning("Switching hypothesis profile to 'pyspark'")
        settings.load_profile("pyspark")


# noinspection PyProtectedMember
def load_default_profile():
    if settings()._current_profile != 'default':
        # Logger("sparkle-hypothesis").warning("Switching hypothesis profile to 'default'")
        settings.load_profile('default')
