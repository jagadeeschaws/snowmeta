"""default values of mparticle function"""

MP_DEFAULT_VALUES = {
    "MAX_RETRIES": 5,
    "BASE_WAIT_TIME": 2,
    "MAX_WAIT_TIME": 60,
    "QUEUE_SIZE": 50,
    "THREADS": 25,
    "CONNECTION_POOL_SIZE": 1,
    "BATCH_SIZE": 1000,
    "ASYNC_COROUTINE_BATCH_SIZE": 100,
    "ASYNC_PAYLOAD_BATCH_SIZE": 100,
    "ASYNC_CLIENT_TIMEOUT": 10,
    "ENDPOINT_URL": "https://s2s.mparticle.com/v2/bulkevents"
}

# DEFAULT_IDLE_TIMEOUT_MINS :
# Default duration in minutes for the session idle timeout.
# (it is set to 4 hours, aligning with the default value of "session_idle_timeout_mins" in Snowflake).

# DEFAULT_REFRESH_BEFORE_TIMEOUT_MINS:
# Default duration in minutes before a session timeout is reached when a session refresh occurs.
# (it is set to 1 hour, indicating that the session will be refreshed one hour prior to the timeout).

SF_TIMEOUT_DEFAULT_VALUES = {
    "DEFAULT_IDLE_TIMEOUT_MINS": 240,
    "DEFAULT_REFRESH_BEFORE_TIMEOUT_MINS": 60,
}
