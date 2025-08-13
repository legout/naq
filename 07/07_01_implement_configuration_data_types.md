### Sub-task 1: Implement Configuration Data Types
**Description:** Define the necessary data structures (dataclasses) for the NAQ configuration, including `NatsConfig`, `WorkerConfig`, `EventsConfig`, and `NAQConfig` in a new `types.py` file. These classes will represent the structured configuration data.
**Implementation Steps:**
- Create the file `src/naq/config/types.py`.
- Implement the `NatsConfig` dataclass with fields for `servers`, `client_name`, `max_reconnect_attempts`, `reconnect_time_wait`, `connection_timeout`, `drain_timeout`, and optional `auth` and `tls`.
- Implement the `WorkerConfig` dataclass with fields for `concurrency`, `heartbeat_interval`, `ttl`, `max_job_duration`, `shutdown_timeout`, and optional `pools`.
- Implement the `EventsConfig` dataclass with fields for `enabled`, `batch_size`, `flush_interval`, `max_buffer_size`, `stream`, and `filters`.
- Implement the `NAQConfig` dataclass, aggregating the other configuration dataclasses and including `queues`, `scheduler`, `results`, `serialization`, and `logging` fields. Add a `@property` for `environment` that reads from `os.getenv('NAQ_ENVIRONMENT')`.
**Success Criteria:**
- The `src/naq/config/types.py` file is created.
- All specified dataclasses (`NatsConfig`, `WorkerConfig`, `EventsConfig`, `NAQConfig`) are correctly defined with their respective fields and types.
- The `NAQConfig` dataclass includes the `environment` property.
**Testing:**
- Create unit tests for `types.py` to ensure dataclasses are correctly defined and can be instantiated.
- Verify that the `environment` property in `NAQConfig` correctly reads the `NAQ_ENVIRONMENT` environment variable.
**Documentation:**
- Add a section to the configuration documentation describing the structure of the `NAQConfig` and its nested components, referencing the `types.py` file.