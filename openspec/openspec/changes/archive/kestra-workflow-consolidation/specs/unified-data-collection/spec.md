## ADDED Requirements

### Requirement: Unified data collection workflow supports multiple modes
The system SHALL provide a unified data collection workflow that supports standard collection, daily update, and GE validation modes through input parameters.

#### Scenario: Standard collection mode
- **WHEN** the unified workflow is triggered with mode="standard"
- **THEN** it SHALL execute data collection for the target date without updating stock list

#### Scenario: Daily update mode
- **WHEN** the unified workflow is triggered with mode="daily"
- **THEN** it SHALL first update the stock list, then collect data for all active stocks

#### Scenario: GE validation mode
- **WHEN** the unified workflow is triggered with mode="ge" or enable_ge_validation=true
- **THEN** it SHALL perform GE data quality validation after collection

### Requirement: Unified workflow accepts flexible date inputs
The system SHALL support both single date collection and date range backfill through input parameters.

#### Scenario: Single date collection
- **WHEN** only target_date is provided (start_date and end_date are empty)
- **THEN** it SHALL collect data for the specified target date only

#### Scenario: Date range backfill
- **WHEN** both start_date and end_date are provided
- **THEN** it SHALL collect data for all dates in the specified range

#### Scenario: Default to today
- **WHEN** no date parameters are provided
- **THEN** it SHALL default to collecting data for the current date

### Requirement: Unified workflow supports distributed locking
The system SHALL acquire a distributed lock before execution to prevent conflicts with APScheduler.

#### Scenario: Lock acquisition success
- **WHEN** the workflow starts and Redis is available
- **THEN** it SHALL acquire a distributed lock and proceed with execution

#### Scenario: Lock already held
- **WHEN** the workflow starts and the lock is already held by another scheduler
- **THEN** it SHALL skip execution and log a warning

#### Scenario: Redis unavailable
- **WHEN** the workflow starts and Redis is unavailable
- **THEN** it SHALL log a warning and continue in degraded mode

### Requirement: Unified workflow provides comprehensive reporting
The system SHALL generate a detailed collection report including success/failure counts and data freshness metrics.

#### Scenario: Collection report generation
- **WHEN** data collection completes
- **THEN** it SHALL generate a JSON report with total stocks, success count, failure count, and data freshness

#### Scenario: Notification on completion
- **WHEN** the workflow completes successfully
- **THEN** it SHALL send a notification with the collection summary
