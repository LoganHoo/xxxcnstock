## MODIFIED Requirements

### Requirement: Morning report supports debug mode
The system SHALL support a debug mode that outputs detailed path information and intermediate results for troubleshooting.

#### Scenario: Debug mode enabled
- **WHEN** the morning report workflow is triggered with debug_mode=true
- **THEN** it SHALL output detailed file paths, data statistics, and intermediate calculation results

#### Scenario: Debug mode disabled (default)
- **WHEN** the morning report workflow is triggered with debug_mode=false (default)
- **THEN** it SHALL execute normally without verbose debug output

#### Scenario: Debug output includes file paths
- **WHEN** debug mode is enabled
- **THEN** the output SHALL include full paths to all accessed files and generated reports

## REMOVED Requirements

### Requirement: Simple morning report variant
**Reason**: Functionality merged into main morning report with debug_mode parameter
**Migration**: Use xcnstock_morning_report with debug_mode=true instead of xcnstock_morning_report_simple
