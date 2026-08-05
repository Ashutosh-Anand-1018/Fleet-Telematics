-- DriverPerformance table (UC-2: Driver Behavior & Fuel Efficiency)
-- One row per driver (deviceID), aggregated from the Databricks
-- driver_behavior_analysis.py output.

CREATE TABLE IF NOT EXISTS DriverPerformance (
    deviceID                INT PRIMARY KEY,
    total_readings           BIGINT,
    total_trips               INT,
    high_rpm_count            BIGINT,
    rapid_accel_count         BIGINT,
    hard_brake_count          BIGINT,
    aggressive_event_count    BIGINT,
    avg_kpl_while_moving      DOUBLE,
    avg_speed                 DOUBLE,
    avg_rpm                   DOUBLE,
    events_per_100_readings   DOUBLE,
    driver_safety_score       DOUBLE,
    fuel_efficiency_score     DOUBLE,
    driver_category           VARCHAR(20),
    driver_rank                INT,
    processed_datestamp       VARCHAR(20),
    loaded_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Quick sanity queries once loaded:
-- SELECT * FROM DriverPerformance ORDER BY driver_rank;
-- SELECT driver_category, COUNT(*) FROM DriverPerformance GROUP BY driver_category;
