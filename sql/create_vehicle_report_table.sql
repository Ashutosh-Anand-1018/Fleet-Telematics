-- Active: 1785918688226@@fleet-telematics-db-eng2.cdam0y0k6wsx.ap-south-1.rds.amazonaws.com@3306
CREATE DATABASE IF NOT EXISTS fleet_telematics;

USE fleet_telematics_eng2;

select DATABASE()

drop table VehicleHealthReport;
-- VehicleHealthReport table (UC-1: Vehicle Health Monitoring & Predictive Maintenance)
-- One row per vehicle (deviceID), aggregated from vehicle_health_analysis.py

CREATE TABLE IF NOT EXISTS VehicleHealthReport (
    deviceID                    INT PRIMARY KEY,
    total_readings                BIGINT,
    total_trips                    INT,
    high_rpm_count                 BIGINT,
    overheating_count               BIGINT,
    excessive_load_count            BIGINT,
    low_battery_count                BIGINT,
    dtc_count                         BIGINT,
    total_alert_count                 BIGINT,
    avg_cTemp                          DOUBLE,
    avg_eLoad                          DOUBLE,
    alerts_per_100_readings            DOUBLE,
    vehicle_health_score                DOUBLE,
    is_high_risk                         BOOLEAN,
    maintenance_recommendation           VARCHAR(100),
    risk_rank                             INT,
    processed_datestamp                    VARCHAR(20),
    loaded_at                               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- SELECT * FROM VehicleHealthReport ORDER BY risk_rank;
-- SELECT * FROM VehicleHealthReport WHERE is_high_risk = TRUE ORDER BY vehicle_health_score;

show TABLES;

select * from VehicleHealthReport;

select * from DriverPerformance;

select count(*) from VehicleHealthReport;