drop table if exists ArchivePVRequests;

create table ArchivePVRequests (
    pvName VARCHAR(255) PRIMARY KEY,
    samplingMethod ENUM('SCAN', 'MONITOR') NOT NULL,
    samplingPeriod FLOAT NOT NULL,
    controllingPV VARCHAR(255),
    policyName VARCHAR(255),
    usePVAccess ENUM("true", "false"),
    `last_modified` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)ENGINE=InnoDB;

drop table if exists PVTypeInfo;

create table PVTypeInfo (
    pvName VARCHAR(255) PRIMARY KEY,
    paused ENUM('true', 'false'),
    creationTime TIMESTAMP,
    lowerAlarmLimit DOUBLE,
    `precision` DOUBLE,
    lowerCtrlLimit DOUBLE,
    computedBytesPerEvent INT,
    computedEventRate DOUBLE,
    usePVAccess ENUM('true', 'false'),
    computedStorageRate DOUBLE,
    modificationTime TIMESTAMP,
    upperDisplayLimit DOUBLE,
    upperWarningLimit DOUBLE,
    DBRType VARCHAR(20),
    sts VARCHAR(255),
    mts VARCHAR(255),
    lts VARCHAR(255),
    upperAlarmLimit DOUBLE,
    userSpecifiedEventRate DOUBLE,
    policyName VARCHAR(255),
    useDBEProperties ENUM('true','false'),
    hasReducedDataSet ENUM('true', 'false'),
    lowerWarningLimit DOUBLE,
    applianceIdentity VARCHAR(255),
    scalar ENUM('true', 'false'),
    upperCtrlLimit DOUBLE,
    lowerDisplayLimit DOUBLE,
    samplingPeriod DOUBLE,
    elementCount INT,
    samplingMethod ENUM('SCAN', 'MONITOR'),
    rtype VARCHAR(45),
    mdel float,
    adel float,
    scan float,
    archive_fields VARCHAR(255),
    `last_modified` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)ENGINE=InnoDB DEFAULT CHARSET=latin1;
