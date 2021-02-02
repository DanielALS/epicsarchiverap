drop table if exists ArchivePVRequests;

create table ArchivePVRequests (
    pvName VARCHAR(255) PRIMARY KEY NOT NULL,
    userSpecifiedSamplingMethod ENUM('SCAN', 'MONITOR') NOT NULL,
    skipAliasCheck ENUM('true', 'false') DEFAULT 'true',
    skipCapacityPlanning ENUM('true', 'false') DEFAULT 'false',
    userSpecifiedSamplingPeriod FLOAT NOT NULL,
    controllingPV VARCHAR(255) DEFAULT "",
    policyName VARCHAR(255) DEFAULT "",
    usePVAccess ENUM("true", "false") DEFAULT "false",
    alias VARCHAR(255) DEFAULT "",
    archiveFields VARCHAR(255) DEFAULT "",
    `last_modified` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)ENGINE=InnoDB;

drop table if exists PVTypeInfo;

create table PVTypeInfo (
    pvName VARCHAR(255) PRIMARY KEY,
    paused ENUM('true', 'false'),
    creationTime TIMESTAMP DEFAULT 0,
    lowerAlarmLimit DOUBLE DEFAULT 0,
    `precision` DOUBLE DEFAULT 0,
    lowerCtrlLimit DOUBLE DEFAULT 0,
    computedBytesPerEvent INT DEFAULT 0,
    computedEventRate DOUBLE DEFAULT 0,
    usePVAccess ENUM('true', 'false'),
    computedStorageRate DOUBLE,
    modificationTime TIMESTAMP DEFAULT 0,
    upperDisplayLimit DOUBLE DEFAULT 0,
    upperWarningLimit DOUBLE DEFAULT 0,
    DBRType VARCHAR(20),
    sts VARCHAR(255),
    mts VARCHAR(255),
    lts VARCHAR(255),
    upperAlarmLimit DOUBLE DEFAULT 0,
    userSpecifiedEventRate DOUBLE,
    policyName VARCHAR(255),
    useDBEProperties ENUM('true','false'),
    hasReducedDataSet ENUM('true', 'false'),
    lowerWarningLimit DOUBLE DEFAULT 0,
    applianceIdentity VARCHAR(255),
    scalar ENUM('true', 'false'),
    upperCtrlLimit DOUBLE DEFAULT 0,
    lowerDisplayLimit DOUBLE DEFAULT 0,
    samplingPeriod DOUBLE,
    elementCount INT,
    samplingMethod ENUM('SCAN', 'MONITOR'),
    rtype VARCHAR(45),
    mdel float DEFAULT 0,
    adel float DEFAULT 0,
    scan float DEFAULT 0,
    archive_fields VARCHAR(255),
    `last_modified` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)ENGINE=InnoDB DEFAULT CHARSET=latin1;
