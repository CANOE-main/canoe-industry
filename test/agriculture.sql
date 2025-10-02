PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE MetaData
(
    element TEXT PRIMARY KEY,
    value   INT,
    notes   TEXT
);
INSERT INTO MetaData VALUES('DB_MAJOR',3,'DB major version number');
INSERT INTO MetaData VALUES('DB_MINOR',1,'DB minor version number');
INSERT INTO MetaData VALUES('days_per_period',365,'count of days in each period');
CREATE TABLE MetaDataReal
(
    element TEXT PRIMARY KEY,
    value   REAL,
    notes   TEXT
);
INSERT INTO MetaDataReal VALUES('global_discount_rate',0.03,'Canadian social discount rate');
INSERT INTO MetaDataReal VALUES('default_loan_rate',0.03,'Matching GDR');
CREATE TABLE SeasonLabel
(
    season TEXT
        PRIMARY KEY,
    notes  TEXT
);
INSERT INTO SeasonLabel VALUES('D022','');
INSERT INTO SeasonLabel VALUES('D047','');
INSERT INTO SeasonLabel VALUES('D157','');
INSERT INTO SeasonLabel VALUES('D213','');
CREATE TABLE SectorLabel
(
    sector TEXT,
    notes  TEXT,
    PRIMARY KEY (sector)
);
CREATE TABLE CapacityCredit
(
    region  TEXT,
    period  INTEGER
        REFERENCES TimePeriod (period),
    tech    TEXT,
    vintage INTEGER,
    credit  REAL,
    notes   TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, period, tech, vintage),
    CHECK (credit >= 0 AND credit <= 1)
);
CREATE TABLE CapacityFactorProcess
(
    region  TEXT,
    period  INTEGER
        REFERENCES TimePeriod (period),
    season TEXT
        REFERENCES SeasonLabel (season),
    tod     TEXT
        REFERENCES TimeOfDay (tod),
    tech    TEXT,
    vintage INTEGER,
    factor  REAL,
    notes   TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, period, season, tod, tech, vintage),
    CHECK (factor >= 0 AND factor <= 1)
);
CREATE TABLE CapacityFactorTech
(
    region TEXT,
    period INTEGER
        REFERENCES TimePeriod (period),
    season TEXT
        REFERENCES SeasonLabel (season),
    tod    TEXT
        REFERENCES TimeOfDay (tod),
    tech   TEXT,
    factor REAL,
    notes  TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, period, season, tod, tech),
    CHECK (factor >= 0 AND factor <= 1)
);
CREATE TABLE CapacityToActivity
(
    region TEXT,
    tech   TEXT,
    c2a    REAL,
    notes  TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, tech)
);
CREATE TABLE Commodity
(
    name        TEXT,
    flag        TEXT
        REFERENCES CommodityType (label),
    description TEXT,
    data_id TEXT
        REFERENCES DataSet (data_id),
    PRIMARY KEY (name)
);
INSERT INTO Commodity VALUES('A_elc','s','Represents Electricity in the agriculture sector','AGRIHR1');
INSERT INTO Commodity VALUES('A_ng','s','Represents Natural Gas in the agriculture sector','AGRIHR1');
INSERT INTO Commodity VALUES('A_dsl','s','Represents Diesel in the agriculture sector','AGRIHR1');
INSERT INTO Commodity VALUES('A_gsl','s','Represents Gasoline in the agriculture sector','AGRIHR1');
INSERT INTO Commodity VALUES('A_d_agri','d','Demand for the Agriculture sector','AGRIHR1');
CREATE TABLE CommodityType
(
    label       TEXT PRIMARY KEY,
    description TEXT
);
INSERT INTO CommodityType VALUES('s','source commodity');
INSERT INTO CommodityType VALUES('a','annual commodity');
INSERT INTO CommodityType VALUES('p','physical commodity');
INSERT INTO CommodityType VALUES('d','demand commodity');
INSERT INTO CommodityType VALUES('e','emissions commodity');
INSERT INTO CommodityType VALUES('w','waste commodity');
INSERT INTO CommodityType VALUES('wa','waste annual commodity');
INSERT INTO CommodityType VALUES('wp','waste physical commodity');
CREATE TABLE ConstructionInput
(
    region      TEXT,
    input_comm   TEXT,
    tech        TEXT,
    vintage     INTEGER
        REFERENCES TimePeriod (period),
    value       REAL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    FOREIGN KEY (input_comm) REFERENCES Commodity (name),
    PRIMARY KEY (region, input_comm, tech, vintage)
);
CREATE TABLE CostEmission
(
    region    TEXT,
    period    INTEGER
        REFERENCES TimePeriod (period),
    emis_comm TEXT NOT NULL,
    cost      REAL NOT NULL,
    units     TEXT,
    notes     TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (emis_comm) REFERENCES Commodity (name),
    PRIMARY KEY (region, period, emis_comm)
);
CREATE TABLE CostFixed
(
    region  TEXT    NOT NULL,
    period  INTEGER NOT NULL
        REFERENCES TimePeriod (period),
    tech    TEXT    NOT NULL,
    vintage INTEGER NOT NULL
        REFERENCES TimePeriod (period),
    cost    REAL,
    units   TEXT,
    notes   TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, period, tech, vintage)
);
CREATE TABLE CostInvest
(
    region  TEXT,
    tech    TEXT,
    vintage INTEGER
        REFERENCES TimePeriod (period),
    cost    REAL,
    units   TEXT,
    notes   TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, tech, vintage)
);
CREATE TABLE CostVariable
(
    region  TEXT    NOT NULL,
    period  INTEGER NOT NULL
        REFERENCES TimePeriod (period),
    tech    TEXT    NOT NULL,
    vintage INTEGER NOT NULL
        REFERENCES TimePeriod (period),
    cost    REAL,
    units   TEXT,
    notes   TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, period, tech, vintage)
);
CREATE TABLE Demand
(
    region    TEXT,
    period    INTEGER
        REFERENCES TimePeriod (period),
    commodity TEXT,
    demand    REAL,
    units     TEXT,
    notes     TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (commodity) REFERENCES Commodity (name),
    PRIMARY KEY (region, period, commodity)
);
INSERT INTO Demand VALUES('AB',2025,'A_d_agri',62.099999999999999644,'PJ','Value from NRCan Comprehensive DB','[A1]',1,1,2,3,2,'AGRIHRAB1');
INSERT INTO Demand VALUES('AB',2030,'A_d_agri',68.617256270705171417,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRAB1');
INSERT INTO Demand VALUES('AB',2035,'A_d_agri',66.516045424711531808,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRAB1');
INSERT INTO Demand VALUES('AB',2040,'A_d_agri',64.529993602004704911,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRAB1');
INSERT INTO Demand VALUES('AB',2045,'A_d_agri',64.73664858859566884,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRAB1');
INSERT INTO Demand VALUES('AB',2050,'A_d_agri',65.898094125456170289,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRAB1');
INSERT INTO Demand VALUES('ON',2025,'A_d_agri',70.299999999999993605,'PJ','Value from NRCan Comprehensive DB','[A1]',1,1,2,3,2,'AGRIHRON1');
INSERT INTO Demand VALUES('ON',2030,'A_d_agri',77.677827952183138293,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRON1');
INSERT INTO Demand VALUES('ON',2035,'A_d_agri',75.299162533932699759,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRON1');
INSERT INTO Demand VALUES('ON',2040,'A_d_agri',73.050862322398248949,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRON1');
INSERT INTO Demand VALUES('ON',2045,'A_d_agri',73.284805084996378354,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRON1');
INSERT INTO Demand VALUES('ON',2050,'A_d_agri',74.599613800637172289,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRON1');
INSERT INTO Demand VALUES('BC',2025,'A_d_agri',32.0,'PJ','Value from NRCan Comprehensive DB','[A1]',1,1,2,3,2,'AGRIHRBC1');
INSERT INTO Demand VALUES('BC',2030,'A_d_agri',35.358328513084784638,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRBC1');
INSERT INTO Demand VALUES('BC',2035,'A_d_agri',34.27557896281431482,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRBC1');
INSERT INTO Demand VALUES('BC',2040,'A_d_agri',33.252170616169900441,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRBC1');
INSERT INTO Demand VALUES('BC',2045,'A_d_agri',33.35865949814913911,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRBC1');
INSERT INTO Demand VALUES('BC',2050,'A_d_agri',33.95714995192588681,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRBC1');
INSERT INTO Demand VALUES('MB',2025,'A_d_agri',26.100000000000003197,'PJ','Value from NRCan Comprehensive DB','[A1]',1,1,2,3,2,'AGRIHRMB1');
INSERT INTO Demand VALUES('MB',2030,'A_d_agri',28.83913669348477704,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRMB1');
INSERT INTO Demand VALUES('MB',2035,'A_d_agri',27.956019091545427635,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRMB1');
INSERT INTO Demand VALUES('MB',2040,'A_d_agri',27.121301658813576906,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRMB1');
INSERT INTO Demand VALUES('MB',2045,'A_d_agri',27.208156653177892891,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRMB1');
INSERT INTO Demand VALUES('MB',2050,'A_d_agri',27.696300429539553178,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRMB1');
INSERT INTO Demand VALUES('SK',2025,'A_d_agri',68.799999999999998934,'PJ','Value from NRCan Comprehensive DB','[A1]',1,1,2,3,2,'AGRIHRSK1');
INSERT INTO Demand VALUES('SK',2030,'A_d_agri',76.020406303132288527,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRSK1');
INSERT INTO Demand VALUES('SK',2035,'A_d_agri',73.692494770050771535,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRSK1');
INSERT INTO Demand VALUES('SK',2040,'A_d_agri',71.49216682476527751,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRSK1');
INSERT INTO Demand VALUES('SK',2045,'A_d_agri',71.721117921020631769,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRSK1');
INSERT INTO Demand VALUES('SK',2050,'A_d_agri',73.007872396640651757,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRSK1');
INSERT INTO Demand VALUES('QC',2025,'A_d_agri',32.099999999999999644,'PJ','Value from NRCan Comprehensive DB','[A1]',1,1,2,3,2,'AGRIHRQC1');
INSERT INTO Demand VALUES('QC',2030,'A_d_agri',35.468823289688176103,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRQC1');
INSERT INTO Demand VALUES('QC',2035,'A_d_agri',34.382690147073113884,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRQC1');
INSERT INTO Demand VALUES('QC',2040,'A_d_agri',33.356083649345427133,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRQC1');
INSERT INTO Demand VALUES('QC',2045,'A_d_agri',33.462905309080852589,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRQC1');
INSERT INTO Demand VALUES('QC',2050,'A_d_agri',34.06326604552565751,'PJ','Scaled by GDP growth from CER CEF','[A1][A2]',1,1,2,3,2,'AGRIHRQC1');
INSERT INTO Demand VALUES('PEI',2025,'A_d_agri',2.6869758812615951448,'PJ','Value from NRCan Comprehensive DB','[A1][A3]',1,1,2,3,2,'AGRIHRPEI1');
INSERT INTO Demand VALUES('PEI',2030,'A_d_agri',2.9689679973869683493,'PJ','Scaled by GDP growth from CER CEF','[A1][A2][A3]',1,1,2,3,2,'AGRIHRPEI1');
INSERT INTO Demand VALUES('PEI',2035,'A_d_agri',2.8780516871674809209,'PJ','Scaled by GDP growth from CER CEF','[A1][A2][A3]',1,1,2,3,2,'AGRIHRPEI1');
INSERT INTO Demand VALUES('PEI',2040,'A_d_agri',2.7921181389138758355,'PJ','Scaled by GDP growth from CER CEF','[A1][A2][A3]',1,1,2,3,2,'AGRIHRPEI1');
INSERT INTO Demand VALUES('PEI',2045,'A_d_agri',2.8010597969607736956,'PJ','Scaled by GDP growth from CER CEF','[A1][A2][A3]',1,1,2,3,2,'AGRIHRPEI1');
INSERT INTO Demand VALUES('PEI',2050,'A_d_agri',2.8513138411627556578,'PJ','Scaled by GDP growth from CER CEF','[A1][A2][A3]',1,1,2,3,2,'AGRIHRPEI1');
INSERT INTO Demand VALUES('NS',2025,'A_d_agri',2.8833395176252318847,'PJ','Value from NRCan Comprehensive DB','[A1][A3]',1,1,2,3,2,'AGRIHRNS1');
INSERT INTO Demand VALUES('NS',2030,'A_d_agri',3.1859395587172616437,'PJ','Scaled by GDP growth from CER CEF','[A1][A2][A3]',1,1,2,3,2,'AGRIHRNS1');
INSERT INTO Demand VALUES('NS',2035,'A_d_agri',3.088379103530205505,'PJ','Scaled by GDP growth from CER CEF','[A1][A2][A3]',1,1,2,3,2,'AGRIHRNS1');
INSERT INTO Demand VALUES('NS',2040,'A_d_agri',2.9961655495131008564,'PJ','Scaled by GDP growth from CER CEF','[A1][A2][A3]',1,1,2,3,2,'AGRIHRNS1');
INSERT INTO Demand VALUES('NS',2045,'A_d_agri',3.0057606620630528126,'PJ','Scaled by GDP growth from CER CEF','[A1][A2][A3]',1,1,2,3,2,'AGRIHRNS1');
INSERT INTO Demand VALUES('NS',2050,'A_d_agri',3.0596872613223013992,'PJ','Scaled by GDP growth from CER CEF','[A1][A2][A3]',1,1,2,3,2,'AGRIHRNS1');
INSERT INTO Demand VALUES('NB',2025,'A_d_agri',1.6610760667903523568,'PJ','Value from NRCan Comprehensive DB','[A1][A3]',1,1,2,3,2,'AGRIHRNB1');
INSERT INTO Demand VALUES('NB',2030,'A_d_agri',1.8354022892123762034,'PJ','Scaled by GDP growth from CER CEF','[A1][A2][A3]',1,1,2,3,2,'AGRIHRNB1');
INSERT INTO Demand VALUES('NB',2035,'A_d_agri',1.7791982465785545208,'PJ','Scaled by GDP growth from CER CEF','[A1][A2][A3]',1,1,2,3,2,'AGRIHRNB1');
INSERT INTO Demand VALUES('NB',2040,'A_d_agri',1.7260745243546631755,'PJ','Scaled by GDP growth from CER CEF','[A1][A2][A3]',1,1,2,3,2,'AGRIHRNB1');
INSERT INTO Demand VALUES('NB',2045,'A_d_agri',1.7316022160182562572,'PJ','Scaled by GDP growth from CER CEF','[A1][A2][A3]',1,1,2,3,2,'AGRIHRNB1');
INSERT INTO Demand VALUES('NB',2050,'A_d_agri',1.7626690337986015144,'PJ','Scaled by GDP growth from CER CEF','[A1][A2][A3]',1,1,2,3,2,'AGRIHRNB1');
INSERT INTO Demand VALUES('NLLAB',2025,'A_d_agri',0.86860853432281999175,'PJ','Value from NRCan Comprehensive DB','[A1][A3]',1,1,2,3,2,'AGRIHRNLLAB1');
INSERT INTO Demand VALUES('NLLAB',2030,'A_d_agri',0.95976705955797978475,'PJ','Scaled by GDP growth from CER CEF','[A1][A2][A3]',1,1,2,3,2,'AGRIHRNLLAB1');
INSERT INTO Demand VALUES('NLLAB',2035,'A_d_agri',0.93037688768613193701,'PJ','Scaled by GDP growth from CER CEF','[A1][A2][A3]',1,1,2,3,2,'AGRIHRNLLAB1');
INSERT INTO Demand VALUES('NLLAB',2040,'A_d_agri',0.90259747443636495489,'PJ','Scaled by GDP growth from CER CEF','[A1][A2][A3]',1,1,2,3,2,'AGRIHRNLLAB1');
INSERT INTO Demand VALUES('NLLAB',2045,'A_d_agri',0.90548801042691700047,'PJ','Scaled by GDP growth from CER CEF','[A1][A2][A3]',1,1,2,3,2,'AGRIHRNLLAB1');
INSERT INTO Demand VALUES('NLLAB',2050,'A_d_agri',0.92173344529758001186,'PJ','Scaled by GDP growth from CER CEF','[A1][A2][A3]',1,1,2,3,2,'AGRIHRNLLAB1');
CREATE TABLE DemandSpecificDistribution
(
    region      TEXT,
    period      INTEGER
        REFERENCES TimePeriod (period),
    season TEXT
        REFERENCES SeasonLabel (season),
    tod         TEXT
        REFERENCES TimeOfDay (tod),
    demand_name TEXT,
    dsd         REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (demand_name) REFERENCES Commodity (name),
    PRIMARY KEY (region, period, season, tod, demand_name),
    CHECK (dsd >= 0 AND dsd <= 1)
);
CREATE TABLE EndOfLifeOutput
(
    region      TEXT,
    tech        TEXT,
    vintage     INTEGER
        REFERENCES TimePeriod (period),
    output_comm   TEXT,
    value       REAL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    FOREIGN KEY (output_comm) REFERENCES Commodity (name),
    PRIMARY KEY (region, tech, vintage, output_comm)
);
CREATE TABLE Efficiency
(
    region      TEXT,
    input_comm  TEXT,
    tech        TEXT,
    vintage     INTEGER
        REFERENCES TimePeriod (period),
    output_comm TEXT,
    efficiency  REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    FOREIGN KEY (input_comm) REFERENCES Commodity (name),
    FOREIGN KEY (output_comm) REFERENCES Commodity (name),
    PRIMARY KEY (region, input_comm, tech, vintage, output_comm),
    CHECK (efficiency > 0)
);
INSERT INTO Efficiency VALUES('AB','A_elc','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRAB1');
INSERT INTO Efficiency VALUES('AB','A_ng','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRAB1');
INSERT INTO Efficiency VALUES('AB','A_dsl','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRAB1');
INSERT INTO Efficiency VALUES('AB','A_gsl','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRAB1');
INSERT INTO Efficiency VALUES('AB','A_elc','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRAB1');
INSERT INTO Efficiency VALUES('AB','A_ng','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRAB1');
INSERT INTO Efficiency VALUES('AB','A_dsl','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRAB1');
INSERT INTO Efficiency VALUES('AB','A_gsl','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRAB1');
INSERT INTO Efficiency VALUES('AB','A_elc','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRAB1');
INSERT INTO Efficiency VALUES('AB','A_ng','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRAB1');
INSERT INTO Efficiency VALUES('AB','A_dsl','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRAB1');
INSERT INTO Efficiency VALUES('AB','A_gsl','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRAB1');
INSERT INTO Efficiency VALUES('AB','A_elc','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRAB1');
INSERT INTO Efficiency VALUES('AB','A_ng','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRAB1');
INSERT INTO Efficiency VALUES('AB','A_dsl','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRAB1');
INSERT INTO Efficiency VALUES('AB','A_gsl','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRAB1');
INSERT INTO Efficiency VALUES('AB','A_elc','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRAB1');
INSERT INTO Efficiency VALUES('AB','A_ng','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRAB1');
INSERT INTO Efficiency VALUES('AB','A_dsl','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRAB1');
INSERT INTO Efficiency VALUES('AB','A_gsl','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRAB1');
INSERT INTO Efficiency VALUES('AB','A_elc','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRAB1');
INSERT INTO Efficiency VALUES('AB','A_ng','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRAB1');
INSERT INTO Efficiency VALUES('AB','A_dsl','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRAB1');
INSERT INTO Efficiency VALUES('AB','A_gsl','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRAB1');
INSERT INTO Efficiency VALUES('ON','A_elc','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRON1');
INSERT INTO Efficiency VALUES('ON','A_ng','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRON1');
INSERT INTO Efficiency VALUES('ON','A_dsl','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRON1');
INSERT INTO Efficiency VALUES('ON','A_gsl','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRON1');
INSERT INTO Efficiency VALUES('ON','A_elc','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRON1');
INSERT INTO Efficiency VALUES('ON','A_ng','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRON1');
INSERT INTO Efficiency VALUES('ON','A_dsl','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRON1');
INSERT INTO Efficiency VALUES('ON','A_gsl','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRON1');
INSERT INTO Efficiency VALUES('ON','A_elc','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRON1');
INSERT INTO Efficiency VALUES('ON','A_ng','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRON1');
INSERT INTO Efficiency VALUES('ON','A_dsl','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRON1');
INSERT INTO Efficiency VALUES('ON','A_gsl','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRON1');
INSERT INTO Efficiency VALUES('ON','A_elc','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRON1');
INSERT INTO Efficiency VALUES('ON','A_ng','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRON1');
INSERT INTO Efficiency VALUES('ON','A_dsl','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRON1');
INSERT INTO Efficiency VALUES('ON','A_gsl','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRON1');
INSERT INTO Efficiency VALUES('ON','A_elc','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRON1');
INSERT INTO Efficiency VALUES('ON','A_ng','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRON1');
INSERT INTO Efficiency VALUES('ON','A_dsl','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRON1');
INSERT INTO Efficiency VALUES('ON','A_gsl','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRON1');
INSERT INTO Efficiency VALUES('ON','A_elc','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRON1');
INSERT INTO Efficiency VALUES('ON','A_ng','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRON1');
INSERT INTO Efficiency VALUES('ON','A_dsl','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRON1');
INSERT INTO Efficiency VALUES('ON','A_gsl','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRON1');
INSERT INTO Efficiency VALUES('BC','A_elc','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRBC1');
INSERT INTO Efficiency VALUES('BC','A_ng','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRBC1');
INSERT INTO Efficiency VALUES('BC','A_dsl','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRBC1');
INSERT INTO Efficiency VALUES('BC','A_gsl','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRBC1');
INSERT INTO Efficiency VALUES('BC','A_elc','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRBC1');
INSERT INTO Efficiency VALUES('BC','A_ng','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRBC1');
INSERT INTO Efficiency VALUES('BC','A_dsl','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRBC1');
INSERT INTO Efficiency VALUES('BC','A_gsl','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRBC1');
INSERT INTO Efficiency VALUES('BC','A_elc','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRBC1');
INSERT INTO Efficiency VALUES('BC','A_ng','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRBC1');
INSERT INTO Efficiency VALUES('BC','A_dsl','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRBC1');
INSERT INTO Efficiency VALUES('BC','A_gsl','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRBC1');
INSERT INTO Efficiency VALUES('BC','A_elc','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRBC1');
INSERT INTO Efficiency VALUES('BC','A_ng','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRBC1');
INSERT INTO Efficiency VALUES('BC','A_dsl','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRBC1');
INSERT INTO Efficiency VALUES('BC','A_gsl','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRBC1');
INSERT INTO Efficiency VALUES('BC','A_elc','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRBC1');
INSERT INTO Efficiency VALUES('BC','A_ng','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRBC1');
INSERT INTO Efficiency VALUES('BC','A_dsl','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRBC1');
INSERT INTO Efficiency VALUES('BC','A_gsl','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRBC1');
INSERT INTO Efficiency VALUES('BC','A_elc','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRBC1');
INSERT INTO Efficiency VALUES('BC','A_ng','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRBC1');
INSERT INTO Efficiency VALUES('BC','A_dsl','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRBC1');
INSERT INTO Efficiency VALUES('BC','A_gsl','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRBC1');
INSERT INTO Efficiency VALUES('MB','A_elc','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRMB1');
INSERT INTO Efficiency VALUES('MB','A_ng','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRMB1');
INSERT INTO Efficiency VALUES('MB','A_dsl','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRMB1');
INSERT INTO Efficiency VALUES('MB','A_gsl','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRMB1');
INSERT INTO Efficiency VALUES('MB','A_elc','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRMB1');
INSERT INTO Efficiency VALUES('MB','A_ng','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRMB1');
INSERT INTO Efficiency VALUES('MB','A_dsl','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRMB1');
INSERT INTO Efficiency VALUES('MB','A_gsl','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRMB1');
INSERT INTO Efficiency VALUES('MB','A_elc','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRMB1');
INSERT INTO Efficiency VALUES('MB','A_ng','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRMB1');
INSERT INTO Efficiency VALUES('MB','A_dsl','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRMB1');
INSERT INTO Efficiency VALUES('MB','A_gsl','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRMB1');
INSERT INTO Efficiency VALUES('MB','A_elc','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRMB1');
INSERT INTO Efficiency VALUES('MB','A_ng','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRMB1');
INSERT INTO Efficiency VALUES('MB','A_dsl','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRMB1');
INSERT INTO Efficiency VALUES('MB','A_gsl','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRMB1');
INSERT INTO Efficiency VALUES('MB','A_elc','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRMB1');
INSERT INTO Efficiency VALUES('MB','A_ng','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRMB1');
INSERT INTO Efficiency VALUES('MB','A_dsl','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRMB1');
INSERT INTO Efficiency VALUES('MB','A_gsl','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRMB1');
INSERT INTO Efficiency VALUES('MB','A_elc','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRMB1');
INSERT INTO Efficiency VALUES('MB','A_ng','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRMB1');
INSERT INTO Efficiency VALUES('MB','A_dsl','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRMB1');
INSERT INTO Efficiency VALUES('MB','A_gsl','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRMB1');
INSERT INTO Efficiency VALUES('SK','A_elc','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRSK1');
INSERT INTO Efficiency VALUES('SK','A_ng','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRSK1');
INSERT INTO Efficiency VALUES('SK','A_dsl','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRSK1');
INSERT INTO Efficiency VALUES('SK','A_gsl','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRSK1');
INSERT INTO Efficiency VALUES('SK','A_elc','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRSK1');
INSERT INTO Efficiency VALUES('SK','A_ng','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRSK1');
INSERT INTO Efficiency VALUES('SK','A_dsl','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRSK1');
INSERT INTO Efficiency VALUES('SK','A_gsl','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRSK1');
INSERT INTO Efficiency VALUES('SK','A_elc','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRSK1');
INSERT INTO Efficiency VALUES('SK','A_ng','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRSK1');
INSERT INTO Efficiency VALUES('SK','A_dsl','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRSK1');
INSERT INTO Efficiency VALUES('SK','A_gsl','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRSK1');
INSERT INTO Efficiency VALUES('SK','A_elc','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRSK1');
INSERT INTO Efficiency VALUES('SK','A_ng','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRSK1');
INSERT INTO Efficiency VALUES('SK','A_dsl','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRSK1');
INSERT INTO Efficiency VALUES('SK','A_gsl','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRSK1');
INSERT INTO Efficiency VALUES('SK','A_elc','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRSK1');
INSERT INTO Efficiency VALUES('SK','A_ng','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRSK1');
INSERT INTO Efficiency VALUES('SK','A_dsl','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRSK1');
INSERT INTO Efficiency VALUES('SK','A_gsl','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRSK1');
INSERT INTO Efficiency VALUES('SK','A_elc','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRSK1');
INSERT INTO Efficiency VALUES('SK','A_ng','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRSK1');
INSERT INTO Efficiency VALUES('SK','A_dsl','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRSK1');
INSERT INTO Efficiency VALUES('SK','A_gsl','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRSK1');
INSERT INTO Efficiency VALUES('QC','A_elc','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRQC1');
INSERT INTO Efficiency VALUES('QC','A_ng','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRQC1');
INSERT INTO Efficiency VALUES('QC','A_dsl','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRQC1');
INSERT INTO Efficiency VALUES('QC','A_gsl','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRQC1');
INSERT INTO Efficiency VALUES('QC','A_elc','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRQC1');
INSERT INTO Efficiency VALUES('QC','A_ng','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRQC1');
INSERT INTO Efficiency VALUES('QC','A_dsl','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRQC1');
INSERT INTO Efficiency VALUES('QC','A_gsl','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRQC1');
INSERT INTO Efficiency VALUES('QC','A_elc','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRQC1');
INSERT INTO Efficiency VALUES('QC','A_ng','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRQC1');
INSERT INTO Efficiency VALUES('QC','A_dsl','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRQC1');
INSERT INTO Efficiency VALUES('QC','A_gsl','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRQC1');
INSERT INTO Efficiency VALUES('QC','A_elc','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRQC1');
INSERT INTO Efficiency VALUES('QC','A_ng','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRQC1');
INSERT INTO Efficiency VALUES('QC','A_dsl','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRQC1');
INSERT INTO Efficiency VALUES('QC','A_gsl','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRQC1');
INSERT INTO Efficiency VALUES('QC','A_elc','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRQC1');
INSERT INTO Efficiency VALUES('QC','A_ng','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRQC1');
INSERT INTO Efficiency VALUES('QC','A_dsl','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRQC1');
INSERT INTO Efficiency VALUES('QC','A_gsl','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRQC1');
INSERT INTO Efficiency VALUES('QC','A_elc','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRQC1');
INSERT INTO Efficiency VALUES('QC','A_ng','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRQC1');
INSERT INTO Efficiency VALUES('QC','A_dsl','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRQC1');
INSERT INTO Efficiency VALUES('QC','A_gsl','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRQC1');
INSERT INTO Efficiency VALUES('PEI','A_elc','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRPEI1');
INSERT INTO Efficiency VALUES('PEI','A_dsl','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRPEI1');
INSERT INTO Efficiency VALUES('PEI','A_gsl','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRPEI1');
INSERT INTO Efficiency VALUES('PEI','A_elc','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRPEI1');
INSERT INTO Efficiency VALUES('PEI','A_dsl','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRPEI1');
INSERT INTO Efficiency VALUES('PEI','A_gsl','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRPEI1');
INSERT INTO Efficiency VALUES('PEI','A_elc','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRPEI1');
INSERT INTO Efficiency VALUES('PEI','A_dsl','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRPEI1');
INSERT INTO Efficiency VALUES('PEI','A_gsl','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRPEI1');
INSERT INTO Efficiency VALUES('PEI','A_elc','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRPEI1');
INSERT INTO Efficiency VALUES('PEI','A_dsl','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRPEI1');
INSERT INTO Efficiency VALUES('PEI','A_gsl','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRPEI1');
INSERT INTO Efficiency VALUES('PEI','A_elc','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRPEI1');
INSERT INTO Efficiency VALUES('PEI','A_dsl','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRPEI1');
INSERT INTO Efficiency VALUES('PEI','A_gsl','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRPEI1');
INSERT INTO Efficiency VALUES('PEI','A_elc','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRPEI1');
INSERT INTO Efficiency VALUES('PEI','A_dsl','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRPEI1');
INSERT INTO Efficiency VALUES('PEI','A_gsl','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRPEI1');
INSERT INTO Efficiency VALUES('NS','A_elc','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNS1');
INSERT INTO Efficiency VALUES('NS','A_dsl','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNS1');
INSERT INTO Efficiency VALUES('NS','A_gsl','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNS1');
INSERT INTO Efficiency VALUES('NS','A_elc','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNS1');
INSERT INTO Efficiency VALUES('NS','A_dsl','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNS1');
INSERT INTO Efficiency VALUES('NS','A_gsl','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNS1');
INSERT INTO Efficiency VALUES('NS','A_elc','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNS1');
INSERT INTO Efficiency VALUES('NS','A_dsl','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNS1');
INSERT INTO Efficiency VALUES('NS','A_gsl','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNS1');
INSERT INTO Efficiency VALUES('NS','A_elc','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNS1');
INSERT INTO Efficiency VALUES('NS','A_dsl','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNS1');
INSERT INTO Efficiency VALUES('NS','A_gsl','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNS1');
INSERT INTO Efficiency VALUES('NS','A_elc','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNS1');
INSERT INTO Efficiency VALUES('NS','A_dsl','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNS1');
INSERT INTO Efficiency VALUES('NS','A_gsl','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNS1');
INSERT INTO Efficiency VALUES('NS','A_elc','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNS1');
INSERT INTO Efficiency VALUES('NS','A_dsl','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNS1');
INSERT INTO Efficiency VALUES('NS','A_gsl','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNS1');
INSERT INTO Efficiency VALUES('NB','A_elc','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNB1');
INSERT INTO Efficiency VALUES('NB','A_dsl','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNB1');
INSERT INTO Efficiency VALUES('NB','A_gsl','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNB1');
INSERT INTO Efficiency VALUES('NB','A_elc','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNB1');
INSERT INTO Efficiency VALUES('NB','A_dsl','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNB1');
INSERT INTO Efficiency VALUES('NB','A_gsl','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNB1');
INSERT INTO Efficiency VALUES('NB','A_elc','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNB1');
INSERT INTO Efficiency VALUES('NB','A_dsl','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNB1');
INSERT INTO Efficiency VALUES('NB','A_gsl','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNB1');
INSERT INTO Efficiency VALUES('NB','A_elc','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNB1');
INSERT INTO Efficiency VALUES('NB','A_dsl','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNB1');
INSERT INTO Efficiency VALUES('NB','A_gsl','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNB1');
INSERT INTO Efficiency VALUES('NB','A_elc','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNB1');
INSERT INTO Efficiency VALUES('NB','A_dsl','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNB1');
INSERT INTO Efficiency VALUES('NB','A_gsl','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNB1');
INSERT INTO Efficiency VALUES('NB','A_elc','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNB1');
INSERT INTO Efficiency VALUES('NB','A_dsl','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNB1');
INSERT INTO Efficiency VALUES('NB','A_gsl','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNB1');
INSERT INTO Efficiency VALUES('NLLAB','A_elc','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNLLAB1');
INSERT INTO Efficiency VALUES('NLLAB','A_dsl','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNLLAB1');
INSERT INTO Efficiency VALUES('NLLAB','A_gsl','A_AGRI',2025,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNLLAB1');
INSERT INTO Efficiency VALUES('NLLAB','A_elc','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNLLAB1');
INSERT INTO Efficiency VALUES('NLLAB','A_dsl','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNLLAB1');
INSERT INTO Efficiency VALUES('NLLAB','A_gsl','A_AGRI',2030,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNLLAB1');
INSERT INTO Efficiency VALUES('NLLAB','A_elc','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNLLAB1');
INSERT INTO Efficiency VALUES('NLLAB','A_dsl','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNLLAB1');
INSERT INTO Efficiency VALUES('NLLAB','A_gsl','A_AGRI',2035,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNLLAB1');
INSERT INTO Efficiency VALUES('NLLAB','A_elc','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNLLAB1');
INSERT INTO Efficiency VALUES('NLLAB','A_dsl','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNLLAB1');
INSERT INTO Efficiency VALUES('NLLAB','A_gsl','A_AGRI',2040,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNLLAB1');
INSERT INTO Efficiency VALUES('NLLAB','A_elc','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNLLAB1');
INSERT INTO Efficiency VALUES('NLLAB','A_dsl','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNLLAB1');
INSERT INTO Efficiency VALUES('NLLAB','A_gsl','A_AGRI',2045,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNLLAB1');
INSERT INTO Efficiency VALUES('NLLAB','A_elc','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNLLAB1');
INSERT INTO Efficiency VALUES('NLLAB','A_dsl','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNLLAB1');
INSERT INTO Efficiency VALUES('NLLAB','A_gsl','A_AGRI',2050,'A_d_agri',1.0,'All technologies assumed efficiency=1; commodities from NRCan Comp DB','[A1]','','','','','','AGRIHRNLLAB1');
CREATE TABLE EfficiencyVariable
(
    region      TEXT,
    period      INTEGER
        REFERENCES TimePeriod (period),
    season TEXT
        REFERENCES SeasonLabel (season),
    tod         TEXT
        REFERENCES TimeOfDay (tod),
    input_comm  TEXT,
    tech        TEXT,
    vintage     INTEGER
        REFERENCES TimePeriod (period),
    output_comm TEXT,
    efficiency  REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    FOREIGN KEY (input_comm) REFERENCES Commodity (name),
    FOREIGN KEY (output_comm) REFERENCES Commodity (name),
    PRIMARY KEY (region, period, season, tod, input_comm, tech, vintage, output_comm),
    CHECK (efficiency > 0)
);
CREATE TABLE EmissionActivity
(
    region      TEXT,
    emis_comm   TEXT,
    input_comm  TEXT,
    tech        TEXT,
    vintage     INTEGER
        REFERENCES TimePeriod (period),
    output_comm TEXT,
    activity    REAL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    FOREIGN KEY (emis_comm) REFERENCES Commodity (name),
    FOREIGN KEY (input_comm) REFERENCES Commodity (name),
    FOREIGN KEY (output_comm) REFERENCES Commodity (name),
    PRIMARY KEY (region, emis_comm, input_comm, tech, vintage, output_comm)
);
CREATE TABLE EmissionEmbodied
(
    region      TEXT,
    emis_comm   TEXT,
    tech        TEXT,
    vintage     INTEGER
        REFERENCES TimePeriod (period),
    value       REAL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    FOREIGN KEY (emis_comm) REFERENCES Commodity (name),
    PRIMARY KEY (region, emis_comm,  tech, vintage)
);
CREATE TABLE EmissionEndOfLife
(
    region      TEXT,
    emis_comm   TEXT,
    tech        TEXT,
    vintage     INTEGER
        REFERENCES TimePeriod (period),
    value       REAL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    FOREIGN KEY (emis_comm) REFERENCES Commodity (name),
    PRIMARY KEY (region, emis_comm,  tech, vintage)
);
CREATE TABLE ExistingCapacity
(
    region   TEXT,
    tech     TEXT,
    vintage  INTEGER
        REFERENCES TimePeriod (period),
    capacity REAL,
    units    TEXT,
    notes    TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, tech, vintage)
);
CREATE TABLE TechGroup
(
    group_name TEXT,
    notes      TEXT,
    data_id TEXT
        REFERENCES DataSet (data_id),
    PRIMARY KEY (group_name)
);
CREATE TABLE LoanLifetimeProcess
(
    region   TEXT,
    tech     TEXT,
    vintage  INTEGER
        REFERENCES TimePeriod (period),
    lifetime REAL,
    notes    TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, tech, vintage)
);
CREATE TABLE LoanRate
(
    region  TEXT,
    tech    TEXT,
    vintage INTEGER
        REFERENCES TimePeriod (period),
    rate    REAL,
    notes   TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, tech, vintage)
);
CREATE TABLE LifetimeProcess
(
    region   TEXT,
    tech     TEXT,
    vintage  INTEGER
        REFERENCES TimePeriod (period),
    lifetime REAL,
    notes    TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, tech, vintage)
);
CREATE TABLE LifetimeTech
(
    region   TEXT,
    tech     TEXT,
    lifetime REAL,
    notes    TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, tech)
);
CREATE TABLE Operator
(
	operator TEXT PRIMARY KEY,
	notes TEXT
);
INSERT INTO Operator VALUES('e','equal to');
INSERT INTO Operator VALUES('le','less than or equal to');
INSERT INTO Operator VALUES('ge','greater than or equal to');
CREATE TABLE OutputDualVariable
(
    scenario        TEXT,
    constraint_name TEXT,
    dual            REAL,
    PRIMARY KEY (constraint_name, scenario)
);
CREATE TABLE OutputObjective
(
    scenario          TEXT,
    objective_name    TEXT,
    total_system_cost REAL
);
CREATE TABLE OutputCurtailment
(
    scenario    TEXT,
    region      TEXT,
    sector      TEXT,
    period      INTEGER
        REFERENCES TimePeriod (period),
    season      TEXT
        REFERENCES TimePeriod (period),
    tod         TEXT
        REFERENCES TimeOfDay (tod),
    input_comm  TEXT
        REFERENCES Commodity (name),
    tech        TEXT
        REFERENCES Technology (tech),
    vintage     INTEGER
        REFERENCES TimePeriod (period),
    output_comm TEXT
        REFERENCES Commodity (name),
    curtailment REAL,
    PRIMARY KEY (region, scenario, period, season, tod, input_comm, tech, vintage, output_comm)
);
CREATE TABLE OutputNetCapacity
(
    scenario TEXT,
    region   TEXT,
    sector   TEXT
        REFERENCES SectorLabel (sector),
    period   INTEGER
        REFERENCES TimePeriod (period),
    tech     TEXT
        REFERENCES Technology (tech),
    vintage  INTEGER
        REFERENCES TimePeriod (period),
    capacity REAL,
    PRIMARY KEY (region, scenario, period, tech, vintage)
);
CREATE TABLE OutputBuiltCapacity
(
    scenario TEXT,
    region   TEXT,
    sector   TEXT
        REFERENCES SectorLabel (sector),
    tech     TEXT
        REFERENCES Technology (tech),
    vintage  INTEGER
        REFERENCES TimePeriod (period),
    capacity REAL,
    PRIMARY KEY (region, scenario, tech, vintage)
);
CREATE TABLE OutputRetiredCapacity
(
    scenario TEXT,
    region   TEXT,
    sector   TEXT
        REFERENCES SectorLabel (sector),
    period   INTEGER
        REFERENCES TimePeriod (period),
    tech     TEXT
        REFERENCES Technology (tech),
    vintage  INTEGER
        REFERENCES TimePeriod (period),
    cap_eol REAL,
    cap_early REAL,
    PRIMARY KEY (region, scenario, period, tech, vintage)
);
CREATE TABLE OutputFlowIn
(
    scenario    TEXT,
    region      TEXT,
    sector      TEXT
        REFERENCES SectorLabel (sector),
    period      INTEGER
        REFERENCES TimePeriod (period),
    season TEXT
        REFERENCES SeasonLabel (season),
    tod         TEXT
        REFERENCES TimeOfDay (tod),
    input_comm  TEXT
        REFERENCES Commodity (name),
    tech        TEXT
        REFERENCES Technology (tech),
    vintage     INTEGER
        REFERENCES TimePeriod (period),
    output_comm TEXT
        REFERENCES Commodity (name),
    flow        REAL,
    PRIMARY KEY (region, scenario, period, season, tod, input_comm, tech, vintage, output_comm)
);
CREATE TABLE OutputFlowOut
(
    scenario    TEXT,
    region      TEXT,
    sector      TEXT
        REFERENCES SectorLabel (sector),
    period      INTEGER
        REFERENCES TimePeriod (period),
    season TEXT
        REFERENCES SeasonLabel (season),
    tod         TEXT
        REFERENCES TimeOfDay (tod),
    input_comm  TEXT
        REFERENCES Commodity (name),
    tech        TEXT
        REFERENCES Technology (tech),
    vintage     INTEGER
        REFERENCES TimePeriod (period),
    output_comm TEXT
        REFERENCES Commodity (name),
    flow        REAL,
    PRIMARY KEY (region, scenario, period, season, tod, input_comm, tech, vintage, output_comm)
);
CREATE TABLE OutputStorageLevel
(
    scenario TEXT,
    region TEXT,
    sector TEXT
        REFERENCES SectorLabel (sector),
    period INTEGER
        REFERENCES TimePeriod (period),
    season TEXT
        REFERENCES SeasonLabel (season),
    tod TEXT
        REFERENCES TimeOfDay (tod),
    tech TEXT
        REFERENCES Technology (tech),
    vintage INTEGER
        REFERENCES TimePeriod (period),
    level REAL,
    PRIMARY KEY (scenario, region, period, season, tod, tech, vintage)
);
CREATE TABLE OutputEmission
(
    scenario  TEXT,
    region    TEXT,
    sector    TEXT
        REFERENCES SectorLabel (sector),
    period    INTEGER
        REFERENCES TimePeriod (period),
    emis_comm TEXT
        REFERENCES Commodity (name),
    tech      TEXT
        REFERENCES Technology (tech),
    vintage   INTEGER
        REFERENCES TimePeriod (period),
    emission  REAL,
    PRIMARY KEY (region, scenario, period, emis_comm, tech, vintage)
);
CREATE TABLE OutputCost
(
    scenario TEXT,
    region   TEXT,
    sector   TEXT REFERENCES SectorLabel (sector),
    period   INTEGER REFERENCES TimePeriod (period),
    tech     TEXT REFERENCES Technology (tech),
    vintage  INTEGER REFERENCES TimePeriod (period),
    d_invest REAL,
    d_fixed  REAL,
    d_var    REAL,
    d_emiss  REAL,
    invest   REAL,
    fixed    REAL,
    var      REAL,
    emiss    REAL,
    PRIMARY KEY (scenario, region, period, tech, vintage),
    FOREIGN KEY (vintage) REFERENCES TimePeriod (period),
    FOREIGN KEY (tech) REFERENCES Technology (tech)
);
CREATE TABLE LimitGrowthCapacity
(
    region TEXT,
    tech_or_group   TEXT,
    operator TEXT NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    rate   REAL NOT NULL DEFAULT 0,
    seed   REAL NOT NULL DEFAULT 0,
    seed_units TEXT,
    notes  TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, tech_or_group, operator)
);
CREATE TABLE LimitDegrowthCapacity
(
    region TEXT,
    tech_or_group   TEXT,
    operator TEXT NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    rate   REAL NOT NULL DEFAULT 0,
    seed   REAL NOT NULL DEFAULT 0,
    seed_units TEXT,
    notes  TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, tech_or_group, operator)
);
CREATE TABLE LimitGrowthNewCapacity
(
    region TEXT,
    tech_or_group   TEXT,
    operator TEXT NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    rate   REAL NOT NULL DEFAULT 0,
    seed   REAL NOT NULL DEFAULT 0,
    seed_units TEXT,
    notes  TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, tech_or_group, operator)
);
CREATE TABLE LimitDegrowthNewCapacity
(
    region TEXT,
    tech_or_group   TEXT,
    operator TEXT NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    rate   REAL NOT NULL DEFAULT 0,
    seed   REAL NOT NULL DEFAULT 0,
    seed_units TEXT,
    notes  TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, tech_or_group, operator)
);
CREATE TABLE LimitGrowthNewCapacityDelta
(
    region TEXT,
    tech_or_group   TEXT,
    operator TEXT NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    rate   REAL NOT NULL DEFAULT 0,
    seed   REAL NOT NULL DEFAULT 0,
    seed_units TEXT,
    notes  TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, tech_or_group, operator)
);
CREATE TABLE LimitDegrowthNewCapacityDelta
(
    region TEXT,
    tech_or_group   TEXT,
    operator TEXT NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    rate   REAL NOT NULL DEFAULT 0,
    seed   REAL NOT NULL DEFAULT 0,
    seed_units TEXT,
    notes  TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, tech_or_group, operator)
);
CREATE TABLE LimitStorageLevelFraction
(
    region   TEXT,
    period   INTEGER
        REFERENCES TimePeriod (period),
    season TEXT
        REFERENCES SeasonLabel (season),
    tod      TEXT
        REFERENCES TimeOfDay (tod),
    tech     TEXT,
    vintage  INTEGER
        REFERENCES TimePeriod (period),
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    fraction REAL,
    notes    TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, period, season, tod, tech, vintage, operator)
);
CREATE TABLE LimitActivity
(
    region  TEXT,
    period  INTEGER
        REFERENCES TimePeriod (period),
    tech_or_group   TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    activity REAL,
    units   TEXT,
    notes   TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, period, tech_or_group, operator)
);
CREATE TABLE LimitActivityShare
(
    region         TEXT,
    period         INTEGER
        REFERENCES TimePeriod (period),
    sub_group      TEXT,
    super_group    TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    share REAL,
    notes          TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, period, sub_group, super_group, operator)
);
CREATE TABLE LimitAnnualCapacityFactor
(
    region      TEXT,
    period      INTEGER
        REFERENCES TimePeriod (period),
    tech        TEXT,
    output_comm TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    factor      REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    FOREIGN KEY (output_comm) REFERENCES Commodity (name),
    PRIMARY KEY (region, period, tech, operator),
    CHECK (factor >= 0 AND factor <= 1)
);
CREATE TABLE LimitCapacity
(
    region  TEXT,
    period  INTEGER
        REFERENCES TimePeriod (period),
    tech_or_group   TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    capacity REAL,
    units   TEXT,
    notes   TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, period, tech_or_group, operator)
);
CREATE TABLE LimitCapacityShare
(
    region         TEXT,
    period         INTEGER
        REFERENCES TimePeriod (period),
    sub_group      TEXT,
    super_group    TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    share REAL,
    notes          TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, period, sub_group, super_group, operator)
);
CREATE TABLE LimitNewCapacity
(
    region  TEXT,
    period  INTEGER
        REFERENCES TimePeriod (period),
    tech_or_group   TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    new_cap REAL,
    units   TEXT,
    notes   TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, period, tech_or_group, operator)
);
CREATE TABLE LimitNewCapacityShare
(
    region         TEXT,
    period         INTEGER
        REFERENCES TimePeriod (period),
    sub_group      TEXT,
    super_group    TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    share REAL,
    notes          TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, period, sub_group, super_group, operator)
);
CREATE TABLE LimitResource
(
    region  TEXT,
    tech_or_group   TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    cum_act REAL,
    units   TEXT,
    notes   TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, tech_or_group, operator)
);
CREATE TABLE LimitSeasonalCapacityFactor
(
	region  TEXT,
	period	INTEGER
        REFERENCES TimePeriod (period),
	season TEXT
        REFERENCES SeasonLabel (season),
	tech    TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
	factor	REAL,
	notes	TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (region) REFERENCES Region (region),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
	PRIMARY KEY (region, period, season, tech, operator)
);
CREATE TABLE LimitTechInputSplit
(
    region         TEXT,
    period         INTEGER
        REFERENCES TimePeriod (period),
    input_comm     TEXT,
    tech           TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    proportion REAL,
    notes          TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    FOREIGN KEY (input_comm) REFERENCES Commodity (name),
    PRIMARY KEY (region, period, input_comm, tech, operator)
);
CREATE TABLE LimitTechInputSplitAnnual
(
    region         TEXT,
    period         INTEGER
        REFERENCES TimePeriod (period),
    input_comm     TEXT,
    tech           TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    proportion REAL,
    notes          TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, period, input_comm, tech, operator)
);
INSERT INTO LimitTechInputSplitAnnual VALUES('AB',2025,'A_elc','A_AGRI','e',0.068000000000000007105,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('AB',2025,'A_ng','A_AGRI','e',0.057000000000000010658,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('AB',2025,'A_dsl','A_AGRI','e',0.14399999999999999467,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('AB',2025,'A_gsl','A_AGRI','e',0.34399999999999995026,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('AB',2030,'A_elc','A_AGRI','e',0.068000000000000007105,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('AB',2030,'A_ng','A_AGRI','e',0.057000000000000010658,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('AB',2030,'A_dsl','A_AGRI','e',0.14399999999999999467,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('AB',2030,'A_gsl','A_AGRI','e',0.34399999999999995026,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('AB',2035,'A_elc','A_AGRI','e',0.068000000000000007105,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('AB',2035,'A_ng','A_AGRI','e',0.057000000000000010658,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('AB',2035,'A_dsl','A_AGRI','e',0.14399999999999999467,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('AB',2035,'A_gsl','A_AGRI','e',0.34399999999999995026,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('AB',2040,'A_elc','A_AGRI','e',0.068000000000000007105,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('AB',2040,'A_ng','A_AGRI','e',0.057000000000000010658,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('AB',2040,'A_dsl','A_AGRI','e',0.14399999999999999467,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('AB',2040,'A_gsl','A_AGRI','e',0.34399999999999995026,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('AB',2045,'A_elc','A_AGRI','e',0.068000000000000007105,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('AB',2045,'A_ng','A_AGRI','e',0.057000000000000010658,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('AB',2045,'A_dsl','A_AGRI','e',0.14399999999999999467,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('AB',2045,'A_gsl','A_AGRI','e',0.34399999999999995026,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('AB',2050,'A_elc','A_AGRI','e',0.068000000000000007105,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('AB',2050,'A_ng','A_AGRI','e',0.057000000000000010658,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('AB',2050,'A_dsl','A_AGRI','e',0.14399999999999999467,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('AB',2050,'A_gsl','A_AGRI','e',0.34399999999999995026,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('ON',2025,'A_elc','A_AGRI','e',0.10500000000000000444,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRON1');
INSERT INTO LimitTechInputSplitAnnual VALUES('ON',2025,'A_ng','A_AGRI','e',0.29499999999999996447,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRON1');
INSERT INTO LimitTechInputSplitAnnual VALUES('ON',2025,'A_dsl','A_AGRI','e',0.08,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRON1');
INSERT INTO LimitTechInputSplitAnnual VALUES('ON',2025,'A_gsl','A_AGRI','e',0.17700000000000000177,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRON1');
INSERT INTO LimitTechInputSplitAnnual VALUES('ON',2030,'A_elc','A_AGRI','e',0.10500000000000000444,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRON1');
INSERT INTO LimitTechInputSplitAnnual VALUES('ON',2030,'A_ng','A_AGRI','e',0.29499999999999996447,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRON1');
INSERT INTO LimitTechInputSplitAnnual VALUES('ON',2030,'A_dsl','A_AGRI','e',0.08,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRON1');
INSERT INTO LimitTechInputSplitAnnual VALUES('ON',2030,'A_gsl','A_AGRI','e',0.17700000000000000177,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRON1');
INSERT INTO LimitTechInputSplitAnnual VALUES('ON',2035,'A_elc','A_AGRI','e',0.10500000000000000444,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRON1');
INSERT INTO LimitTechInputSplitAnnual VALUES('ON',2035,'A_ng','A_AGRI','e',0.29499999999999996447,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRON1');
INSERT INTO LimitTechInputSplitAnnual VALUES('ON',2035,'A_dsl','A_AGRI','e',0.08,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRON1');
INSERT INTO LimitTechInputSplitAnnual VALUES('ON',2035,'A_gsl','A_AGRI','e',0.17700000000000000177,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRON1');
INSERT INTO LimitTechInputSplitAnnual VALUES('ON',2040,'A_elc','A_AGRI','e',0.10500000000000000444,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRON1');
INSERT INTO LimitTechInputSplitAnnual VALUES('ON',2040,'A_ng','A_AGRI','e',0.29499999999999996447,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRON1');
INSERT INTO LimitTechInputSplitAnnual VALUES('ON',2040,'A_dsl','A_AGRI','e',0.08,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRON1');
INSERT INTO LimitTechInputSplitAnnual VALUES('ON',2040,'A_gsl','A_AGRI','e',0.17700000000000000177,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRON1');
INSERT INTO LimitTechInputSplitAnnual VALUES('ON',2045,'A_elc','A_AGRI','e',0.10500000000000000444,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRON1');
INSERT INTO LimitTechInputSplitAnnual VALUES('ON',2045,'A_ng','A_AGRI','e',0.29499999999999996447,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRON1');
INSERT INTO LimitTechInputSplitAnnual VALUES('ON',2045,'A_dsl','A_AGRI','e',0.08,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRON1');
INSERT INTO LimitTechInputSplitAnnual VALUES('ON',2045,'A_gsl','A_AGRI','e',0.17700000000000000177,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRON1');
INSERT INTO LimitTechInputSplitAnnual VALUES('ON',2050,'A_elc','A_AGRI','e',0.10500000000000000444,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRON1');
INSERT INTO LimitTechInputSplitAnnual VALUES('ON',2050,'A_ng','A_AGRI','e',0.29499999999999996447,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRON1');
INSERT INTO LimitTechInputSplitAnnual VALUES('ON',2050,'A_dsl','A_AGRI','e',0.08,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRON1');
INSERT INTO LimitTechInputSplitAnnual VALUES('ON',2050,'A_gsl','A_AGRI','e',0.17700000000000000177,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRON1');
INSERT INTO LimitTechInputSplitAnnual VALUES('BC',2025,'A_elc','A_AGRI','e',0.032000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRBC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('BC',2025,'A_ng','A_AGRI','e',0.10500000000000000444,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRBC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('BC',2025,'A_dsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRBC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('BC',2025,'A_gsl','A_AGRI','e',0.14399999999999999467,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRBC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('BC',2030,'A_elc','A_AGRI','e',0.032000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRBC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('BC',2030,'A_ng','A_AGRI','e',0.10500000000000000444,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRBC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('BC',2030,'A_dsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRBC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('BC',2030,'A_gsl','A_AGRI','e',0.14399999999999999467,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRBC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('BC',2035,'A_elc','A_AGRI','e',0.032000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRBC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('BC',2035,'A_ng','A_AGRI','e',0.10500000000000000444,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRBC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('BC',2035,'A_dsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRBC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('BC',2035,'A_gsl','A_AGRI','e',0.14399999999999999467,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRBC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('BC',2040,'A_elc','A_AGRI','e',0.032000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRBC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('BC',2040,'A_ng','A_AGRI','e',0.10500000000000000444,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRBC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('BC',2040,'A_dsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRBC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('BC',2040,'A_gsl','A_AGRI','e',0.14399999999999999467,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRBC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('BC',2045,'A_elc','A_AGRI','e',0.032000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRBC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('BC',2045,'A_ng','A_AGRI','e',0.10500000000000000444,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRBC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('BC',2045,'A_dsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRBC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('BC',2045,'A_gsl','A_AGRI','e',0.14399999999999999467,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRBC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('BC',2050,'A_elc','A_AGRI','e',0.032000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRBC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('BC',2050,'A_ng','A_AGRI','e',0.10500000000000000444,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRBC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('BC',2050,'A_dsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRBC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('BC',2050,'A_gsl','A_AGRI','e',0.14399999999999999467,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRBC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('MB',2025,'A_elc','A_AGRI','e',0.034000000000000003552,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRMB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('MB',2025,'A_ng','A_AGRI','e',0.001,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRMB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('MB',2025,'A_dsl','A_AGRI','e',0.062000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRMB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('MB',2025,'A_gsl','A_AGRI','e',0.15700000000000001065,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRMB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('MB',2030,'A_elc','A_AGRI','e',0.034000000000000003552,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRMB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('MB',2030,'A_ng','A_AGRI','e',0.001,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRMB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('MB',2030,'A_dsl','A_AGRI','e',0.062000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRMB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('MB',2030,'A_gsl','A_AGRI','e',0.15700000000000001065,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRMB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('MB',2035,'A_elc','A_AGRI','e',0.034000000000000003552,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRMB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('MB',2035,'A_ng','A_AGRI','e',0.001,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRMB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('MB',2035,'A_dsl','A_AGRI','e',0.062000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRMB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('MB',2035,'A_gsl','A_AGRI','e',0.15700000000000001065,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRMB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('MB',2040,'A_elc','A_AGRI','e',0.034000000000000003552,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRMB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('MB',2040,'A_ng','A_AGRI','e',0.001,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRMB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('MB',2040,'A_dsl','A_AGRI','e',0.062000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRMB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('MB',2040,'A_gsl','A_AGRI','e',0.15700000000000001065,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRMB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('MB',2045,'A_elc','A_AGRI','e',0.034000000000000003552,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRMB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('MB',2045,'A_ng','A_AGRI','e',0.001,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRMB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('MB',2045,'A_dsl','A_AGRI','e',0.062000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRMB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('MB',2045,'A_gsl','A_AGRI','e',0.15700000000000001065,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRMB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('MB',2050,'A_elc','A_AGRI','e',0.034000000000000003552,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRMB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('MB',2050,'A_ng','A_AGRI','e',0.001,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRMB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('MB',2050,'A_dsl','A_AGRI','e',0.062000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRMB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('MB',2050,'A_gsl','A_AGRI','e',0.15700000000000001065,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRMB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('SK',2025,'A_elc','A_AGRI','e',0.046999999999999992894,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRSK1');
INSERT INTO LimitTechInputSplitAnnual VALUES('SK',2025,'A_ng','A_AGRI','e',0.027000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRSK1');
INSERT INTO LimitTechInputSplitAnnual VALUES('SK',2025,'A_dsl','A_AGRI','e',0.125,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRSK1');
INSERT INTO LimitTechInputSplitAnnual VALUES('SK',2025,'A_gsl','A_AGRI','e',0.49000000000000003552,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRSK1');
INSERT INTO LimitTechInputSplitAnnual VALUES('SK',2030,'A_elc','A_AGRI','e',0.046999999999999992894,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRSK1');
INSERT INTO LimitTechInputSplitAnnual VALUES('SK',2030,'A_ng','A_AGRI','e',0.027000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRSK1');
INSERT INTO LimitTechInputSplitAnnual VALUES('SK',2030,'A_dsl','A_AGRI','e',0.125,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRSK1');
INSERT INTO LimitTechInputSplitAnnual VALUES('SK',2030,'A_gsl','A_AGRI','e',0.49000000000000003552,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRSK1');
INSERT INTO LimitTechInputSplitAnnual VALUES('SK',2035,'A_elc','A_AGRI','e',0.046999999999999992894,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRSK1');
INSERT INTO LimitTechInputSplitAnnual VALUES('SK',2035,'A_ng','A_AGRI','e',0.027000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRSK1');
INSERT INTO LimitTechInputSplitAnnual VALUES('SK',2035,'A_dsl','A_AGRI','e',0.125,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRSK1');
INSERT INTO LimitTechInputSplitAnnual VALUES('SK',2035,'A_gsl','A_AGRI','e',0.49000000000000003552,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRSK1');
INSERT INTO LimitTechInputSplitAnnual VALUES('SK',2040,'A_elc','A_AGRI','e',0.046999999999999992894,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRSK1');
INSERT INTO LimitTechInputSplitAnnual VALUES('SK',2040,'A_ng','A_AGRI','e',0.027000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRSK1');
INSERT INTO LimitTechInputSplitAnnual VALUES('SK',2040,'A_dsl','A_AGRI','e',0.125,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRSK1');
INSERT INTO LimitTechInputSplitAnnual VALUES('SK',2040,'A_gsl','A_AGRI','e',0.49000000000000003552,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRSK1');
INSERT INTO LimitTechInputSplitAnnual VALUES('SK',2045,'A_elc','A_AGRI','e',0.046999999999999992894,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRSK1');
INSERT INTO LimitTechInputSplitAnnual VALUES('SK',2045,'A_ng','A_AGRI','e',0.027000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRSK1');
INSERT INTO LimitTechInputSplitAnnual VALUES('SK',2045,'A_dsl','A_AGRI','e',0.125,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRSK1');
INSERT INTO LimitTechInputSplitAnnual VALUES('SK',2045,'A_gsl','A_AGRI','e',0.49000000000000003552,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRSK1');
INSERT INTO LimitTechInputSplitAnnual VALUES('SK',2050,'A_elc','A_AGRI','e',0.046999999999999992894,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRSK1');
INSERT INTO LimitTechInputSplitAnnual VALUES('SK',2050,'A_ng','A_AGRI','e',0.027000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRSK1');
INSERT INTO LimitTechInputSplitAnnual VALUES('SK',2050,'A_dsl','A_AGRI','e',0.125,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRSK1');
INSERT INTO LimitTechInputSplitAnnual VALUES('SK',2050,'A_gsl','A_AGRI','e',0.49000000000000003552,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRSK1');
INSERT INTO LimitTechInputSplitAnnual VALUES('QC',2025,'A_elc','A_AGRI','e',0.072999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRQC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('QC',2025,'A_ng','A_AGRI','e',0.014000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRQC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('QC',2025,'A_dsl','A_AGRI','e',0.045,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRQC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('QC',2025,'A_gsl','A_AGRI','e',0.145,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRQC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('QC',2030,'A_elc','A_AGRI','e',0.072999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRQC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('QC',2030,'A_ng','A_AGRI','e',0.014000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRQC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('QC',2030,'A_dsl','A_AGRI','e',0.045,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRQC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('QC',2030,'A_gsl','A_AGRI','e',0.145,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRQC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('QC',2035,'A_elc','A_AGRI','e',0.072999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRQC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('QC',2035,'A_ng','A_AGRI','e',0.014000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRQC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('QC',2035,'A_dsl','A_AGRI','e',0.045,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRQC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('QC',2035,'A_gsl','A_AGRI','e',0.145,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRQC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('QC',2040,'A_elc','A_AGRI','e',0.072999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRQC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('QC',2040,'A_ng','A_AGRI','e',0.014000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRQC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('QC',2040,'A_dsl','A_AGRI','e',0.045,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRQC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('QC',2040,'A_gsl','A_AGRI','e',0.145,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRQC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('QC',2045,'A_elc','A_AGRI','e',0.072999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRQC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('QC',2045,'A_ng','A_AGRI','e',0.014000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRQC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('QC',2045,'A_dsl','A_AGRI','e',0.045,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRQC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('QC',2045,'A_gsl','A_AGRI','e',0.145,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRQC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('QC',2050,'A_elc','A_AGRI','e',0.072999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRQC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('QC',2050,'A_ng','A_AGRI','e',0.014000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRQC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('QC',2050,'A_dsl','A_AGRI','e',0.045,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRQC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('QC',2050,'A_gsl','A_AGRI','e',0.145,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRQC1');
INSERT INTO LimitTechInputSplitAnnual VALUES('PEI',2025,'A_elc','A_AGRI','e',0.022999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRPEI1');
INSERT INTO LimitTechInputSplitAnnual VALUES('PEI',2025,'A_dsl','A_AGRI','e',0.009,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRPEI1');
INSERT INTO LimitTechInputSplitAnnual VALUES('PEI',2025,'A_gsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRPEI1');
INSERT INTO LimitTechInputSplitAnnual VALUES('PEI',2030,'A_elc','A_AGRI','e',0.022999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRPEI1');
INSERT INTO LimitTechInputSplitAnnual VALUES('PEI',2030,'A_dsl','A_AGRI','e',0.009,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRPEI1');
INSERT INTO LimitTechInputSplitAnnual VALUES('PEI',2030,'A_gsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRPEI1');
INSERT INTO LimitTechInputSplitAnnual VALUES('PEI',2035,'A_elc','A_AGRI','e',0.022999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRPEI1');
INSERT INTO LimitTechInputSplitAnnual VALUES('PEI',2035,'A_dsl','A_AGRI','e',0.009,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRPEI1');
INSERT INTO LimitTechInputSplitAnnual VALUES('PEI',2035,'A_gsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRPEI1');
INSERT INTO LimitTechInputSplitAnnual VALUES('PEI',2040,'A_elc','A_AGRI','e',0.022999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRPEI1');
INSERT INTO LimitTechInputSplitAnnual VALUES('PEI',2040,'A_dsl','A_AGRI','e',0.009,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRPEI1');
INSERT INTO LimitTechInputSplitAnnual VALUES('PEI',2040,'A_gsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRPEI1');
INSERT INTO LimitTechInputSplitAnnual VALUES('PEI',2045,'A_elc','A_AGRI','e',0.022999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRPEI1');
INSERT INTO LimitTechInputSplitAnnual VALUES('PEI',2045,'A_dsl','A_AGRI','e',0.009,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRPEI1');
INSERT INTO LimitTechInputSplitAnnual VALUES('PEI',2045,'A_gsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRPEI1');
INSERT INTO LimitTechInputSplitAnnual VALUES('PEI',2050,'A_elc','A_AGRI','e',0.022999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRPEI1');
INSERT INTO LimitTechInputSplitAnnual VALUES('PEI',2050,'A_dsl','A_AGRI','e',0.009,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRPEI1');
INSERT INTO LimitTechInputSplitAnnual VALUES('PEI',2050,'A_gsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRPEI1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NS',2025,'A_elc','A_AGRI','e',0.022999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNS1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NS',2025,'A_dsl','A_AGRI','e',0.009,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNS1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NS',2025,'A_gsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNS1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NS',2030,'A_elc','A_AGRI','e',0.022999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNS1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NS',2030,'A_dsl','A_AGRI','e',0.009,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNS1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NS',2030,'A_gsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNS1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NS',2035,'A_elc','A_AGRI','e',0.022999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNS1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NS',2035,'A_dsl','A_AGRI','e',0.009,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNS1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NS',2035,'A_gsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNS1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NS',2040,'A_elc','A_AGRI','e',0.022999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNS1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NS',2040,'A_dsl','A_AGRI','e',0.009,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNS1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NS',2040,'A_gsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNS1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NS',2045,'A_elc','A_AGRI','e',0.022999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNS1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NS',2045,'A_dsl','A_AGRI','e',0.009,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNS1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NS',2045,'A_gsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNS1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NS',2050,'A_elc','A_AGRI','e',0.022999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNS1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NS',2050,'A_dsl','A_AGRI','e',0.009,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNS1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NS',2050,'A_gsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNS1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NB',2025,'A_elc','A_AGRI','e',0.022999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NB',2025,'A_dsl','A_AGRI','e',0.009,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NB',2025,'A_gsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NB',2030,'A_elc','A_AGRI','e',0.022999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NB',2030,'A_dsl','A_AGRI','e',0.009,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NB',2030,'A_gsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NB',2035,'A_elc','A_AGRI','e',0.022999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NB',2035,'A_dsl','A_AGRI','e',0.009,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NB',2035,'A_gsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NB',2040,'A_elc','A_AGRI','e',0.022999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NB',2040,'A_dsl','A_AGRI','e',0.009,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NB',2040,'A_gsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NB',2045,'A_elc','A_AGRI','e',0.022999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NB',2045,'A_dsl','A_AGRI','e',0.009,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NB',2045,'A_gsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NB',2050,'A_elc','A_AGRI','e',0.022999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NB',2050,'A_dsl','A_AGRI','e',0.009,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NB',2050,'A_gsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NLLAB',2025,'A_elc','A_AGRI','e',0.022999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNLLAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NLLAB',2025,'A_dsl','A_AGRI','e',0.009,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNLLAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NLLAB',2025,'A_gsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNLLAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NLLAB',2030,'A_elc','A_AGRI','e',0.022999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNLLAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NLLAB',2030,'A_dsl','A_AGRI','e',0.009,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNLLAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NLLAB',2030,'A_gsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNLLAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NLLAB',2035,'A_elc','A_AGRI','e',0.022999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNLLAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NLLAB',2035,'A_dsl','A_AGRI','e',0.009,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNLLAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NLLAB',2035,'A_gsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNLLAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NLLAB',2040,'A_elc','A_AGRI','e',0.022999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNLLAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NLLAB',2040,'A_dsl','A_AGRI','e',0.009,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNLLAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NLLAB',2040,'A_gsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNLLAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NLLAB',2045,'A_elc','A_AGRI','e',0.022999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNLLAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NLLAB',2045,'A_dsl','A_AGRI','e',0.009,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNLLAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NLLAB',2045,'A_gsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNLLAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NLLAB',2050,'A_elc','A_AGRI','e',0.022999999999999998223,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNLLAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NLLAB',2050,'A_dsl','A_AGRI','e',0.009,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNLLAB1');
INSERT INTO LimitTechInputSplitAnnual VALUES('NLLAB',2050,'A_gsl','A_AGRI','e',0.037000000000000001776,'Calculated from NRCan comprehensive database. If values were n.a., the remainder to 100% is evenly distributed.','[A1]',2,1,2,3,3,'AGRIHRNLLAB1');
CREATE TABLE LimitTechOutputSplit
(
    region         TEXT,
    period         INTEGER
        REFERENCES TimePeriod (period),
    tech           TEXT,
    output_comm    TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    proportion REAL,
    notes          TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    FOREIGN KEY (output_comm) REFERENCES Commodity (name),
    PRIMARY KEY (region, period, tech, output_comm, operator)
);
CREATE TABLE LimitTechOutputSplitAnnual
(
    region         TEXT,
    period         INTEGER
        REFERENCES TimePeriod (period),
    tech           TEXT,
    output_comm    TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    proportion REAL,
    notes          TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    FOREIGN KEY (output_comm) REFERENCES Commodity (name),
    PRIMARY KEY (region, period, tech, output_comm, operator)
);
CREATE TABLE LimitEmission
(
    region    TEXT,
    period    INTEGER
        REFERENCES TimePeriod (period),
    emis_comm TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    value     REAL,
    units     TEXT,
    notes     TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (emis_comm) REFERENCES Commodity (name),
    PRIMARY KEY (region, period, emis_comm, operator)
);
CREATE TABLE LinkedTech
(
    primary_region TEXT,
    primary_tech   TEXT,
    emis_comm      TEXT,
    driven_tech    TEXT,
    notes          TEXT,
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (primary_tech) REFERENCES Technology (tech),
    FOREIGN KEY (driven_tech) REFERENCES Technology (tech),
    FOREIGN KEY (emis_comm) REFERENCES Commodity (name),
    PRIMARY KEY (primary_region, primary_tech, emis_comm)
);
CREATE TABLE PlanningReserveMargin
(
    region TEXT,
    margin REAL,
    notes TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (region) REFERENCES Region (region),
    PRIMARY KEY (region)
);
CREATE TABLE RampDownHourly
(
    region TEXT,
    tech   TEXT,
    rate   REAL,
    notes TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, tech)
);
CREATE TABLE RampUpHourly
(
    region TEXT,
    tech   TEXT,
    rate   REAL,
    notes TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, tech)
);
CREATE TABLE Region
(
    region TEXT,
    notes  TEXT,
    PRIMARY KEY (region)
);
INSERT INTO Region VALUES('AB','');
INSERT INTO Region VALUES('ON','');
INSERT INTO Region VALUES('BC','');
INSERT INTO Region VALUES('MB','');
INSERT INTO Region VALUES('SK','');
INSERT INTO Region VALUES('QC','');
INSERT INTO Region VALUES('PEI','');
INSERT INTO Region VALUES('NS','');
INSERT INTO Region VALUES('NB','');
INSERT INTO Region VALUES('NLLAB','');
CREATE TABLE ReserveCapacityDerate
(
    region  TEXT,
    period  INTEGER
        REFERENCES TimePeriod (period),
    season  TEXT
    	REFERENCES SeasonLabel (season),
    tech    TEXT,
    vintage INTEGER,
    factor  REAL,
    notes   TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, period, season, tech, vintage),
    CHECK (factor >= 0 AND factor <= 1)
);
CREATE TABLE TimeSegmentFraction
(   
    period INTEGER
        REFERENCES TimePeriod (period),
    season TEXT
        REFERENCES SeasonLabel (season),
    tod     TEXT
        REFERENCES TimeOfDay (tod),
    segfrac REAL,
    notes   TEXT,
    PRIMARY KEY (period, season, tod),
    CHECK (segfrac >= 0 AND segfrac <= 1)
);
INSERT INTO TimeSegmentFraction VALUES(2025,'D022','H01',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D022','H02',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D022','H03',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D022','H04',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D022','H05',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D022','H06',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D022','H07',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D022','H08',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D022','H09',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D022','H10',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D022','H11',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D022','H12',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D022','H13',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D022','H14',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D022','H15',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D022','H16',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D022','H17',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D022','H18',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D022','H19',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D022','H20',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D022','H21',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D022','H22',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D022','H23',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D022','H24',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D047','H01',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D047','H02',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D047','H03',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D047','H04',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D047','H05',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D047','H06',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D047','H07',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D047','H08',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D047','H09',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D047','H10',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D047','H11',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D047','H12',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D047','H13',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D047','H14',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D047','H15',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D047','H16',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D047','H17',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D047','H18',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D047','H19',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D047','H20',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D047','H21',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D047','H22',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D047','H23',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D047','H24',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D157','H01',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D157','H02',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D157','H03',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D157','H04',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D157','H05',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D157','H06',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D157','H07',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D157','H08',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D157','H09',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D157','H10',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D157','H11',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D157','H12',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D157','H13',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D157','H14',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D157','H15',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D157','H16',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D157','H17',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D157','H18',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D157','H19',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D157','H20',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D157','H21',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D157','H22',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D157','H23',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D157','H24',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D213','H01',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D213','H02',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D213','H03',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D213','H04',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D213','H05',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D213','H06',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D213','H07',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D213','H08',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D213','H09',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D213','H10',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D213','H11',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D213','H12',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D213','H13',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D213','H14',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D213','H15',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D213','H16',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D213','H17',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D213','H18',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D213','H19',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D213','H20',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D213','H21',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D213','H22',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D213','H23',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2025,'D213','H24',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D022','H01',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D022','H02',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D022','H03',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D022','H04',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D022','H05',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D022','H06',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D022','H07',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D022','H08',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D022','H09',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D022','H10',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D022','H11',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D022','H12',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D022','H13',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D022','H14',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D022','H15',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D022','H16',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D022','H17',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D022','H18',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D022','H19',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D022','H20',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D022','H21',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D022','H22',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D022','H23',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D022','H24',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D047','H01',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D047','H02',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D047','H03',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D047','H04',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D047','H05',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D047','H06',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D047','H07',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D047','H08',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D047','H09',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D047','H10',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D047','H11',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D047','H12',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D047','H13',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D047','H14',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D047','H15',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D047','H16',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D047','H17',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D047','H18',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D047','H19',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D047','H20',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D047','H21',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D047','H22',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D047','H23',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D047','H24',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D157','H01',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D157','H02',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D157','H03',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D157','H04',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D157','H05',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D157','H06',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D157','H07',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D157','H08',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D157','H09',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D157','H10',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D157','H11',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D157','H12',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D157','H13',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D157','H14',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D157','H15',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D157','H16',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D157','H17',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D157','H18',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D157','H19',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D157','H20',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D157','H21',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D157','H22',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D157','H23',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D157','H24',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D213','H01',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D213','H02',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D213','H03',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D213','H04',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D213','H05',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D213','H06',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D213','H07',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D213','H08',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D213','H09',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D213','H10',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D213','H11',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D213','H12',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D213','H13',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D213','H14',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D213','H15',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D213','H16',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D213','H17',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D213','H18',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D213','H19',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D213','H20',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D213','H21',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D213','H22',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D213','H23',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2030,'D213','H24',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D022','H01',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D022','H02',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D022','H03',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D022','H04',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D022','H05',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D022','H06',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D022','H07',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D022','H08',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D022','H09',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D022','H10',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D022','H11',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D022','H12',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D022','H13',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D022','H14',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D022','H15',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D022','H16',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D022','H17',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D022','H18',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D022','H19',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D022','H20',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D022','H21',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D022','H22',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D022','H23',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D022','H24',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D047','H01',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D047','H02',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D047','H03',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D047','H04',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D047','H05',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D047','H06',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D047','H07',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D047','H08',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D047','H09',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D047','H10',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D047','H11',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D047','H12',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D047','H13',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D047','H14',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D047','H15',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D047','H16',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D047','H17',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D047','H18',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D047','H19',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D047','H20',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D047','H21',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D047','H22',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D047','H23',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D047','H24',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D157','H01',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D157','H02',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D157','H03',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D157','H04',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D157','H05',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D157','H06',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D157','H07',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D157','H08',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D157','H09',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D157','H10',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D157','H11',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D157','H12',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D157','H13',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D157','H14',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D157','H15',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D157','H16',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D157','H17',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D157','H18',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D157','H19',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D157','H20',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D157','H21',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D157','H22',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D157','H23',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D157','H24',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D213','H01',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D213','H02',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D213','H03',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D213','H04',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D213','H05',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D213','H06',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D213','H07',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D213','H08',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D213','H09',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D213','H10',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D213','H11',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D213','H12',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D213','H13',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D213','H14',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D213','H15',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D213','H16',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D213','H17',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D213','H18',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D213','H19',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D213','H20',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D213','H21',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D213','H22',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D213','H23',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2035,'D213','H24',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D022','H01',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D022','H02',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D022','H03',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D022','H04',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D022','H05',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D022','H06',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D022','H07',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D022','H08',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D022','H09',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D022','H10',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D022','H11',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D022','H12',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D022','H13',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D022','H14',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D022','H15',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D022','H16',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D022','H17',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D022','H18',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D022','H19',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D022','H20',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D022','H21',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D022','H22',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D022','H23',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D022','H24',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D047','H01',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D047','H02',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D047','H03',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D047','H04',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D047','H05',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D047','H06',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D047','H07',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D047','H08',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D047','H09',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D047','H10',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D047','H11',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D047','H12',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D047','H13',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D047','H14',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D047','H15',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D047','H16',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D047','H17',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D047','H18',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D047','H19',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D047','H20',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D047','H21',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D047','H22',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D047','H23',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D047','H24',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D157','H01',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D157','H02',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D157','H03',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D157','H04',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D157','H05',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D157','H06',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D157','H07',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D157','H08',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D157','H09',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D157','H10',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D157','H11',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D157','H12',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D157','H13',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D157','H14',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D157','H15',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D157','H16',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D157','H17',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D157','H18',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D157','H19',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D157','H20',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D157','H21',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D157','H22',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D157','H23',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D157','H24',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D213','H01',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D213','H02',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D213','H03',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D213','H04',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D213','H05',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D213','H06',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D213','H07',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D213','H08',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D213','H09',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D213','H10',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D213','H11',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D213','H12',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D213','H13',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D213','H14',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D213','H15',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D213','H16',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D213','H17',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D213','H18',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D213','H19',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D213','H20',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D213','H21',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D213','H22',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D213','H23',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2040,'D213','H24',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D022','H01',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D022','H02',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D022','H03',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D022','H04',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D022','H05',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D022','H06',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D022','H07',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D022','H08',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D022','H09',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D022','H10',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D022','H11',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D022','H12',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D022','H13',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D022','H14',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D022','H15',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D022','H16',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D022','H17',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D022','H18',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D022','H19',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D022','H20',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D022','H21',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D022','H22',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D022','H23',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D022','H24',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D047','H01',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D047','H02',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D047','H03',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D047','H04',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D047','H05',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D047','H06',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D047','H07',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D047','H08',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D047','H09',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D047','H10',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D047','H11',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D047','H12',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D047','H13',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D047','H14',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D047','H15',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D047','H16',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D047','H17',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D047','H18',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D047','H19',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D047','H20',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D047','H21',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D047','H22',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D047','H23',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D047','H24',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D157','H01',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D157','H02',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D157','H03',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D157','H04',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D157','H05',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D157','H06',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D157','H07',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D157','H08',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D157','H09',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D157','H10',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D157','H11',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D157','H12',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D157','H13',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D157','H14',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D157','H15',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D157','H16',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D157','H17',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D157','H18',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D157','H19',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D157','H20',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D157','H21',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D157','H22',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D157','H23',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D157','H24',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D213','H01',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D213','H02',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D213','H03',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D213','H04',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D213','H05',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D213','H06',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D213','H07',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D213','H08',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D213','H09',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D213','H10',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D213','H11',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D213','H12',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D213','H13',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D213','H14',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D213','H15',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D213','H16',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D213','H17',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D213','H18',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D213','H19',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D213','H20',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D213','H21',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D213','H22',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D213','H23',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2045,'D213','H24',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D022','H01',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D022','H02',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D022','H03',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D022','H04',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D022','H05',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D022','H06',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D022','H07',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D022','H08',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D022','H09',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D022','H10',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D022','H11',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D022','H12',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D022','H13',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D022','H14',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D022','H15',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D022','H16',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D022','H17',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D022','H18',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D022','H19',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D022','H20',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D022','H21',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D022','H22',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D022','H23',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D022','H24',0.0060835629017447203636,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D047','H01',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D047','H02',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D047','H03',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D047','H04',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D047','H05',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D047','H06',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D047','H07',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D047','H08',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D047','H09',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D047','H10',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D047','H11',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D047','H12',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D047','H13',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D047','H14',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D047','H15',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D047','H16',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D047','H17',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D047','H18',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D047','H19',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D047','H20',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D047','H21',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D047','H22',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D047','H23',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D047','H24',0.01193755739210284652,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D157','H01',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D157','H02',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D157','H03',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D157','H04',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D157','H05',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D157','H06',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D157','H07',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D157','H08',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D157','H09',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D157','H10',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D157','H11',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D157','H12',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D157','H13',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D157','H14',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D157','H15',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D157','H16',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D157','H17',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D157','H18',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D157','H19',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D157','H20',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D157','H21',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D157','H22',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D157','H23',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D157','H24',0.012396694214876031736,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D213','H01',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D213','H02',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D213','H03',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D213','H04',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D213','H05',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D213','H06',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D213','H07',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D213','H08',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D213','H09',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D213','H10',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D213','H11',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D213','H12',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D213','H13',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D213','H14',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D213','H15',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D213','H16',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D213','H17',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D213','H18',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D213','H19',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D213','H20',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D213','H21',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D213','H22',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D213','H23',0.011248852157943067808,'Weight from clustering');
INSERT INTO TimeSegmentFraction VALUES(2050,'D213','H24',0.011248852157943067808,'Weight from clustering');
CREATE TABLE StorageDuration
(
    region   TEXT,
    tech     TEXT,
    duration REAL,
    notes    TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, tech)
);
CREATE TABLE LifetimeSurvivalCurve
(
    region  TEXT    NOT NULL,
    period  INTEGER NOT NULL,
    tech    TEXT    NOT NULL,
    vintage INTEGER NOT NULL
        REFERENCES TimePeriod (period),
    fraction  REAL,
    notes   TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, period, tech, vintage)
);
CREATE TABLE TechnologyType
(
    label       TEXT PRIMARY KEY,
    description TEXT
);
INSERT INTO TechnologyType VALUES('p','production technology');
INSERT INTO TechnologyType VALUES('pb','baseload production technology');
INSERT INTO TechnologyType VALUES('ps','storage production technology');
CREATE TABLE TimeOfDay
(
    sequence INTEGER UNIQUE,
    tod      TEXT
        PRIMARY KEY
);
INSERT INTO TimeOfDay VALUES(0,'H01');
INSERT INTO TimeOfDay VALUES(1,'H02');
INSERT INTO TimeOfDay VALUES(2,'H03');
INSERT INTO TimeOfDay VALUES(3,'H04');
INSERT INTO TimeOfDay VALUES(4,'H05');
INSERT INTO TimeOfDay VALUES(5,'H06');
INSERT INTO TimeOfDay VALUES(6,'H07');
INSERT INTO TimeOfDay VALUES(7,'H08');
INSERT INTO TimeOfDay VALUES(8,'H09');
INSERT INTO TimeOfDay VALUES(9,'H10');
INSERT INTO TimeOfDay VALUES(10,'H11');
INSERT INTO TimeOfDay VALUES(11,'H12');
INSERT INTO TimeOfDay VALUES(12,'H13');
INSERT INTO TimeOfDay VALUES(13,'H14');
INSERT INTO TimeOfDay VALUES(14,'H15');
INSERT INTO TimeOfDay VALUES(15,'H16');
INSERT INTO TimeOfDay VALUES(16,'H17');
INSERT INTO TimeOfDay VALUES(17,'H18');
INSERT INTO TimeOfDay VALUES(18,'H19');
INSERT INTO TimeOfDay VALUES(19,'H20');
INSERT INTO TimeOfDay VALUES(20,'H21');
INSERT INTO TimeOfDay VALUES(21,'H22');
INSERT INTO TimeOfDay VALUES(22,'H23');
INSERT INTO TimeOfDay VALUES(23,'H24');
CREATE TABLE TimePeriod
(
    sequence INTEGER UNIQUE,
    period   INTEGER
        PRIMARY KEY,
    flag     TEXT
        REFERENCES TimePeriodType (label)
);
INSERT INTO TimePeriod VALUES(0,2025,'f');
INSERT INTO TimePeriod VALUES(1,2030,'f');
INSERT INTO TimePeriod VALUES(2,2035,'f');
INSERT INTO TimePeriod VALUES(3,2040,'f');
INSERT INTO TimePeriod VALUES(4,2045,'f');
INSERT INTO TimePeriod VALUES(5,2050,'f');
INSERT INTO TimePeriod VALUES(6,2055,'f');
CREATE TABLE TimeSeason
(
    period INTEGER
        REFERENCES TimePeriod (period),
    sequence INTEGER,
    season TEXT
        REFERENCES SeasonLabel (season),
    notes TEXT,
    PRIMARY KEY (period, sequence, season)
);
INSERT INTO TimeSeason VALUES(2025,0,'D022',NULL);
INSERT INTO TimeSeason VALUES(2025,1,'D047',NULL);
INSERT INTO TimeSeason VALUES(2025,2,'D157',NULL);
INSERT INTO TimeSeason VALUES(2025,3,'D213',NULL);
INSERT INTO TimeSeason VALUES(2030,0,'D022',NULL);
INSERT INTO TimeSeason VALUES(2030,1,'D047',NULL);
INSERT INTO TimeSeason VALUES(2030,2,'D157',NULL);
INSERT INTO TimeSeason VALUES(2030,3,'D213',NULL);
INSERT INTO TimeSeason VALUES(2035,0,'D022',NULL);
INSERT INTO TimeSeason VALUES(2035,1,'D047',NULL);
INSERT INTO TimeSeason VALUES(2035,2,'D157',NULL);
INSERT INTO TimeSeason VALUES(2035,3,'D213',NULL);
INSERT INTO TimeSeason VALUES(2040,0,'D022',NULL);
INSERT INTO TimeSeason VALUES(2040,1,'D047',NULL);
INSERT INTO TimeSeason VALUES(2040,2,'D157',NULL);
INSERT INTO TimeSeason VALUES(2040,3,'D213',NULL);
INSERT INTO TimeSeason VALUES(2045,0,'D022',NULL);
INSERT INTO TimeSeason VALUES(2045,1,'D047',NULL);
INSERT INTO TimeSeason VALUES(2045,2,'D157',NULL);
INSERT INTO TimeSeason VALUES(2045,3,'D213',NULL);
INSERT INTO TimeSeason VALUES(2050,0,'D022',NULL);
INSERT INTO TimeSeason VALUES(2050,1,'D047',NULL);
INSERT INTO TimeSeason VALUES(2050,2,'D157',NULL);
INSERT INTO TimeSeason VALUES(2050,3,'D213',NULL);
CREATE TABLE TimeSeasonSequential
(
    period INTEGER
        REFERENCES TimePeriod (period),
    sequence INTEGER,
    seas_seq TEXT,
    season TEXT
        REFERENCES SeasonLabel (season),
    num_days REAL NOT NULL,
    notes TEXT,
    PRIMARY KEY (period, sequence, seas_seq, season),
    CHECK (num_days > 0)
);
INSERT INTO TimeSeasonSequential VALUES(2025,0,'S000','D022',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,1,'S001','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,2,'S002','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,3,'S003','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,4,'S004','D022',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,5,'S005','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,6,'S006','D213',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,7,'S007','D022',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,8,'S008','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,9,'S009','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,10,'S010','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,11,'S011','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,12,'S012','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,13,'S013','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,14,'S014','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,15,'S015','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,16,'S016','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,17,'S017','D022',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,18,'S018','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,19,'S019','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,20,'S020','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,21,'S021','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,22,'S022','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,23,'S023','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,24,'S024','D047',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,25,'S025','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,26,'S026','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,27,'S027','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,28,'S028','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,29,'S029','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,30,'S030','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,31,'S031','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,32,'S032','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,33,'S033','D047',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,34,'S034','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,35,'S035','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,36,'S036','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,37,'S037','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,38,'S038','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,39,'S039','D157',11.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,40,'S040','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,41,'S041','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,42,'S042','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,43,'S043','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,44,'S044','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,45,'S045','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,46,'S046','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,47,'S047','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,48,'S048','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,49,'S049','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,50,'S050','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,51,'S051','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,52,'S052','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,53,'S053','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,54,'S054','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,55,'S055','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,56,'S056','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,57,'S057','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,58,'S058','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,59,'S059','D157',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,60,'S060','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,61,'S061','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,62,'S062','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,63,'S063','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,64,'S064','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,65,'S065','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,66,'S066','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,67,'S067','D157',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,68,'S068','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,69,'S069','D157',7.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,70,'S070','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,71,'S071','D157',10.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,72,'S072','D213',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,73,'S073','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,74,'S074','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,75,'S075','D157',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,76,'S076','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,77,'S077','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,78,'S078','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,79,'S079','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,80,'S080','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,81,'S081','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,82,'S082','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,83,'S083','D157',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,84,'S084','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,85,'S085','D213',8.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,86,'S086','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,87,'S087','D213',9.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,88,'S088','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,89,'S089','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,90,'S090','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,91,'S091','D213',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,92,'S092','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,93,'S093','D213',11.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,94,'S094','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,95,'S095','D213',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,96,'S096','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,97,'S097','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,98,'S098','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,99,'S099','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,100,'S100','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,101,'S101','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,102,'S102','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,103,'S103','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,104,'S104','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,105,'S105','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,106,'S106','D213',6.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,107,'S107','D047',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,108,'S108','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,109,'S109','D213',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,110,'S110','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,111,'S111','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,112,'S112','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,113,'S113','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,114,'S114','D047',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,115,'S115','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,116,'S116','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,117,'S117','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,118,'S118','D047',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,119,'S119','D157',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,120,'S120','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,121,'S121','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,122,'S122','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,123,'S123','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,124,'S124','D047',6.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,125,'S125','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,126,'S126','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,127,'S127','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,128,'S128','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,129,'S129','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,130,'S130','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,131,'S131','D047',8.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,132,'S132','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,133,'S133','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,134,'S134','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,135,'S135','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,136,'S136','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,137,'S137','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,138,'S138','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,139,'S139','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,140,'S140','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,141,'S141','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,142,'S142','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,143,'S143','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,144,'S144','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,145,'S145','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,146,'S146','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,147,'S147','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,148,'S148','D022',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,149,'S149','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,150,'S150','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,151,'S151','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,152,'S152','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,153,'S153','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,154,'S154','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,155,'S155','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,156,'S156','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,157,'S157','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,158,'S158','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,159,'S159','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,160,'S160','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,161,'S161','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,162,'S162','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,163,'S163','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,164,'S164','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,165,'S165','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,166,'S166','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,167,'S167','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,168,'S168','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,169,'S169','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,170,'S170','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,171,'S171','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2025,172,'S172','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,0,'S000','D022',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,1,'S001','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,2,'S002','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,3,'S003','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,4,'S004','D022',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,5,'S005','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,6,'S006','D213',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,7,'S007','D022',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,8,'S008','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,9,'S009','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,10,'S010','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,11,'S011','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,12,'S012','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,13,'S013','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,14,'S014','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,15,'S015','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,16,'S016','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,17,'S017','D022',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,18,'S018','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,19,'S019','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,20,'S020','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,21,'S021','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,22,'S022','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,23,'S023','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,24,'S024','D047',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,25,'S025','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,26,'S026','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,27,'S027','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,28,'S028','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,29,'S029','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,30,'S030','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,31,'S031','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,32,'S032','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,33,'S033','D047',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,34,'S034','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,35,'S035','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,36,'S036','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,37,'S037','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,38,'S038','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,39,'S039','D157',11.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,40,'S040','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,41,'S041','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,42,'S042','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,43,'S043','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,44,'S044','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,45,'S045','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,46,'S046','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,47,'S047','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,48,'S048','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,49,'S049','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,50,'S050','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,51,'S051','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,52,'S052','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,53,'S053','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,54,'S054','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,55,'S055','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,56,'S056','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,57,'S057','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,58,'S058','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,59,'S059','D157',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,60,'S060','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,61,'S061','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,62,'S062','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,63,'S063','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,64,'S064','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,65,'S065','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,66,'S066','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,67,'S067','D157',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,68,'S068','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,69,'S069','D157',7.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,70,'S070','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,71,'S071','D157',10.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,72,'S072','D213',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,73,'S073','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,74,'S074','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,75,'S075','D157',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,76,'S076','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,77,'S077','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,78,'S078','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,79,'S079','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,80,'S080','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,81,'S081','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,82,'S082','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,83,'S083','D157',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,84,'S084','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,85,'S085','D213',8.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,86,'S086','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,87,'S087','D213',9.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,88,'S088','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,89,'S089','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,90,'S090','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,91,'S091','D213',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,92,'S092','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,93,'S093','D213',11.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,94,'S094','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,95,'S095','D213',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,96,'S096','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,97,'S097','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,98,'S098','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,99,'S099','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,100,'S100','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,101,'S101','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,102,'S102','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,103,'S103','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,104,'S104','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,105,'S105','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,106,'S106','D213',6.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,107,'S107','D047',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,108,'S108','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,109,'S109','D213',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,110,'S110','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,111,'S111','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,112,'S112','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,113,'S113','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,114,'S114','D047',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,115,'S115','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,116,'S116','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,117,'S117','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,118,'S118','D047',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,119,'S119','D157',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,120,'S120','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,121,'S121','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,122,'S122','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,123,'S123','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,124,'S124','D047',6.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,125,'S125','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,126,'S126','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,127,'S127','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,128,'S128','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,129,'S129','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,130,'S130','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,131,'S131','D047',8.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,132,'S132','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,133,'S133','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,134,'S134','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,135,'S135','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,136,'S136','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,137,'S137','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,138,'S138','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,139,'S139','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,140,'S140','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,141,'S141','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,142,'S142','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,143,'S143','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,144,'S144','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,145,'S145','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,146,'S146','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,147,'S147','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,148,'S148','D022',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,149,'S149','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,150,'S150','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,151,'S151','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,152,'S152','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,153,'S153','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,154,'S154','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,155,'S155','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,156,'S156','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,157,'S157','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,158,'S158','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,159,'S159','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,160,'S160','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,161,'S161','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,162,'S162','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,163,'S163','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,164,'S164','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,165,'S165','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,166,'S166','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,167,'S167','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,168,'S168','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,169,'S169','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,170,'S170','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,171,'S171','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2030,172,'S172','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,0,'S000','D022',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,1,'S001','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,2,'S002','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,3,'S003','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,4,'S004','D022',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,5,'S005','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,6,'S006','D213',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,7,'S007','D022',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,8,'S008','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,9,'S009','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,10,'S010','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,11,'S011','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,12,'S012','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,13,'S013','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,14,'S014','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,15,'S015','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,16,'S016','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,17,'S017','D022',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,18,'S018','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,19,'S019','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,20,'S020','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,21,'S021','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,22,'S022','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,23,'S023','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,24,'S024','D047',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,25,'S025','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,26,'S026','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,27,'S027','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,28,'S028','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,29,'S029','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,30,'S030','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,31,'S031','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,32,'S032','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,33,'S033','D047',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,34,'S034','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,35,'S035','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,36,'S036','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,37,'S037','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,38,'S038','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,39,'S039','D157',11.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,40,'S040','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,41,'S041','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,42,'S042','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,43,'S043','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,44,'S044','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,45,'S045','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,46,'S046','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,47,'S047','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,48,'S048','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,49,'S049','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,50,'S050','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,51,'S051','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,52,'S052','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,53,'S053','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,54,'S054','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,55,'S055','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,56,'S056','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,57,'S057','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,58,'S058','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,59,'S059','D157',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,60,'S060','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,61,'S061','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,62,'S062','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,63,'S063','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,64,'S064','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,65,'S065','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,66,'S066','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,67,'S067','D157',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,68,'S068','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,69,'S069','D157',7.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,70,'S070','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,71,'S071','D157',10.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,72,'S072','D213',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,73,'S073','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,74,'S074','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,75,'S075','D157',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,76,'S076','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,77,'S077','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,78,'S078','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,79,'S079','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,80,'S080','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,81,'S081','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,82,'S082','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,83,'S083','D157',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,84,'S084','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,85,'S085','D213',8.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,86,'S086','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,87,'S087','D213',9.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,88,'S088','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,89,'S089','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,90,'S090','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,91,'S091','D213',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,92,'S092','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,93,'S093','D213',11.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,94,'S094','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,95,'S095','D213',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,96,'S096','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,97,'S097','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,98,'S098','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,99,'S099','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,100,'S100','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,101,'S101','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,102,'S102','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,103,'S103','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,104,'S104','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,105,'S105','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,106,'S106','D213',6.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,107,'S107','D047',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,108,'S108','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,109,'S109','D213',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,110,'S110','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,111,'S111','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,112,'S112','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,113,'S113','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,114,'S114','D047',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,115,'S115','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,116,'S116','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,117,'S117','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,118,'S118','D047',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,119,'S119','D157',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,120,'S120','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,121,'S121','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,122,'S122','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,123,'S123','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,124,'S124','D047',6.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,125,'S125','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,126,'S126','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,127,'S127','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,128,'S128','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,129,'S129','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,130,'S130','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,131,'S131','D047',8.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,132,'S132','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,133,'S133','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,134,'S134','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,135,'S135','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,136,'S136','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,137,'S137','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,138,'S138','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,139,'S139','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,140,'S140','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,141,'S141','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,142,'S142','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,143,'S143','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,144,'S144','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,145,'S145','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,146,'S146','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,147,'S147','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,148,'S148','D022',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,149,'S149','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,150,'S150','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,151,'S151','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,152,'S152','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,153,'S153','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,154,'S154','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,155,'S155','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,156,'S156','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,157,'S157','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,158,'S158','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,159,'S159','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,160,'S160','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,161,'S161','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,162,'S162','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,163,'S163','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,164,'S164','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,165,'S165','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,166,'S166','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,167,'S167','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,168,'S168','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,169,'S169','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,170,'S170','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,171,'S171','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2035,172,'S172','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,0,'S000','D022',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,1,'S001','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,2,'S002','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,3,'S003','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,4,'S004','D022',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,5,'S005','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,6,'S006','D213',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,7,'S007','D022',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,8,'S008','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,9,'S009','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,10,'S010','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,11,'S011','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,12,'S012','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,13,'S013','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,14,'S014','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,15,'S015','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,16,'S016','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,17,'S017','D022',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,18,'S018','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,19,'S019','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,20,'S020','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,21,'S021','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,22,'S022','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,23,'S023','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,24,'S024','D047',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,25,'S025','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,26,'S026','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,27,'S027','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,28,'S028','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,29,'S029','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,30,'S030','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,31,'S031','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,32,'S032','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,33,'S033','D047',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,34,'S034','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,35,'S035','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,36,'S036','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,37,'S037','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,38,'S038','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,39,'S039','D157',11.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,40,'S040','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,41,'S041','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,42,'S042','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,43,'S043','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,44,'S044','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,45,'S045','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,46,'S046','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,47,'S047','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,48,'S048','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,49,'S049','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,50,'S050','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,51,'S051','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,52,'S052','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,53,'S053','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,54,'S054','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,55,'S055','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,56,'S056','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,57,'S057','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,58,'S058','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,59,'S059','D157',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,60,'S060','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,61,'S061','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,62,'S062','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,63,'S063','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,64,'S064','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,65,'S065','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,66,'S066','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,67,'S067','D157',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,68,'S068','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,69,'S069','D157',7.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,70,'S070','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,71,'S071','D157',10.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,72,'S072','D213',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,73,'S073','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,74,'S074','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,75,'S075','D157',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,76,'S076','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,77,'S077','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,78,'S078','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,79,'S079','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,80,'S080','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,81,'S081','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,82,'S082','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,83,'S083','D157',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,84,'S084','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,85,'S085','D213',8.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,86,'S086','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,87,'S087','D213',9.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,88,'S088','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,89,'S089','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,90,'S090','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,91,'S091','D213',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,92,'S092','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,93,'S093','D213',11.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,94,'S094','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,95,'S095','D213',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,96,'S096','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,97,'S097','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,98,'S098','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,99,'S099','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,100,'S100','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,101,'S101','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,102,'S102','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,103,'S103','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,104,'S104','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,105,'S105','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,106,'S106','D213',6.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,107,'S107','D047',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,108,'S108','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,109,'S109','D213',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,110,'S110','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,111,'S111','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,112,'S112','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,113,'S113','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,114,'S114','D047',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,115,'S115','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,116,'S116','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,117,'S117','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,118,'S118','D047',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,119,'S119','D157',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,120,'S120','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,121,'S121','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,122,'S122','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,123,'S123','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,124,'S124','D047',6.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,125,'S125','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,126,'S126','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,127,'S127','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,128,'S128','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,129,'S129','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,130,'S130','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,131,'S131','D047',8.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,132,'S132','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,133,'S133','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,134,'S134','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,135,'S135','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,136,'S136','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,137,'S137','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,138,'S138','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,139,'S139','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,140,'S140','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,141,'S141','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,142,'S142','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,143,'S143','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,144,'S144','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,145,'S145','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,146,'S146','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,147,'S147','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,148,'S148','D022',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,149,'S149','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,150,'S150','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,151,'S151','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,152,'S152','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,153,'S153','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,154,'S154','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,155,'S155','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,156,'S156','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,157,'S157','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,158,'S158','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,159,'S159','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,160,'S160','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,161,'S161','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,162,'S162','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,163,'S163','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,164,'S164','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,165,'S165','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,166,'S166','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,167,'S167','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,168,'S168','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,169,'S169','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,170,'S170','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,171,'S171','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2040,172,'S172','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,0,'S000','D022',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,1,'S001','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,2,'S002','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,3,'S003','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,4,'S004','D022',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,5,'S005','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,6,'S006','D213',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,7,'S007','D022',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,8,'S008','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,9,'S009','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,10,'S010','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,11,'S011','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,12,'S012','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,13,'S013','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,14,'S014','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,15,'S015','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,16,'S016','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,17,'S017','D022',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,18,'S018','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,19,'S019','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,20,'S020','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,21,'S021','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,22,'S022','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,23,'S023','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,24,'S024','D047',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,25,'S025','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,26,'S026','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,27,'S027','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,28,'S028','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,29,'S029','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,30,'S030','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,31,'S031','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,32,'S032','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,33,'S033','D047',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,34,'S034','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,35,'S035','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,36,'S036','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,37,'S037','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,38,'S038','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,39,'S039','D157',11.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,40,'S040','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,41,'S041','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,42,'S042','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,43,'S043','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,44,'S044','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,45,'S045','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,46,'S046','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,47,'S047','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,48,'S048','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,49,'S049','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,50,'S050','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,51,'S051','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,52,'S052','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,53,'S053','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,54,'S054','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,55,'S055','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,56,'S056','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,57,'S057','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,58,'S058','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,59,'S059','D157',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,60,'S060','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,61,'S061','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,62,'S062','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,63,'S063','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,64,'S064','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,65,'S065','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,66,'S066','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,67,'S067','D157',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,68,'S068','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,69,'S069','D157',7.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,70,'S070','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,71,'S071','D157',10.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,72,'S072','D213',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,73,'S073','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,74,'S074','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,75,'S075','D157',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,76,'S076','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,77,'S077','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,78,'S078','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,79,'S079','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,80,'S080','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,81,'S081','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,82,'S082','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,83,'S083','D157',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,84,'S084','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,85,'S085','D213',8.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,86,'S086','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,87,'S087','D213',9.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,88,'S088','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,89,'S089','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,90,'S090','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,91,'S091','D213',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,92,'S092','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,93,'S093','D213',11.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,94,'S094','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,95,'S095','D213',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,96,'S096','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,97,'S097','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,98,'S098','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,99,'S099','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,100,'S100','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,101,'S101','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,102,'S102','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,103,'S103','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,104,'S104','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,105,'S105','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,106,'S106','D213',6.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,107,'S107','D047',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,108,'S108','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,109,'S109','D213',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,110,'S110','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,111,'S111','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,112,'S112','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,113,'S113','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,114,'S114','D047',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,115,'S115','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,116,'S116','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,117,'S117','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,118,'S118','D047',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,119,'S119','D157',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,120,'S120','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,121,'S121','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,122,'S122','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,123,'S123','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,124,'S124','D047',6.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,125,'S125','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,126,'S126','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,127,'S127','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,128,'S128','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,129,'S129','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,130,'S130','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,131,'S131','D047',8.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,132,'S132','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,133,'S133','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,134,'S134','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,135,'S135','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,136,'S136','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,137,'S137','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,138,'S138','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,139,'S139','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,140,'S140','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,141,'S141','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,142,'S142','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,143,'S143','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,144,'S144','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,145,'S145','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,146,'S146','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,147,'S147','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,148,'S148','D022',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,149,'S149','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,150,'S150','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,151,'S151','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,152,'S152','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,153,'S153','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,154,'S154','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,155,'S155','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,156,'S156','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,157,'S157','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,158,'S158','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,159,'S159','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,160,'S160','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,161,'S161','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,162,'S162','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,163,'S163','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,164,'S164','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,165,'S165','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,166,'S166','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,167,'S167','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,168,'S168','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,169,'S169','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,170,'S170','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,171,'S171','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2045,172,'S172','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,0,'S000','D022',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,1,'S001','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,2,'S002','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,3,'S003','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,4,'S004','D022',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,5,'S005','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,6,'S006','D213',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,7,'S007','D022',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,8,'S008','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,9,'S009','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,10,'S010','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,11,'S011','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,12,'S012','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,13,'S013','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,14,'S014','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,15,'S015','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,16,'S016','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,17,'S017','D022',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,18,'S018','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,19,'S019','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,20,'S020','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,21,'S021','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,22,'S022','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,23,'S023','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,24,'S024','D047',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,25,'S025','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,26,'S026','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,27,'S027','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,28,'S028','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,29,'S029','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,30,'S030','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,31,'S031','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,32,'S032','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,33,'S033','D047',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,34,'S034','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,35,'S035','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,36,'S036','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,37,'S037','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,38,'S038','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,39,'S039','D157',11.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,40,'S040','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,41,'S041','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,42,'S042','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,43,'S043','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,44,'S044','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,45,'S045','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,46,'S046','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,47,'S047','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,48,'S048','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,49,'S049','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,50,'S050','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,51,'S051','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,52,'S052','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,53,'S053','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,54,'S054','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,55,'S055','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,56,'S056','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,57,'S057','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,58,'S058','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,59,'S059','D157',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,60,'S060','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,61,'S061','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,62,'S062','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,63,'S063','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,64,'S064','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,65,'S065','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,66,'S066','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,67,'S067','D157',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,68,'S068','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,69,'S069','D157',7.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,70,'S070','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,71,'S071','D157',10.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,72,'S072','D213',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,73,'S073','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,74,'S074','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,75,'S075','D157',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,76,'S076','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,77,'S077','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,78,'S078','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,79,'S079','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,80,'S080','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,81,'S081','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,82,'S082','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,83,'S083','D157',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,84,'S084','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,85,'S085','D213',8.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,86,'S086','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,87,'S087','D213',9.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,88,'S088','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,89,'S089','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,90,'S090','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,91,'S091','D213',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,92,'S092','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,93,'S093','D213',11.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,94,'S094','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,95,'S095','D213',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,96,'S096','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,97,'S097','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,98,'S098','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,99,'S099','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,100,'S100','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,101,'S101','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,102,'S102','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,103,'S103','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,104,'S104','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,105,'S105','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,106,'S106','D213',6.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,107,'S107','D047',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,108,'S108','D157',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,109,'S109','D213',5.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,110,'S110','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,111,'S111','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,112,'S112','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,113,'S113','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,114,'S114','D047',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,115,'S115','D157',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,116,'S116','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,117,'S117','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,118,'S118','D047',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,119,'S119','D157',4.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,120,'S120','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,121,'S121','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,122,'S122','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,123,'S123','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,124,'S124','D047',6.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,125,'S125','D157',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,126,'S126','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,127,'S127','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,128,'S128','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,129,'S129','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,130,'S130','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,131,'S131','D047',8.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,132,'S132','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,133,'S133','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,134,'S134','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,135,'S135','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,136,'S136','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,137,'S137','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,138,'S138','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,139,'S139','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,140,'S140','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,141,'S141','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,142,'S142','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,143,'S143','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,144,'S144','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,145,'S145','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,146,'S146','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,147,'S147','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,148,'S148','D022',3.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,149,'S149','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,150,'S150','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,151,'S151','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,152,'S152','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,153,'S153','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,154,'S154','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,155,'S155','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,156,'S156','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,157,'S157','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,158,'S158','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,159,'S159','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,160,'S160','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,161,'S161','D047',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,162,'S162','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,163,'S163','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,164,'S164','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,165,'S165','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,166,'S166','D022',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,167,'S167','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,168,'S168','D213',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,169,'S169','D047',1.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,170,'S170','D213',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,171,'S171','D022',2.0,'Reconstructed original year from clustering');
INSERT INTO TimeSeasonSequential VALUES(2050,172,'S172','D047',2.0,'Reconstructed original year from clustering');
CREATE TABLE TimePeriodType
(
    label       TEXT PRIMARY KEY,
    description TEXT
);
INSERT INTO TimePeriodType VALUES('e','existing vintages');
INSERT INTO TimePeriodType VALUES('f','future');
CREATE TABLE RPSRequirement
(
    region      TEXT    NOT NULL,
    period      INTEGER NOT NULL
        REFERENCES TimePeriod (period),
    tech_group  TEXT    NOT NULL,
    requirement REAL    NOT NULL,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    notes       TEXT,
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (region) REFERENCES Region (region),
    FOREIGN KEY (tech_group) REFERENCES TechGroup (group_name),
    PRIMARY KEY (region)
);
CREATE TABLE TechGroupMember
(
    group_name TEXT,
    tech       TEXT,
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    FOREIGN KEY (group_name) REFERENCES TechGroup (group_name),
    PRIMARY KEY (group_name, tech)
);
CREATE TABLE Technology
(
    tech         TEXT    NOT NULL,
    flag         TEXT    NOT NULL,
    sector       TEXT,
    category     TEXT,
    sub_category TEXT,
    unlim_cap    INTEGER NOT NULL DEFAULT 0,
    annual       INTEGER NOT NULL DEFAULT 0,
    reserve      INTEGER NOT NULL DEFAULT 0,
    curtail      INTEGER NOT NULL DEFAULT 0,
    retire       INTEGER NOT NULL DEFAULT 0,
    flex         INTEGER NOT NULL DEFAULT 0,
    exchange     INTEGER NOT NULL DEFAULT 0,
    seas_stor    INTEGER NOT NULL DEFAULT 0,
    description  TEXT,
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (flag) REFERENCES TechnologyType (label),
    PRIMARY KEY (tech)
);
INSERT INTO Technology VALUES('A_AGRI','p','agriculture','','',1,1,0,0,0,0,0,0,'Generic technology representing Agriculture','AGRIHR1');
CREATE TABLE DataSource
(
    source_id TEXT,
    source TEXT,
    notes TEXT,
    data_id TEXT
        REFERENCES DataSet (data_id),
    PRIMARY KEY (source_id)
);
INSERT INTO DataSource VALUES('[A1]','NRCan Comprehensive Database','Used the appropriate tables for each sector and province','AGRIHR1');
INSERT INTO DataSource VALUES('[A2]','Canada Energy Regulator Canada Energy Futures report','Global net zero macro-economics indicators','AGRIHR1');
INSERT INTO DataSource VALUES('[A3]','Statistics Canada 25-10-0029-01','Used presence/values to dictate sector presence in ATL','AGRIHR1');
CREATE TABLE DataQualityCredibility
(
    dq_cred INTEGER PRIMARY KEY,
    description TEXT
);
INSERT INTO DataQualityCredibility VALUES(1,'Excellent - A trustworthy source backed by strong analysis or direct measurements.');
INSERT INTO DataQualityCredibility VALUES(2,'Good - Trustworthy source. Partly based on assumptions or imperfect analysis.');
INSERT INTO DataQualityCredibility VALUES(3,'Acceptable - Acceptable source. May rely on many assumptions, shallow analysis, or rough measurement.');
INSERT INTO DataQualityCredibility VALUES(4,'Lacking - Questionable or unverified source. Poorly measured or weak analysis.');
INSERT INTO DataQualityCredibility VALUES(5,'Unacceptable - No or untrustworthy source. Unsupported assumption.');
CREATE TABLE DataQualityGeography
(
    dq_geog INTEGER PRIMARY KEY,
    description TEXT
);
INSERT INTO DataQualityGeography VALUES(1,'Excellent - From this region and at the correct aggregation level or a directly-applicable generic value.');
INSERT INTO DataQualityGeography VALUES(2,'Good - From an analogous region or the modelled region at incorrect aggregation level.');
INSERT INTO DataQualityGeography VALUES(3,'Acceptable - From a relevant but non-analogous region or highly aggregated.');
INSERT INTO DataQualityGeography VALUES(4,'Lacking - From a non-analogous region with limited relevance or a generic global value.');
INSERT INTO DataQualityGeography VALUES(5,'Unacceptable - From a region that is highly dissimilar to the modelled region, or from an unknown region.');
CREATE TABLE DataQualityStructure
(
    dq_struc INTEGER PRIMARY KEY,
    description TEXT
);
INSERT INTO DataQualityStructure VALUES(1,'Excellent - Excellent representation of the system, as good or better than other models.');
INSERT INTO DataQualityStructure VALUES(2,'Good - Well modelled, in line with what others are doing.');
INSERT INTO DataQualityStructure VALUES(3,'Acceptable - Room for improved representation but works for now.');
INSERT INTO DataQualityStructure VALUES(4,'Lacking - Poorly represented, overly simplified.');
INSERT INTO DataQualityStructure VALUES(5,'Unacceptable - Placeholder or dummy representation. Essentially not represented.');
CREATE TABLE DataQualityTechnology
(
    dq_tech INTEGER PRIMARY KEY,
    description TEXT
);
INSERT INTO DataQualityTechnology VALUES(1,'Excellent - For the modelled technology as represented. Directly applicable.');
INSERT INTO DataQualityTechnology VALUES(2,'Good - For the same general technology but not perfectly representative.');
INSERT INTO DataQualityTechnology VALUES(3,'Acceptable - For an analogous technology. Possibly a subset or general class. Roughly applicable.');
INSERT INTO DataQualityTechnology VALUES(4,'Lacking - Loosely representative. A niche subset or overbroad general class of the technology.');
INSERT INTO DataQualityTechnology VALUES(5,'Unacceptable - For a dissimilar or unknown technology. Unknown or poor applicability.');
CREATE TABLE DataQualityTime
(
    dq_time INTEGER PRIMARY KEY,
    description TEXT
);
INSERT INTO DataQualityTime VALUES(1,'Excellent - From or directly applicable to the modelled time.');
INSERT INTO DataQualityTime VALUES(2,'Good - From a different but similar time or only slightly out of date. Still highly relevant.');
INSERT INTO DataQualityTime VALUES(3,'Acceptable - From a somewhat similar time or several years out of date but still relevant.');
INSERT INTO DataQualityTime VALUES(4,'Lacking - From a time with different conditions or significantly out of date. Questionable relevance.');
INSERT INTO DataQualityTime VALUES(5,'Unacceptable - From an irrelevant time or badly out of date.');
CREATE TABLE DataSet
(
    data_id TEXT PRIMARY KEY,
    label TEXT,
    version TEXT,
    description TEXT,
    status TEXT,
    author TEXT,
    date TEXT,
    parent_id TEXT
        REFERENCES DataSet (data_id),
    changelog TEXT,
    notes TEXT
);
INSERT INTO DataSet VALUES('AGRIHRAB1','AB - Agriculture - high resolution','v1','2025 annual update','active','David Turnbull - david.turnbull1@ucalgary.ca','08-2025','','Original sector design','');
INSERT INTO DataSet VALUES('AGRIHRON1','ON - Agriculture - high resolution','v1','2025 annual update','active','David Turnbull - david.turnbull1@ucalgary.ca','08-2025','','Original sector design','');
INSERT INTO DataSet VALUES('AGRIHRBC1','BC - Agriculture - high resolution','v1','2025 annual update','active','David Turnbull - david.turnbull1@ucalgary.ca','08-2025','','Original sector design','');
INSERT INTO DataSet VALUES('AGRIHRMB1','MB - Agriculture - high resolution','v1','2025 annual update','active','David Turnbull - david.turnbull1@ucalgary.ca','08-2025','','Original sector design','');
INSERT INTO DataSet VALUES('AGRIHRSK1','SK - Agriculture - high resolution','v1','2025 annual update','active','David Turnbull - david.turnbull1@ucalgary.ca','08-2025','','Original sector design','');
INSERT INTO DataSet VALUES('AGRIHRQC1','QC - Agriculture - high resolution','v1','2025 annual update','active','David Turnbull - david.turnbull1@ucalgary.ca','08-2025','','Original sector design','');
INSERT INTO DataSet VALUES('AGRIHRPEI1','PEI - Agriculture - high resolution','v1','2025 annual update','active','David Turnbull - david.turnbull1@ucalgary.ca','08-2025','','Original sector design','');
INSERT INTO DataSet VALUES('AGRIHRNS1','NS - Agriculture - high resolution','v1','2025 annual update','active','David Turnbull - david.turnbull1@ucalgary.ca','08-2025','','Original sector design','');
INSERT INTO DataSet VALUES('AGRIHRNB1','NB - Agriculture - high resolution','v1','2025 annual update','active','David Turnbull - david.turnbull1@ucalgary.ca','08-2025','','Original sector design','');
INSERT INTO DataSet VALUES('AGRIHRNLLAB1','NLLAB - Agriculture - high resolution','v1','2025 annual update','active','David Turnbull - david.turnbull1@ucalgary.ca','08-2025','','Original sector design','');
COMMIT;
