-- ---------------------------------------------------------------------------
-- 03_create_hr_schema.sql
--
-- gvenzl/oracle-free executes .sql files in /container-entrypoint-initdb.d/
-- while connected to the app PDB (ORCLPDB1) as SYSDBA.
--
-- Creates the HR user, DEPARTMENTS and EMPLOYEES tables (matching the
-- table list in application-oracle.properties), inserts sample data, and
-- enables ALL-COLUMN supplemental logging on each table so that Debezium
-- captures full before/after row images.
-- ---------------------------------------------------------------------------

-- ── HR user ─────────────────────────────────────────────────────────────────
CREATE USER hr
  IDENTIFIED BY hr
  DEFAULT TABLESPACE   USERS
  TEMPORARY TABLESPACE TEMP
  QUOTA UNLIMITED ON   USERS;

GRANT CREATE SESSION   TO hr;
GRANT CREATE TABLE     TO hr;
GRANT CREATE SEQUENCE  TO hr;
GRANT CREATE TRIGGER   TO hr;
GRANT CREATE PROCEDURE TO hr;
GRANT CREATE VIEW      TO hr;

-- ── Sequences ───────────────────────────────────────────────────────────────
CREATE SEQUENCE hr.departments_seq
  START WITH     100
  INCREMENT BY   10
  NOCACHE;

CREATE SEQUENCE hr.employees_seq
  START WITH     1000
  INCREMENT BY   1
  NOCACHE;

-- ── DEPARTMENTS ─────────────────────────────────────────────────────────────
CREATE TABLE hr.departments (
  department_id   NUMBER(4)    NOT NULL,
  department_name VARCHAR2(30) NOT NULL,
  manager_id      NUMBER(6),
  location_id     NUMBER(4),
  CONSTRAINT departments_pk PRIMARY KEY (department_id)
);

ALTER TABLE hr.departments
  ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- ── EMPLOYEES ───────────────────────────────────────────────────────────────
CREATE TABLE hr.employees (
  employee_id    NUMBER(6)     NOT NULL,
  first_name     VARCHAR2(20),
  last_name      VARCHAR2(25)  NOT NULL,
  email          VARCHAR2(25)  NOT NULL,
  phone_number   VARCHAR2(20),
  hire_date      DATE          NOT NULL,
  job_id         VARCHAR2(10)  NOT NULL,
  salary         NUMBER(8,2),
  commission_pct NUMBER(2,2),
  manager_id     NUMBER(6),
  department_id  NUMBER(4),
  CONSTRAINT employees_pk          PRIMARY KEY (employee_id),
  CONSTRAINT employees_email_uk    UNIQUE (email),
  CONSTRAINT emp_dept_fk           FOREIGN KEY (department_id)
                                     REFERENCES hr.departments(department_id)
);

ALTER TABLE hr.employees
  ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- ── Sample data — departments ────────────────────────────────────────────────
INSERT INTO hr.departments VALUES (10,  'Administration',      200, 1700);
INSERT INTO hr.departments VALUES (20,  'Marketing',           201, 1800);
INSERT INTO hr.departments VALUES (30,  'Purchasing',          114, 1700);
INSERT INTO hr.departments VALUES (40,  'Human Resources',     203, 2400);
INSERT INTO hr.departments VALUES (50,  'Shipping',            121, 1500);
INSERT INTO hr.departments VALUES (60,  'IT',                  103, 1400);
INSERT INTO hr.departments VALUES (70,  'Public Relations',    204, 2700);
INSERT INTO hr.departments VALUES (80,  'Sales',               145, 2500);
INSERT INTO hr.departments VALUES (90,  'Executive',           100, 1700);
INSERT INTO hr.departments VALUES (100, 'Finance',             108, 1700);
INSERT INTO hr.departments VALUES (110, 'Accounting',          205, 1700);
INSERT INTO hr.departments VALUES (120, 'Treasury',            NULL, 1700);

-- ── Sample data — employees ──────────────────────────────────────────────────
INSERT INTO hr.employees VALUES (100,'Steven',   'King',      'SKING',    '515.123.4567', DATE '2003-06-17', 'AD_PRES', 24000, NULL, NULL, 90);
INSERT INTO hr.employees VALUES (101,'Neena',    'Kochhar',   'NKOCHHAR', '515.123.4568', DATE '2005-09-21', 'AD_VP',   17000, NULL,  100, 90);
INSERT INTO hr.employees VALUES (102,'Lex',      'De Haan',   'LDEHAAN',  '515.123.4569', DATE '2001-01-13', 'AD_VP',   17000, NULL,  100, 90);
INSERT INTO hr.employees VALUES (103,'Alexander','Hunold',    'AHUNOLD',  '590.423.4567', DATE '2006-01-03', 'IT_PROG', 9000,  NULL,  102, 60);
INSERT INTO hr.employees VALUES (104,'Bruce',    'Ernst',     'BERNST',   '590.423.4568', DATE '2007-05-21', 'IT_PROG', 6000,  NULL,  103, 60);
INSERT INTO hr.employees VALUES (107,'Diana',    'Lorentz',   'DLORENTZ', '590.423.5567', DATE '2007-02-07', 'IT_PROG', 4200,  NULL,  103, 60);
INSERT INTO hr.employees VALUES (108,'Nancy',    'Greenberg', 'NGREENBE', '515.124.4569', DATE '2002-08-17', 'FI_MGR',  12008, NULL,  101, 100);
INSERT INTO hr.employees VALUES (114,'Den',      'Raphaely',  'DRAPHEAL', '515.127.4561', DATE '2002-12-07', 'PU_MAN',  11000, NULL,  100, 30);
INSERT INTO hr.employees VALUES (121,'Adam',     'Fripp',     'AFRIPP',   '650.123.2234', DATE '2005-04-10', 'ST_MAN',  8200,  NULL,  100, 50);
INSERT INTO hr.employees VALUES (145,'John',     'Russell',   'JRUSSEL',  '011.44.1344.429268', DATE '2004-10-01', 'SA_MAN', 14000, 0.4, 100, 80);
INSERT INTO hr.employees VALUES (200,'Jennifer', 'Whalen',    'JWHALEN',  '515.123.4444', DATE '2003-09-17', 'AD_ASST', 4400,  NULL,  101, 10);
INSERT INTO hr.employees VALUES (201,'Michael',  'Hartstein', 'MHARTSTE', '515.123.5555', DATE '2004-02-17', 'MK_MAN',  13000, NULL,  100, 20);
INSERT INTO hr.employees VALUES (203,'Susan',    'Mavris',    'SMAVRIS',  '515.123.7777', DATE '2002-06-07', 'HR_REP',  6500,  NULL,  101, 40);
INSERT INTO hr.employees VALUES (204,'Hermann',  'Baer',      'HBAER',    '515.123.8888', DATE '2002-06-07', 'PR_REP',  10000, NULL,  101, 70);
INSERT INTO hr.employees VALUES (205,'Shelley',  'Higgins',   'SHIGGINS', '515.123.8080', DATE '2002-06-07', 'AC_MGR',  12008, NULL,  101, 110);

-- ── Grants to Debezium LogMiner user (after tables exist) ───────────────────
GRANT SELECT ON hr.departments TO c##dbzuser;
GRANT SELECT ON hr.employees   TO c##dbzuser;

COMMIT;

EXIT;
