-- Drop existing tables if they exist
DROP TABLE IF EXISTS sflw_recs;
DROP TABLE IF EXISTS ss_logs;
DROP TABLE IF EXISTS error_definitions;

-- Create the sflw_recs table
CREATE TABLE sflw_recs (
    "user" VARCHAR(255),                -- User who created the entry
    "dt_created" BIGINT,                -- Timestamp when the entry was created
    "dt_submitted" BIGINT,              -- Timestamp when the entry was submitted
    "ast_name" VARCHAR(255),            -- Asset name associated with the entry
    "location" VARCHAR(255),            -- Location information
    "status" VARCHAR(100),              -- Status of the entry
    "json_hash" VARCHAR(64),            -- Hash of the JSON data
    "local_id" VARCHAR(255),            -- Local identifier for the entry
    "filename" VARCHAR(255),            -- Name of the file associated with the entry
    "fnumber" VARCHAR(100),             -- File number or unique identifier
    "scan_time" VARCHAR(255)            -- String representing the scan time
);

-- Create the ss_logs table
CREATE TABLE ss_logs (
    id SERIAL PRIMARY KEY,              -- Unique identifier for each log entry
    job_name VARCHAR(255) NOT NULL,     -- Name of the job
    job_type VARCHAR(100),              -- Type/category of the job
    severity VARCHAR(1),                -- Severity level (I, W, E, S)
    status VARCHAR(50) NOT NULL,        -- Status of the job (e.g., IN PROGRESS, SUCCESS, FAILURE)
    start_time TIMESTAMP NOT NULL,      -- Start time of the job
    end_time TIMESTAMP,                 -- End time of the job
    message TEXT,                       -- Log message
    query TEXT,                         -- SQL query associated with the job
    user_id VARCHAR(255),               -- ID of the user who initiated the job
    host_name VARCHAR(255),             -- Hostname of the machine running the job
    metadata JSONB                      -- Additional metadata for the job
);

-- Create the error_definitions table
CREATE TABLE error_definitions (
    id SERIAL PRIMARY KEY,              -- Unique identifier for each error code
    symb VARCHAR(10) UNIQUE NOT NULL,   -- Symbolic name of the error (e.g., GS6782E)
    svrt VARCHAR(1) NOT NULL,           -- Severity (I, W, E, S)
    dscr TEXT NOT NULL,                 -- Description of the error
    oplk BOOLEAN DEFAULT FALSE,         -- Optional lock
    canl_bhvr VARCHAR(50),              -- Cancel behavior
    allw_canl_bhvr_chg BOOLEAN DEFAULT TRUE, -- Allow cancel behavior change
    dflt_svrt VARCHAR(1),               -- Default severity
    def_dscr TEXT,                      -- Default description
    cat VARCHAR(50),                    -- Category
    alt_svrt VARCHAR(1),                -- Alternate severity
    agcy_cnfg_fl BOOLEAN DEFAULT FALSE, -- Agency configuration flag
    alt_symb VARCHAR(10),               -- Alternate symbol
    supp_pblm_fl BOOLEAN DEFAULT FALSE, -- Suppress problem flag
    enbl_ovrd_jtfn BOOLEAN DEFAULT TRUE, -- Enable override justification
    supp_pblm_on_dltn_fl BOOLEAN DEFAULT FALSE -- Suppress problem on deletion flag
);

-- Insert initial error definitions
INSERT INTO error_definitions (symb, svrt, dscr, cat, dflt_svrt) VALUES
('GS1001I', 'I', 'Informational message for process start', 'General', 'I'),
('GS2001W', 'W', 'Warning: No records to insert', 'Data', 'W'),
('GS2002E', 'E', 'Error: Batch insert failed due to %1', 'Database', 'E'),
('GS3001S', 'S', 'Severe: System encountered an unexpected failure', 'System', 'S');