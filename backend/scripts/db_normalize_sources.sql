-- Normalize source aliases in data_rows to reduce graph mismatches.
-- Run against the same database used by backend.

BEGIN;

UPDATE data_rows
SET source = 'godrej'
WHERE lower(trim(source)) IN ('goodrej', 'goddrej');

UPDATE data_rows
SET source = 'samsung_vs'
WHERE lower(trim(source)) IN ('samsung_vijay_sales');

UPDATE data_rows
SET source = 'reliance'
WHERE lower(trim(source)) IN ('reliance resq', 'reliance_resq', 'reliance-resq', 'resq');

COMMIT;
