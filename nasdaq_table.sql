
CREATE TABLE IF NOT EXISTS marianolopez7749_coderhouse.nasdaq_table
(
	date DATE NOT NULL  ENCODE az64
	,open NUMERIC(18,0) NOT NULL  ENCODE az64
	,high NUMERIC(18,0) NOT NULL  ENCODE az64
	,low NUMERIC(18,0) NOT NULL  ENCODE az64
	,close NUMERIC(18,0) NOT NULL  ENCODE az64
	,volumen NUMERIC(18,0) NOT NULL  ENCODE az64
	,ex_dividend NUMERIC(18,0) NOT NULL  ENCODE az64
	,split_ratio NUMERIC(18,0) NOT NULL  ENCODE az64
	,adj_open NUMERIC(18,0) NOT NULL  ENCODE az64
	,adj_high NUMERIC(18,0) NOT NULL  ENCODE az64
	,adj_low NUMERIC(18,0) NOT NULL  ENCODE az64
	,adj_close NUMERIC(18,0) NOT NULL  ENCODE az64
	,adj_volumen NUMERIC(18,0) NOT NULL  ENCODE az64
)
DISTSTYLE AUTO
;
ALTER TABLE marianolopez7749_coderhouse.nasdaq_table owner to marianolopez7749_coderhouse;