-- Auto-generated schema from Parser Microservice Models
-- Dialect: DuckDB

CREATE TABLE request_logs (
	id VARCHAR NOT NULL, 
	tenant_id VARCHAR NOT NULL,
	created_at TIMESTAMPTZ WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP, 
	source VARCHAR NOT NULL, 
	input_hash VARCHAR, 
	input_payload JSON, 
	output_payload JSON, 
	status VARCHAR, 
	parser_steps JSON, 
	PRIMARY KEY (id)
);
CREATE INDEX ix_request_logs_input_hash ON request_logs (input_hash);

CREATE TABLE file_parsing_configs (
	fingerprint VARCHAR NOT NULL, 
	tenant_id VARCHAR NOT NULL,
	format VARCHAR DEFAULT 'EXCEL', 
	header_row_index INTEGER DEFAULT 0, 
	columns_json JSON NOT NULL, 
	created_at TIMESTAMPTZ WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP, 
	updated_at TIMESTAMPTZ WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP, 
	PRIMARY KEY (fingerprint)
);

CREATE TABLE ai_configs (
	id VARCHAR NOT NULL DEFAULT 'default', 
	tenant_id VARCHAR NOT NULL,
	provider VARCHAR DEFAULT 'gemini', 
	api_key_enc VARCHAR, 
	model_name VARCHAR DEFAULT 'gemini-1.5-flash', 
	is_enabled BOOLEAN DEFAULT TRUE, 
	prompts_json JSON, 
	PRIMARY KEY (id)
);

CREATE TABLE pattern_rules (
	id VARCHAR NOT NULL, 
	tenant_id VARCHAR NOT NULL,
	source VARCHAR NOT NULL, 
	regex_pattern VARCHAR NOT NULL, 
	mapping_json JSON NOT NULL, 
	is_active BOOLEAN DEFAULT TRUE, 
	is_ai_generated BOOLEAN DEFAULT FALSE, 
	confidence JSON, 
	date_format VARCHAR,
	created_at TIMESTAMPTZ WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP, 
	PRIMARY KEY (id)
);

CREATE TABLE merchant_aliases (
	id VARCHAR NOT NULL, 
	tenant_id VARCHAR NOT NULL,
	pattern VARCHAR NOT NULL, 
	alias VARCHAR NOT NULL, 
	created_at TIMESTAMPTZ WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP, 
	PRIMARY KEY (id), 
	UNIQUE (pattern)
);

CREATE TABLE ai_call_cache (
	id VARCHAR NOT NULL, 
	content_hash VARCHAR NOT NULL, 
	source VARCHAR NOT NULL, 
	response_json JSON NOT NULL, 
	created_at TIMESTAMPTZ WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP, 
	PRIMARY KEY (id)
);
CREATE INDEX ix_ai_call_cache_content_hash ON ai_call_cache (content_hash);
