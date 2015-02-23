CREATE EXTERNAL TABLE ${0} (
remote_addr TEXT,
logname TEXT,
remote_user TEXT,
access_timestamp TIMESTAMP,
request_method TEXT,
request_path TEXT,
http_version TEXT,
response_status INT,
transferred_bytes INT,
ANY_THING TEXT,
user_agent TEXT)
USING TEXT WITH ('text.serde'='org.apache.tajo.storage.text.ApacheLogLineSerDe','text.delimiter' = ' ') LOCATION ${table.path};