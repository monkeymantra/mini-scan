CREATE KEYSPACE IF NOT EXISTS scans WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': '1'
};


USE scans;


CREATE TABLE IF NOT EXISTS scans (
                                     ip TEXT,
                                     port INT,
                                     service TEXT,
                                     timestamp BIGINT,
                                     version INT,
                                     data TEXT,
                                     PRIMARY KEY ((ip, port, service), version)
) WITH CLUSTERING ORDER BY (version DESC);

CREATE TABLE IF NOT EXISTS version_counter (
                                               ip TEXT,
                                               port INT,
                                               service TEXT,
                                               version INT,
                                               timestamp BIGINT,
                                               PRIMARY KEY ((ip, port, service), version)
) WITH CLUSTERING ORDER BY (version DESC);