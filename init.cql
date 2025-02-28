CREATE KEYSPACE IF NOT EXISTS scans WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': '1'
};


USE scans;

-- Table for storing all versions of Scan records.
CREATE TABLE IF NOT EXISTS scans (
                       ip TEXT,
                       port INT,
                       service TEXT,
                       timestamp BIGINT,
                       version INT,
                       data TEXT,
                       PRIMARY KEY ((ip, port, service), version)
    /* I wrestled with making timestamp part of the primary
    * key. Or, heck, maybe making data_version part of the primary key, as it's easier to predict
    and just iterate down from the latest. Still, it makes querying for it more expensive as we would have to
    manually count down versions. So many possible directions to go with the duplication issue.
    */
) WITH CLUSTERING ORDER BY (version DESC);

-- Counter table for tracking the latest version.
CREATE TABLE IF NOT EXISTS version_counter (
                                 ip TEXT,
                                 port INT,
                                 service TEXT,
                                 version COUNTER,
                                 timestamp BIGINT,
                                 PRIMARY KEY ((ip, port, service), timestamp)) WITH CLUSTERING ORDER BY (timestamp DESC);


