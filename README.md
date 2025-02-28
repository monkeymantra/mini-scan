## How to run

There are a few mains in this that I've used to do quick verifications.

One is in the processing directory under pkg/scanning/main.go. This is the main method
for starting up ingestors with a pool size of 3, of course to be set in the future.

The database used is Cassandra, and using `docker compose up` will bring up Cassandra and
by default will write to a `./data` directory as a volume mapped to `/opt/cassandra/data` in the container. 
If you want to clear out the data in the tables between runs to avoid duplicates being blocked, you'll need to drop the tables with a cassandra client or delete 
the `./data` directory.

If the `cmd/scanner/main` is running via docker-compose, you should start to see this kind of log:

```
2025/02/28 14:34:00 processing message 1408 from 1.1.1.163:%!s(uint32=35624) with service DNS
2025/02/28 14:34:00 Checking existing version for service DNS and ip 1.1.1.163 and port 35624 and timestamp 1740782040
2025/02/28 14:34:00 Version checked: 0
2025/02/28 14:34:00 Successfully processed message 1408
2025/02/28 14:34:01 processing message 1409 from 1.1.1.142:%!s(uint32=64171) with service SSH
2025/02/28 14:34:01 Checking existing version for service SSH and ip 1.1.1.142 and port 64171 and timestamp 1740782041
2025/02/28 14:34:01 Version checked: 0
2025/02/28 14:34:01 Successfully processed message 1409
2025/02/28 14:34:03 processing message 1411 from 1.1.1.125:%!s(uint32=39156) with service HTTP
2025/02/28 14:34:03 Checking existing version for service HTTP and ip 1.1.1.125 and port 39156 and timestamp 1740782043
2025/02/28 14:34:03 Version checked: 0
2025/02/28 14:34:03 Successfully processed message 1411
2025/02/28 14:34:04 processing message 1412 from 1.1.1.81:%!s(uint32=24417) with service SSH
2025/02/28 14:34:04 Checking existing version for service SSH and ip 1.1.1.81 and port 24417 and timestamp 1740782044
2025/02/28 14:34:04 Version checked: 0
2025/02/28 14:34:04 Successfully processed message 1412
```

You can query these messages from Cassandra, as demonstrated in the `scanning/retrieval/test/main.go`
file. 

That should get you a copy of the saved information.

---
### Notes about Persistence:

I spent a lot of time with **deduplicating** this data based on the **at-least-once** guarantees.
Also, I wanted to make sure that historical data is available. While I do not expose this feature, all versions
of the data with unique timestamps are saved, instead of just the latest. It would be trivial to write
a method that would deliver all versions of each scan between given timestamps, as timestamp is a partition key
for the version_counter table used to keep versions in sync.

I'm somewhat new to Cassandra, but it seemed like the best option to store high volumes of data.
Redis might be appropriate if the size of the individual data stored were lower, but I'm imagining that "hello world"
is a pretty small example, and the response data could be much, much larger in the real world. Cassandra seems ideal
for use in situations where the response size is much larger.

