```sh
# Playing w/ Spotify's Scio

# Using DirectRunner

# Build
$ sbt pack
# Run
$ target/pack/bin/main

# Just run
sbt run
```

```sql
# (BigQuery) Get top 5 trending topics within the last 5 minutes

SELECT SUM(score) as score, topic_name, timestamp
FROM `drawndom-app.meetups.trends`
WHERE timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
GROUP BY timestamp, topic_name
ORDER BY score DESC 
LIMIT 5
```
