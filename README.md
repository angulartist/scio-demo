## Playing w/ Spotify's Scio

* [X] Ingest [https://www.meetup.com/fr-FR/meetup_api/](https://www.meetup.com/fr-FR/meetup_api/)
* [X] Streaming processing w/ Apache-Beam X Scio
* [X] Query trending topics from BigQuery

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
