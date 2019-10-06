package demo

/* Describes an event */
case class DataEvent(userId: String,
                     server: String,
                     experience: Int,
                     timestamp: Int)

case class Topic(urlkey: String, topic_name: String)

case class Group(group_city: String,
                 group_country: String,
                 group_topics: List[Topic])

case class RSVPEvent(eventId: Int, timestamp: Long, group: Group)
