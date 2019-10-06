package demo

// Describes a topic: urlkey is normalized, topic_name is used for display
case class Topic(urlkey: String, topic_name: String)

// Describes a group
case class Group(group_city: String,
                 group_country: String,
                 group_topics: List[Topic])

// Describes the RSVP event
case class RSVPEvent(eventId: Int, timestamp: Long, group: Group)
