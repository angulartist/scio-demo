package demo

/* Describes an event */
case class DataEvent(userId: String,
                     server: String,
                     experience: Int,
                     timestamp: Int)
