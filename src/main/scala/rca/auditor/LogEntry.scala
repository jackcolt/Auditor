package rca.auditor

import org.joda.time.DateTime

import scala.beans.BeanProperty

class LogEntry (
                      linkageMetric: LinkageMetric,
                      message: String
                      ) extends Serializable {


  @BeanProperty val TimeStamp: String = DateTime.now().toString("yyyy-MM-dd'T'HH:mm:ss")

  @BeanProperty val TimeZone: String = DateTime.now().toString("z")

  @BeanProperty val dateTimeMillis: Long = DateTime.now.getMillis

  @BeanProperty val Message: String = message

  @BeanProperty val Metric: LinkageMetric = linkageMetric

}
