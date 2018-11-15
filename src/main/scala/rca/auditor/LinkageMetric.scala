package rca.auditor

import org.joda.time.DateTime

import scala.beans.BeanProperty

class LinkageMetric (_name:String, _version: String) extends Serializable {

  @BeanProperty var characteristics: List[LinkageCharacteristic] = List()

  @BeanProperty var source: LinkageSource = _

  @BeanProperty var target: LinkageSource = _

  @BeanProperty val timeStamp: String = DateTime.now().toString("yyyy-MM-dd'T'HH:mm:ss")

  @BeanProperty val timeZone: String = DateTime.now().toString("z")

  @BeanProperty val dateTimeMillis: Long = DateTime.now.getMillis

  @BeanProperty val name=_name

  @BeanProperty var methodology: String = _

  @BeanProperty var label: LinkageCharacteristic = _

  @BeanProperty var performance: Performance = _

  @BeanProperty val version = _version


}