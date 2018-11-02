package rca.auditor

import scala.beans.BeanProperty

class LinkageMetric (var _name:String) extends Serializable {

  @BeanProperty var name: String = _name

  @BeanProperty var characteristics: List[LinkageCharacteristic] = List()

  @BeanProperty var source: LinkageSource = _

  @BeanProperty var target: LinkageSource = _

}