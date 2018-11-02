package rca.auditor

import scala.beans.BeanProperty

class LinkageCharacteristic (var _name: String) extends Serializable {

  @BeanProperty var name: String = _name
  @BeanProperty var sample: Int = _
  @BeanProperty var methodology: String = _
  @BeanProperty var score: Float = _
  @BeanProperty var min: Float = _
  @BeanProperty var max: Float = _
  @BeanProperty var avg: Float = _
  @BeanProperty var stddev: Float = _
  @BeanProperty var mean: Float = _
}
