package rca.auditor

import scala.beans.BeanProperty

class LinkageCharacteristic (_name: String, _hitScore: Double) extends Serializable {

  @BeanProperty var sample: Long = _
  @BeanProperty var methodology: String = _
  @BeanProperty var min: Double = _
  @BeanProperty var max: Double = _
  @BeanProperty var avg: Double = _
  @BeanProperty var stddev: Double = _
  @BeanProperty var mean: Double = _
  @BeanProperty var hits: Long = _
  @BeanProperty var name: String = _name
  @BeanProperty val hitScore: Double = _hitScore
}
