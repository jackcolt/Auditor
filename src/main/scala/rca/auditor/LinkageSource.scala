package rca.auditor

import scala.beans.BeanProperty

class LinkageSource (_name:String) extends Serializable {


  @BeanProperty var population: Long = _
  @BeanProperty var eligiblePopulation: Long = _
  @BeanProperty val name:String = _name

}
