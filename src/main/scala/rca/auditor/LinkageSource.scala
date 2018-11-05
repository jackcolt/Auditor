package rca.auditor

import scala.beans.BeanProperty

class LinkageSource (_name:String) extends Serializable {


  @BeanProperty var population: Int = _
  @BeanProperty var eligiblePopulation: Int = _
  @BeanProperty val name:String = _name

}
