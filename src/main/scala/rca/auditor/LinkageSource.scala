package rca.auditor

import scala.beans.BeanProperty

class LinkageSource (var name:String) extends Serializable {


  @BeanProperty val population: Int = _
  @BeanProperty val eligiblePopulation: Int = _

}
