package rca.auditor

import scala.beans.BeanProperty

class LinkageSource (var name:String) extends Serializable {


  @BeanProperty var population: Int = _
  @BeanProperty var eligiblePopulation: Int = _

}
