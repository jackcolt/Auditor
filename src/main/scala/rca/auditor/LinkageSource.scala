package rca.auditor

import scala.beans.BeanProperty

class LinkageSource extends Serializable {


  @BeanProperty var name: String = _
  @BeanProperty var population: Int = _
  @BeanProperty var eligiblePopulation: Int = _

}
