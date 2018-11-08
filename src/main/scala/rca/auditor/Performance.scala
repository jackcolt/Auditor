package rca.auditor

import scala.beans.BeanProperty

class Performance (_accuracy: Double, _precision: Double, _recall: Double, _f1: Double, _falsePositiveRate: Double) extends Serializable {

  @BeanProperty val accuracy:Double=_accuracy

  @BeanProperty val precision:Double=_precision

  @BeanProperty val recall:Double=_recall

  @BeanProperty val f1:Double=_f1

  @BeanProperty val falsePositiveRate: Double = _falsePositiveRate

}
