package id.au.fsat.susan.calvin

object StateTransition {

  case class Stay[A]() extends StateTransition[A] {
    override def pf: PartialFunction[A, StateTransition[A]] = {
      case _ => this
    }
    override def orElse(other: StateTransition[A]): StateTransition[A] = other
  }

  case class PartialFunctionStateTransition[A](pf: PartialFunction[A, StateTransition[A]]) extends StateTransition[A] {
    override def orElse(other: StateTransition[A]): StateTransition[A] = new PartialFunctionStateTransition(pf.orElse(other.pf))
  }

  def apply[A](pf: PartialFunction[A, StateTransition[A]]): StateTransition[A] = new PartialFunctionStateTransition(pf)
  def stay[A]: Stay[A] = Stay()
}

trait StateTransition[A] {
  def pf: PartialFunction[A, StateTransition[A]]
  def orElse(other: StateTransition[A]): StateTransition[A]
}

