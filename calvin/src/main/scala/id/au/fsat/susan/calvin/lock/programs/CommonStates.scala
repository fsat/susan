package id.au.fsat.susan.calvin.lock.programs

import id.au.fsat.susan.calvin.Id
import id.au.fsat.susan.calvin.lock.interpreters.requests.PendingRequestAlgebra
import id.au.fsat.susan.calvin.lock.interpreters.subscribers.SubscriberAlgebra

case class CommonStates(
  pendingRequestAlgebra: PendingRequestAlgebra[Id],
  subscriberAlgebra: SubscriberAlgebra[Id])
