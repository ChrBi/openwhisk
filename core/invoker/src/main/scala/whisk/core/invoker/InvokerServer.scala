package whisk.core.invoker
import akka.http.scaladsl.model.StatusCodes
import whisk.common.TransactionId
import whisk.core.connector.ActivationMessage
import whisk.http.BasicRasService
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._

/**
 * Implements web server to handle certain REST API calls.
 * Currently provides a health ping route, only.
 */
class InvokerServer(invoker: InvokerReactive) extends BasicRasService {
  override def routes(implicit transid: TransactionId) = super.routes ~ path("action") {
    post {
      entity(as[ActivationMessage]) { msg =>
        invoker.processActivationMessage(msg)
        complete(StatusCodes.Accepted)
      }
    }
  }
}
