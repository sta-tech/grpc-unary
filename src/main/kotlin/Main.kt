import io.grpc.Server
import io.grpc.ServerBuilder
import io.grpc.Status
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import ru.statech.*

class BankServer(private val port: Int) {
    val server: Server = ServerBuilder
        .forPort(port)
        .addService(BankService())
        .build()

    fun start() {
        server.start()
        println("Server started, listening on $port")
        Runtime.getRuntime().addShutdownHook(
            Thread {
                println("*** shutting down gRPC server since JVM is shutting down")
                this@BankServer.stop()
                println("*** server shut down")
            }
        )
    }

    private fun stop() {
        server.shutdown()
    }

    fun blockUntilShutdown() {
        server.awaitTermination()
    }

    internal class BankService : ru.statech.BankServiceGrpcKt.BankServiceCoroutineImplBase() {
        override suspend fun getBalance(request: BalanceCheckRequest): Balance {
            println("Requested balance for account: ${request.accountNumber}")
            return Balance.newBuilder().setAmount(100).build()
        }

        override fun withdraw(request: WithdrawRequest): Flow<Money> = flow {
            if (request.amount > 1000) {
                val status = Status.FAILED_PRECONDITION.withDescription("Max withdraw 1000")
                throw status.asRuntimeException()
            }
            for (i in 1..request.amount/100) {
                val money = Money.newBuilder().setValue(100).build()
                emit(money)
                delay(timeMillis = 1000L)
            }
        }

        override suspend fun cashDeposit(requests: Flow<DepositRequest>): Balance {
            var sum = 0
            requests.collect { request -> sum += request.amount }
            return Balance.newBuilder().setAmount(sum).build()
        }
    }
}

fun main() {
    val port = System.getenv("PORT")?.toInt() ?: 50051
    val server = BankServer(port)
    server.start()
    server.blockUntilShutdown()
}