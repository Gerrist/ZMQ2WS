package ist.gerr

import io.ktor.network.sockets.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.cio.*
import io.ktor.server.routing.*
import io.ktor.server.websocket.*
import io.ktor.util.collections.*
import io.ktor.websocket.*
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runBlocking
import org.json.JSONObject
import org.zeromq.SocketType
import org.zeromq.ZMQ
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.collections.LinkedHashSet

fun main() {
    val url = System.getenv("URL")
    val topics = System.getenv("TOPICS")

    if (url == null || topics == null) {
        println("Please set the URL and TOPICS environment variables")
        return
    }

    val sessions = ConcurrentSet<WebSocketServerSession>()
    val queue = mutableListOf<String>()

    Timer().schedule(object : TimerTask() {
        override fun run() {
            sessions.forEach { session ->
                if (!session.isActive) {
                    sessions.remove(session)
                }
            }
        }
    }, 1000, 1000)

    Thread {
        val context = ZMQ.context(1)
        val socket = context.socket(SocketType.SUB)

        socket.connect(url)
        topics.split(",").forEach {
            socket.subscribe(it)
            println("Subscribed to $it")
        }

        while (!Thread.currentThread().isInterrupted) {
            val reply = socket.recv(0)
            if (queue.isEmpty()) {
                queue.add(reply.toString(StandardCharsets.UTF_8))
            } else {
                val topic = queue[0];
                sessions.forEach { session ->
                    runBlocking {
                        if (session.isActive) {
                            session.send(
                                JSONObject().put("topic", topic).put("message", reply.toString(StandardCharsets.UTF_8))
                                    .toString()
                            )
                        } else {
                            sessions.remove(session)
                        }
                    }
                }
                queue.clear()
            }
        }
    }.start()

    embeddedServer(CIO, port = 8080, host = "0.0.0.0") {
        install(WebSockets) {
            pingPeriod = Duration.ofSeconds(15)
            timeout = Duration.ofSeconds(15)
            maxFrameSize = Long.MAX_VALUE
            masking = false
        }

        routing {

            webSocket("/") { // websocketSession
                sessions.add(this)
                println("New session. Total sessions: ${sessions.size}")
            }
        }
    }.start(wait = true)
}
