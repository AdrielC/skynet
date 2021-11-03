package exd.fundamenski.skynet.util

import java.lang.Thread.UncaughtExceptionHandler
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{LinkedBlockingQueue, SynchronousQueue, ThreadFactory, ThreadPoolExecutor, TimeUnit}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{DurationInt, FiniteDuration}

package object threads {
  type ExceptionHandler = PartialFunction[(Thread, Throwable), Unit]

  private final class NamedThreadFactory (
   name: String,
   daemon: Boolean = false,
   priority: ThreadPriority = ThreadPriority.Norm,
   uncaughtExceptionHandler: ExceptionHandler = PartialFunction.empty
  ) extends ThreadFactory {

    private val parentGroup = Option(System.getSecurityManager)
      .fold(Thread.currentThread().getThreadGroup)(_.getThreadGroup)
    private val threadGroup = new ThreadGroup(parentGroup, name)
    private val threadCount = new AtomicInteger(1)

    private val uncaughtExHandler: UncaughtExceptionHandler = new UncaughtExceptionHandler {

      override def uncaughtException(t: Thread, e: Throwable): Unit =
        uncaughtExceptionHandler.orElse(fallthrough)((t, e))

      val fallthrough: PartialFunction[(Thread, Throwable), Unit] = {
        case (t, e) => threadGroup.uncaughtException(t, e)
      }
    }

    override def newThread(r: Runnable): Thread = {
      val newThreadNumber = threadCount.getAndIncrement()
      val thread = new Thread(threadGroup, r)
      thread.setName(s"$name-$newThreadNumber")
      thread.setDaemon(daemon)
      thread.setUncaughtExceptionHandler(uncaughtExHandler)
      thread.setPriority(priority.toInt)
      thread
    }
  }


  sealed abstract class ThreadPriority(val toInt: Int) extends Product with Serializable
  object ThreadPriority {
    case object Min extends ThreadPriority(Thread.MIN_PRIORITY)
    case object Norm extends ThreadPriority(Thread.NORM_PRIORITY)
    case object Max extends ThreadPriority(Thread.MAX_PRIORITY)
  }

  def threadFactory(name: String,
                    daemon: Boolean = false,
                    priority: ThreadPriority = ThreadPriority.Norm,
                    uncaughtExceptionHandler: ExceptionHandler = PartialFunction.empty
                   ): ThreadFactory =
    new NamedThreadFactory(name, daemon, priority, uncaughtExceptionHandler)

  def newDaemonPool(name: String,
                    min: Int = 4,
                    cpuFactor: Double = 3.0,
                    timeout: Boolean = false
                   ): ThreadPoolExecutor = {
    val cpus = Runtime.getRuntime.availableProcessors
    val exec = new ThreadPoolExecutor (
      math.max(min, cpus),
      math.max(min, (cpus * cpuFactor).ceil.toInt),
      10L,
      TimeUnit.SECONDS,
      new LinkedBlockingQueue[Runnable],
      threadFactory(name, daemon = true)
    )
    exec.allowCoreThreadTimeOut(timeout)
    exec
  }

  def newDaemonPoolExecutionContext(name: String,
                                     min: Int = 4,
                                     cpuFactor: Double = 3.0,
                                     timeout: Boolean = false): ExecutionContext =
    ExecutionContext.fromExecutorService(newDaemonPool(name, min, cpuFactor, timeout))

  def newBlockingPool(name: String,
                      corePoolSize: Int = 0,
                      maxPoolSize: Int = Int.MaxValue,
                      keepAliveTime: FiniteDuration = 1.minute
                     ): ThreadPoolExecutor =
    new ThreadPoolExecutor(
      corePoolSize,
      maxPoolSize,
      keepAliveTime.toSeconds,
      TimeUnit.SECONDS,
      new SynchronousQueue[Runnable](false),
      threadFactory(name, daemon = true)
    )

}
