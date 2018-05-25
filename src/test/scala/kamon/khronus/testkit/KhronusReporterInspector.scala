package kamon.khronus.testkit

import com.despegar.khronus.jclient.KhronusClient
import com.typesafe.config.Config
import kamon.MetricReporter
import kamon.khronus.KhronusReporter
import kamon.metric.PeriodSnapshot


trait ReporterInspector { self: MetricReporter =>
  @volatile private var count = 0
  @volatile private var seenMetrics = Seq.empty[String]

  def register(snapshot: PeriodSnapshot): Unit = {
    import snapshot.metrics._
    count += 1
    seenMetrics = counters.map(_.name) ++ histograms.map(_.name) ++ gauges.map(_.name) ++ rangeSamplers.map(_.name)
  }

  def metrics(): Seq[String] = seenMetrics

  def snapshotCount(): Int = count
}

class KhronusReporterInspector(khronusClient: KhronusClient) extends KhronusReporter with ReporterInspector {

  override def reportPeriodSnapshot(snapshot: PeriodSnapshot): Unit = try {
    super.reportPeriodSnapshot(snapshot)
    register(snapshot)
  } catch {
    case exc: Throwable => logger.error(s"Register finished with error: ${exc.getMessage}", exc)
  }

  override protected def configureClient(config: Config): Unit = khronusClientOpt = Some(khronusClient)
}

object KhronusReporterInspector {
  def apply(khronusClient: KhronusClient): KhronusReporterInspector = new KhronusReporterInspector(khronusClient)
}
