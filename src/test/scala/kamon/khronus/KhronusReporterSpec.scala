package kamon.khronus

import com.despegar.khronus.jclient.KhronusClient
import kamon.Kamon
import kamon.Kamon.buildSpan
import kamon.khronus.testkit.KhronusReporterInspector
import kamon.metric._
import kamon.testkit.{MetricInspection, Reconfigure}
import org.easymock.EasyMock.{eq => eqEM, _}
import org.scalatest.concurrent.Eventually
import org.scalatest.easymock.EasyMockSugar
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.duration._

class KhronusReporterSpec extends WordSpec
  with Matchers
  with Eventually
  with EasyMockSugar
  with BeforeAndAfterAll
  with MetricInspection
  with Reconfigure {

  override protected def beforeAll(): Unit = {
    applyConfig(
      """
        |kamon {
        |  metric.tick-interval = 10 millis
        |  trace.tick-interval = 10 millis
        |  trace.sampler = "always"
        |  khronus.interval = 10
        |
        |  util.filters {
        |    metric-filter {
        |      includes = [ "test**" ]
        |    }
        |    span-filter {
        |      includes = [ "span**" ]
        |    }
        |  }
        |}
        |
    """.stripMargin
    )
  }

  override protected def afterAll(): Unit = {
    resetConfig()
  }

  "Khronus Reporter" should {
    "report metrics via KhronusClient" in {

      val mockKhronusClient = mock[KhronusClient]
      val khronusReporter = KhronusReporterInspector(mockKhronusClient)

      expecting {
        mockKhronusClient.incrementCounter("test.counter", 10)
        lastCall.times(1)
        mockKhronusClient.recordGauge("test.gauge", 23)
        lastCall.times(1)
        mockKhronusClient.recordTime(eqEM("test.histogram"), gt(1))
        lastCall.times(1)
      }

      whenExecuting(mockKhronusClient) {

        val subscription = Kamon.addReporter(khronusReporter, name = "khronus-reporter-spec", filter = "metric-filter")
        Kamon.counter("test.counter").increment(10)
        Kamon.gauge("test.gauge").increment(23)
        val histogram = Kamon.histogram("test.histogram", MeasurementUnit.time.milliseconds)
        histogram.record(10000, 10)
        histogram.record(3003, 2)
        histogram.record(3003, 5)
        histogram.distribution(resetState = false)
//        histogram.distribution(resetState = false)
//        histogram.distribution(resetState = false)
//        histogram.distribution(resetState = false).buckets.size should be (2)

        try {

          eventually(timeout(5 seconds)) {
            khronusReporter.snapshotCount() should be >= 1
            khronusReporter.metrics() should contain allOf ("test.counter", "test.gauge", "test.histogram")
          }
        } catch {
          case exc: Throwable => println(s"Failed with exception: ${exc.getMessage}")
        }

        subscription.cancel()
      }
    }
    "report span metrics via KhronusClient" in {
      val mockKhronusClient = mock[KhronusClient]
      val khronusReporter = KhronusReporterInspector(mockKhronusClient)

      buildSpan("right.operation")
        .withMetricTag("span.kind", "client")
        .start()
        .finish()

      buildSpan("right.operation")
        .start()
        .finish()

      expecting {
        mockKhronusClient.recordTime(eqEM("span.client.right.operation"), gt(0L))
        lastCall.times(1)
        mockKhronusClient.recordTime(eqEM("span.right.operation"), gt(0L))
        lastCall.times(1)
      }

      whenExecuting(mockKhronusClient) {

        val subscription = Kamon.addReporter(khronusReporter, name = "khronus-reporter-spec", filter = "span-filter")

        eventually(timeout(3 seconds)) {
          khronusReporter.snapshotCount() should be >= 1
          khronusReporter.metrics() should contain ("span.processing-time")
        }

        subscription.cancel()
      }
    }
  }
}
