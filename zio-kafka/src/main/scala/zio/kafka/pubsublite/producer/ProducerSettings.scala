package zio.kafka.pubsublite.producer

import com.google.cloud.pubsublite.kafka.{ ProducerSettings => GProducerSettings }
import com.google.cloud.pubsublite.{ CloudZone, ProjectNumber, TopicName, TopicPath }
import zio._
final case class ProducerSettings(
  gcpLocation: String,
  gcpProjectNumber: Long,
  gcpTopicName: String,
  closeTimeout: Duration,
  sendBufferSize: Int,
  properties: Map[String, AnyRef]
) {

  def driverSettings: GProducerSettings = {
    val topic = TopicPath
      .newBuilder()
      .setLocation(CloudZone.parse(gcpLocation))
      .setProject(ProjectNumber.of(gcpProjectNumber))
      .setName(TopicName.of(gcpTopicName))
      .build();

    GProducerSettings
      .newBuilder()
      .setTopicPath(topic)
      .build();
  }

  def withCloseTimeout(duration: Duration): ProducerSettings =
    copy(closeTimeout = duration)

  def withProperty(key: String, value: AnyRef): ProducerSettings =
    copy(properties = properties + (key -> value))

  def withProperties(kvs: (String, AnyRef)*): ProducerSettings =
    withProperties(kvs.toMap)

  def withProperties(kvs: Map[String, AnyRef]): ProducerSettings =
    copy(properties = properties ++ kvs)

}

object ProducerSettings {
  def apply(gcpLocation: String, gcpProjectNumber: Long, gcpTopicName: String): ProducerSettings =
    new ProducerSettings(gcpLocation: String, gcpProjectNumber: Long, gcpTopicName: String, 30.seconds, 4096, Map.empty)
}
