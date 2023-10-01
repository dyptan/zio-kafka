package zio.kafka.producer.pubsublite

import org.apache.kafka.clients.producer.{ ProducerRecord, RecordMetadata }
import org.apache.kafka.common.{ Metric, MetricName }
import zio.kafka.producer.ByteRecord
import zio.kafka.pubsublite.producer.Producer.Live
import zio.kafka.pubsublite.producer.Producer
import zio.kafka.serde.Serializer
import zio.{ Chunk, Promise, Queue, RIO, Scope, Task, ZIO }

object PubSubLiteProducer extends Producer {
  override def produce[R, K, V](
    record: ProducerRecord[K, V],
    keySerializer: Serializer[R, K],
    valueSerializer: Serializer[R, V]
  ): RIO[R, RecordMetadata] = PubSubLiteProducer.produce(record, keySerializer, valueSerializer)
  override def produce[R, K, V](
    topic: String,
    key: K,
    value: V,
    keySerializer: Serializer[R, K],
    valueSerializer: Serializer[R, V]
  ): RIO[R, RecordMetadata] = PubSubLiteProducer.produce(topic, key, value, keySerializer, valueSerializer)
  override def produceAsync[R, K, V](
    record: ProducerRecord[K, V],
    keySerializer: Serializer[R, K],
    valueSerializer: Serializer[R, V]
  ): RIO[R, Task[RecordMetadata]] = PubSubLiteProducer.produceAsync(record, keySerializer, valueSerializer)
  override def produceAsync[R, K, V](
    topic: String,
    key: K,
    value: V,
    keySerializer: Serializer[R, K],
    valueSerializer: Serializer[R, V]
  ): RIO[R, Task[RecordMetadata]] = PubSubLiteProducer.produceAsync(topic, key, value, keySerializer, valueSerializer)
  override def produceChunkAsync[R, K, V](
    records: Chunk[ProducerRecord[K, V]],
    keySerializer: Serializer[R, K],
    valueSerializer: Serializer[R, V]
  ): RIO[R, Task[Chunk[RecordMetadata]]] = PubSubLiteProducer.produceChunkAsync(records, keySerializer, valueSerializer)
  override def produceChunk[R, K, V](
    records: Chunk[ProducerRecord[K, V]],
    keySerializer: Serializer[R, K],
    valueSerializer: Serializer[R, V]
  ): RIO[R, Chunk[RecordMetadata]] = PubSubLiteProducer.produceChunk(records, keySerializer, valueSerializer)
  override def flush: Task[Unit]                      = PubSubLiteProducer.flush
  override def metrics: Task[Map[MetricName, Metric]] = PubSubLiteProducer.metrics

  def make(settings: ProducerSettings): ZIO[Scope, Throwable, Producer] =
    for {
      rawProducer <- ZIO.attempt(settings.driverSettings.instantiate())
      runtime     <- ZIO.runtime[Any]
      sendQueue <-
        Queue.bounded[(Chunk[ByteRecord], Promise[Throwable, Chunk[RecordMetadata]])](settings.sendBufferSize)
      producer <- ZIO.acquireRelease(ZIO.succeed(new Live(rawProducer, settings, runtime, sendQueue)))(_.close)
      _        <- ZIO.blocking(producer.sendFromQueue).forkScoped
    } yield producer
}
