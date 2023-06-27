package zio.kafka.pubsublite.producer

import com.google.cloud.pubsublite._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata, Producer => JProducer}
import org.apache.kafka.common.{Metric, MetricName}
import zio._
import zio.kafka.producer.ByteRecord
import zio.kafka.serde.Serializer
import zio.stream.ZPipeline

import scala.jdk.CollectionConverters._


trait Producer {

  /**
   * Produces a single record and await broker acknowledgement. See [[produceAsync[R,K,V](record*]] for version that
   * allows to avoid round-trip-time penalty for each record.
   */
  def produce[R, K, V](
    record: ProducerRecord[K, V],
  ): RIO[R, RecordMetadata]

  /**
   * Produces a single record and await broker acknowledgement. See [[produceAsync[R,K,V](topic:String*]] for version
   * that allows to avoid round-trip-time penalty for each record.
   */
  def produce[R, K, V](
    topic: String,
    key: K,
    value: V
  ): RIO[R, RecordMetadata]


  /**
   * Produces a single record. The effect returned from this method has two layers and describes the completion of two
   * actions:
   *   1. The outer layer describes the enqueueing of the record to the Producer's internal buffer. 2. The inner layer
   *      describes receiving an acknowledgement from the broker for the transmission of the record.
   *
   * It is usually recommended to not await the inner layer of every individual record, but enqueue a batch of records
   * and await all of their acknowledgements at once. That amortizes the cost of sending requests to Kafka and increases
   * throughput. See [[produce[R,K,V](record*]] for version that awaits broker acknowledgement.
   */
  def produceAsync[R, K, V](
    record: ProducerRecord[K, V]
  ): RIO[R, Task[RecordMetadata]]

  /**
   * Produces a single record. The effect returned from this method has two layers and describes the completion of two
   * actions:
   *   1. The outer layer describes the enqueueing of the record to the Producer's internal buffer. 2. The inner layer
   *      describes receiving an acknowledgement from the broker for the transmission of the record.
   *
   * It is usually recommended to not await the inner layer of every individual record, but enqueue a batch of records
   * and await all of their acknowledgements at once. That amortizes the cost of sending requests to Kafka and increases
   * throughput. See [[produce[R,K,V](topic*]] for version that awaits broker acknowledgement.
   */
  def produceAsync[R, K, V](
    topic: String,
    key: K,
    value: V
  ): RIO[R, Task[RecordMetadata]]

  /**
   * Produces a chunk of records. The effect returned from this method has two layers and describes the completion of
   * two actions:
   *   1. The outer layer describes the enqueueing of all the records to the Producer's internal buffer. 2. The inner
   *      layer describes receiving an acknowledgement from the broker for the transmission of the records.
   *
   * It is possible that for chunks that exceed the producer's internal buffer size, the outer layer will also signal
   * the transmission of part of the chunk. Regardless, awaiting the inner layer guarantees the transmission of the
   * entire chunk.
   */
  def produceChunkAsync[R, K, V](
    records: Chunk[ProducerRecord[K, V]]
  ): RIO[R, Task[Chunk[RecordMetadata]]]

  /**
   * Produces a chunk of records. See [[produceChunkAsync]] for version that allows to avoid round-trip-time penalty for
   * each chunk.
   */
  def produceChunk[R, K, V](
    records: Chunk[ProducerRecord[K, V]]
  ): RIO[R, Chunk[RecordMetadata]]

  /**
   * Flushes the producer's internal buffer. This will guarantee that all records currently buffered will be transmitted
   * to the broker.
   */
  def flush: Task[Unit]

  /**
   * Expose internal producer metrics
   */
  def metrics: Task[Map[MetricName, Metric]]
}

object Producer {

  private[producer] final class Live(
    private[producer] val p: JProducer[Array[Byte], Array[Byte]],
    producerSettings: ProducerSettings,
    runtime: Runtime[Any],
    sendQueue: Queue[(Chunk[ByteRecord], Promise[Throwable, Chunk[RecordMetadata]])]
  ) extends Producer {

    override def produceAsync[R, K, V](
      record: ProducerRecord[K, V]
    ): RIO[R, Task[RecordMetadata]] =
      for {
        done             <- Promise.make[Throwable, Chunk[RecordMetadata]]
        _                <- sendQueue.offer((Chunk.single(record), done))
      } yield done.await.map(_.head)

    override def produceChunkAsync[R, K, V](
      records: Chunk[ProducerRecord[K, V]]
    ): RIO[R, Task[Chunk[RecordMetadata]]] =
      if (records.isEmpty) ZIO.succeed(ZIO.succeed(Chunk.empty))
      else {
        for {
          done              <- Promise.make[Throwable, Chunk[RecordMetadata]]
          _                 <- sendQueue.offer((records, done))
        } yield done.await
      }

    /**
     * Calls to send may block when updating metadata or when communication with the broker is (temporarily) lost,
     * therefore this stream is run on a the blocking thread pool
     */

    override def produce[R, K, V](
      record: ProducerRecord[K, V]
    ): RIO[R, RecordMetadata] =
      produceAsync(record).flatten

    override def produce[R, K, V](
      topic: String,
      key: K,
      value: V
    ): RIO[R, RecordMetadata] =
      produce(new ProducerRecord(topic, key, value))

    override def produceAsync[R, K, V](
      topic: String,
      key: K,
      value: V
    ): RIO[R, Task[RecordMetadata]] =
      produceAsync(new ProducerRecord(topic, key, value))

    override def produceChunk[R, K, V](
      records: Chunk[ProducerRecord[K, V]]
    ): RIO[R, Chunk[RecordMetadata]] =
      produceChunkAsync(records).flatten

    override def flush: Task[Unit] = ZIO.attemptBlocking(p.flush())

    override def metrics: Task[Map[MetricName, Metric]] = ZIO.attemptBlocking(p.metrics().asScala.toMap)

    private[producer] def close: UIO[Unit] = ZIO.attemptBlocking(p.close(producerSettings.closeTimeout)).orDie
  }

  val live: RLayer[ProducerSettings, Producer] =
    ZLayer.scoped {
      for {
        settings <- ZIO.service[ProducerSettings]
        producer <- make(settings)
      } yield producer
    }

  def make(settings: ProducerSettings): ZIO[Scope, Throwable, Producer] =
    for {
      rawProducer <- ZIO.attempt(settings.driverSettings.instantiate())
      runtime <- ZIO.runtime[Any]
      sendQueue <-
        Queue.bounded[(Chunk[ByteRecord], Promise[Throwable, Chunk[RecordMetadata]])](settings.sendBufferSize)
      producer <- ZIO.acquireRelease(ZIO.succeed(new Live(rawProducer, settings, runtime, sendQueue)))(_.close)
    } yield producer

  /**
   * Accessor method for [[Producer!.produce[R,K,V](record*]]
   */
  def produce[R, K, V](
    record: ProducerRecord[K, V]
  ): RIO[R & Producer, RecordMetadata] =
    ZIO.serviceWithZIO[Producer](_.produce(record))

  /**
   * Accessor method for [[Producer!.produce[R,K,V](topic*]]
   */
  def produce[R, K, V](
    topic: String,
    key: K,
    value: V
  ): RIO[R & Producer, RecordMetadata] =
    ZIO.serviceWithZIO[Producer](_.produce(topic, key, value))

  /**
   * Accessor method for [[Producer!.produceAsync[R,K,V](record*]]
   */
  def produceAsync[R, K, V](
    record: ProducerRecord[K, V]
  ): RIO[R & Producer, Task[RecordMetadata]] =
    ZIO.serviceWithZIO[Producer](_.produceAsync(record))

  /**
   * Accessor method for [[Producer.produceAsync[R,K,V](topic*]]
   */
  def produceAsync[R, K, V](
    topic: String,
    key: K,
    value: V
  ): RIO[R & Producer, Task[RecordMetadata]] =
    ZIO.serviceWithZIO[Producer](_.produceAsync(topic, key, value))

  /**
   * Accessor method for [[Producer.produceChunkAsync]]
   */
  def produceChunkAsync[R, K, V](
    records: Chunk[ProducerRecord[K, V]]
  ): RIO[R & Producer, Task[Chunk[RecordMetadata]]] =
    ZIO.serviceWithZIO[Producer](_.produceChunkAsync(records))

  /**
   * Accessor method for [[Producer.produceChunk]]
   */
  def produceChunk[R, K, V](
    records: Chunk[ProducerRecord[K, V]]
  ): RIO[R & Producer, Chunk[RecordMetadata]] =
    ZIO.serviceWithZIO[Producer](_.produceChunk(records))

  /**
   * Accessor method for [[Producer.flush]]
   */
  val flush: RIO[Producer, Unit] = ZIO.serviceWithZIO(_.flush)

  /**
   * Accessor method for [[Producer.metrics]]
   */
  val metrics: RIO[Producer, Map[MetricName, Metric]] = ZIO.serviceWithZIO(_.metrics)

  /** Used to prevent warnings about not using the result of an expression. */
  @inline private def exec[A](f: => A): Unit = {
    val _ = f
  }

}
