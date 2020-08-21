package compstak.kafkastreams4s.vulcan.schema

import cats.effect.Sync
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import cats.implicits._
import vulcan.{Codec => VCodec}
import scala.jdk.CollectionConverters._

class AvroSerializer[A](c: VCodec[A]) {
  def createKafkaAvroSerializer[F[_]: Sync](
    schemaRegistryClient: SchemaRegistryClient,
    properties: Map[String, String],
    isKey: Boolean
  ): F[KafkaAvroSerializer] =
    Sync[F].delay {
      val s = new KafkaAvroSerializer(schemaRegistryClient)
      s.configure(properties.asJava, isKey)
      s
    }

  def using[F[_]: Sync](
    schemaRegistryClient: SchemaRegistryClient,
    properties: Map[String, String]
  ): KVSerializer[F, A] = {
    val createSerializer: Boolean => F[Serializer[F, A]] =
      createKafkaAvroSerializer(schemaRegistryClient, properties, _).map { kas =>
        Serializer.instance { (topic, a) =>
          Sync[F].suspend {
            c.encode(a) match {
              case Right(value) => Sync[F].pure(kas.serialize(topic, value))
              case Left(error) => Sync[F].raiseError(error.throwable)
            }
          }
        }
      }

    KVSerializer.instance(
      forKeyF = createSerializer(true),
      forValueF = createSerializer(false)
    )
  }
}
