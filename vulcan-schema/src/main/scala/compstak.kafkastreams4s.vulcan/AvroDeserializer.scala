package compstak.kafkastreams4s.vulcan.schema

import java.nio.ByteBuffer

import cats.effect.Sync
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import vulcan.{Codec => VCodec}
import cats.implicits._
import scala.jdk.CollectionConverters._

class AvroDeserializer[A](c: VCodec[A]) {

  def createAvroDeserializer[F[_]: Sync](
    schemaRegistryClient: SchemaRegistryClient,
    isKey: Boolean,
    properties: Map[String, String]
  ): F[KafkaAvroDeserializer] =
    Sync[F].delay {
      val d = new KafkaAvroDeserializer(schemaRegistryClient)
      d.configure(properties.asJava, isKey)
      d
    }

  def using[F[_]: Sync](
    schemaRegistryClient: SchemaRegistryClient,
    properties: Map[String, String]
  ): KVDeserializer[F, A] =
    c.schema match {
      case Right(schema) =>
        val createDeserializer: Boolean => F[Deserializer[F, A]] =
          createAvroDeserializer(schemaRegistryClient, _, properties).map { kad =>
            Deserializer.instance { (topic, bytes) =>
              val writerSchemaId = ByteBuffer.wrap(bytes).getInt(1) // skip magic byte
              val writerSchema = schemaRegistryClient.getById(writerSchemaId)
              Sync[F].suspend {
                c.decode(kad.deserialize(topic, bytes, schema), writerSchema) match {
                  case Right(a) => Sync[F].pure(a)
                  case Left(error) => Sync[F].raiseError(error.throwable)
                }
              }
            }
          }

        KVDeserializer.instance(
          forKeyF = createDeserializer(true),
          forValueF = createDeserializer(false)
        )
      case Left(error) => KVDeserializer.const(Sync[F].raiseError(error.throwable))
    }
}
