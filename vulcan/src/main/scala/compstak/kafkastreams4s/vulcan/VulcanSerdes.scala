package compstak.kafkastreams4s.vulcan

import java.nio.ByteBuffer

import cats.effect.{ConcurrentEffect, Effect, Sync}
import cats.implicits._
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.{Consumed, Grouped, Materialized, Produced}
import org.apache.kafka.streams.state.KeyValueStore
import vulcan.{Codec => VCodec}
import org.apache.kafka.common.serialization

object VulcanSerdes {
  def serdeForVulcan[A: VCodec]: Serde[A] = new VulcanSerdeCreation[A]

  def producedForVulcan[K: VCodec, V: VCodec]: Produced[K, V] =
    Produced.`with`(serdeForVulcan[K], serdeForVulcan[V])

  def materializedForVulcan[K: VCodec, V: VCodec]: Materialized[K, V, KeyValueStore[Bytes, Array[Byte]]] =
    Materialized
      .`with`(serdeForVulcan[K], serdeForVulcan[V])

  def consumedForVulcan[K: VCodec, V: VCodec]: Consumed[K, V] =
    Consumed.`with`(serdeForVulcan[K], serdeForVulcan[V])

  def groupedForVulcan[K: VCodec, V: VCodec]: Grouped[K, V] =
    Grouped.`with`(serdeForVulcan[K], serdeForVulcan[V])

}

class VulcanSerdeCreation[T: VCodec] extends Serde[T] {
  override def deserializer(): Deserializer[T] =
    new Deserializer[T] {
      override def deserialize(topic: String, data: Array[Byte]): T = {
        val codec = implicitly[VCodec[T]]
        codec.schema.flatMap(schema => VCodec.fromBinary(data, schema)).fold(error => throw error.throwable, identity)
      }
    }
  override def serializer(): Serializer[T] =
    new Serializer[T] {
      override def serialize(topic: String, data: T): Array[Byte] =
        VCodec.toBinary(data).fold(error => throw error.throwable, identity)
    }
}

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import scala.jdk.CollectionConverters._

object T {

  //Example use
  import cats.effect.implicits._
  class VulcanSchemaSerdeCreation[F[_]: Effect, K, V](implicit K: KeySerDe[F, K], V: ValueSerDe[F, V]) {
    def createKeySerDe: Serde[K] =
      new Serde[K] {
        override def serializer(): serialization.Serializer[K] =
          new serialization.Serializer[K] {
            override def serialize(topic: String, data: K): Array[Byte] =
              K.serializer.flatMap(s => s.serialize(topic, data)).toIO.unsafeRunSync
          }

        override def deserializer(): serialization.Deserializer[K] =
          new serialization.Deserializer[K] {
            override def deserialize(topic: String, data: Array[Byte]): K =
              K.deserializer.flatMap(d => d.deserialize(topic, data)).toIO.unsafeRunSync
          }
      }

    def createValueSerDe: Serde[V] =
      new Serde[V] {
        override def serializer(): serialization.Serializer[V] =
          new serialization.Serializer[V] {
            override def serialize(topic: String, data: V): Array[Byte] =
              V.serializer.flatMap(s => s.serialize(topic, data)).toIO.unsafeRunSync
          }

        override def deserializer(): serialization.Deserializer[V] =
          new serialization.Deserializer[V] {
            override def deserialize(topic: String, data: Array[Byte]): V =
              V.deserializer.flatMap(d => d.deserialize(topic, data)).toIO.unsafeRunSync
          }
      }
  }

  //Should we use the typeclass pattern here? YES
  case class AvroSerDes[F[_], Key, Value](key: KeySerDe[F, Key], value: ValueSerDe[F, Value])
  case class KeySerDe[F[_], Key](serializer: F[Serializer[F, Key]], deserializer: F[Deserializer[F, Key]])
  case class ValueSerDe[F[_], Value](serializer: F[Serializer[F, Value]], deserializer: F[Deserializer[F, Value]])

  def createAvroSerDes[F[_]: Sync, Key, Value](
    schemaRegistryClient: SchemaRegistryClient,
    properties: Map[String, String]
  )(implicit codec: VCodec[Key], codecV: VCodec[Value]): AvroSerDes[F, Key, Value] = {
    val keyDes: F[Deserializer[F, Key]] = avroDeserializer[Key].using[F](schemaRegistryClient, properties).forKey
    val keySer: F[Serializer[F, Key]] = avroSerializer[Key].using[F](schemaRegistryClient, properties).forKey

    val valueDes: F[Deserializer[F, Value]] =
      avroDeserializer[Value].using[F](schemaRegistryClient, properties).forValue
    val valueSer: F[Serializer[F, Value]] = avroSerializer[Value].using[F](schemaRegistryClient, properties).forValue

    AvroSerDes(
      key = KeySerDe(keySer, keyDes),
      value = ValueSerDe(valueSer, valueDes)
    )
  }

  def avroDeserializer[A](implicit codec: VCodec[A]): AvroDeserializer[A] =
    new AvroDeserializer(codec)

  def avroSerializer[A](implicit codec: VCodec[A]): AvroSerializer[A] =
    new AvroSerializer(codec)

  sealed abstract class Serializer[F[_], A] {
    def serialize(topic: String, a: A): F[Array[Byte]]
  }
  object Serializer {
    def instance[F[_], A](f: (String, A) => F[Array[Byte]]): Serializer[F, A] =
      new Serializer[F, A] {
        override def serialize(topic: String, a: A): F[Array[Byte]] = f(topic, a)
      }
  }

  sealed abstract class KVSerializer[F[_], A] {
    def forKey: F[Serializer[F, A]]
    def forValue: F[Serializer[F, A]]
  }
  object KVSerializer {
    def instance[F[_], A](forKey: F[Serializer[F, A]], forValue: F[Serializer[F, A]]): KVSerializer[F, A] = {
      def _forKey = forKey
      def _forValue = forValue
      new KVSerializer[F, A] {
        val forKey = _forKey
        val forValue = _forValue
      }
    }
  }

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
        forKey = createSerializer(true),
        forValue = createSerializer(false)
      )
    }
  }

  sealed abstract class KVDeserializer[F[_], A] {
    def forKey: F[Deserializer[F, A]]
    def forValue: F[Deserializer[F, A]]
  }

  object KVDeserializer {
    def instance[F[_], A](forKey: F[Deserializer[F, A]], forValue: F[Deserializer[F, A]]): KVDeserializer[F, A] = {
      def _forKey = forKey
      def _forValue = forValue
      new KVDeserializer[F, A] {
        val forKey = _forKey //Rename F
        val forValue = _forValue
      }
    }

    def const[F[_], A](value: F[Deserializer[F, A]]): KVDeserializer[F, A] =
      KVDeserializer.instance(
        forKey = value,
        forValue = value
      )
  }

  sealed abstract class Deserializer[F[_], A] {
    def deserialize(topic: String, bytes: Array[Byte]): F[A]
  }

  object Deserializer {
    def instance[F[_], A](f: (String, Array[Byte]) => F[A]): Deserializer[F, A] =
      new Deserializer[F, A] {
        override def deserialize(topic: String, bytes: Array[Byte]): F[A] = f(topic, bytes)
      }
  }

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
                c.decode(kad.deserialize(topic, bytes, schema), writerSchema) match {
                  case Right(a) => Sync[F].pure(a)
                  case Left(error) => Sync[F].raiseError(error.throwable)
                }
              }
            }

          KVDeserializer.instance(
            forKey = createDeserializer(true),
            forValue = createDeserializer(false)
          )
        case Left(error) => KVDeserializer.const(Sync[F].raiseError(error.throwable))
      }
  }

}
