package compstak.kafkastreams4s.vulcan

import java.nio.ByteBuffer

import cats.effect.{Effect, Sync}
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

  object Pack {

  }

  object CDec {

    trait VulcanSchemaCodec[A] {
      implicit def vcodec: VCodec[A]

      def serde: Serde[A] = ???
    }

    object VulcanSchemaCodec {
      def apply[A: VulcanSchemaCodec]: VulcanSchemaCodec[A] = implicitly
      import compstak.kafkastreams4s.Codec
      implicit val vulcanSchemaCodec: Codec[VulcanSchemaCodec] =
        new Codec[VulcanSchemaCodec] {
          def serde[A: VulcanSchemaCodec]: Serde[A] = apply[A].serde
          def optionSerde[A: VulcanSchemaCodec]: VulcanSchemaCodec[Option[A]] =
            new VulcanSchemaCodec[Option[A]] {
              implicit def vcodec: VCodec[Option[A]] = VCodec.option(apply[A].vcodec)
            }
        }

      implicit def vulcanCodecFromVCodec[A](implicit V: VCodec[A]): VulcanSchemaCodec[A] =
        new VulcanSchemaCodec[A] {
          implicit def vcodec: VCodec[A] = V
        }
    }
  }

  object VulcanSchemaSerdes {
    def serdeForSchema[F[_] : Effect, K, V](implicit K: KeySerDes[F, K], V: ValueSerDes[F, V]): (Serde[K], Serde[V]) = {
      val v = new VulcanSchemaSerdeCreation[F, K, V]
      (v.createKeySerDe, v.createValueSerDe)
    }
      def producedForVulcan[F[_]: Effect, K, V](implicit K: KeySerDes[F, K], V: ValueSerDes[F, V]): Produced[K, V] = {
        val x = serdeForSchema[F,K,V]
        Produced.`with`(x._1, x._2)
      }

    def materializedForVulcan[F[_]: Effect, K, V](implicit K: KeySerDes[F, K], V: ValueSerDes[F, V]): Materialized[K, V, KeyValueStore[Bytes, Array[Byte]]] = {
      val x = serdeForSchema[F,K,V]
      Materialized.`with`(x._1, x._2)
    }

    def consumedForVulcan[F[_]: Effect, K, V](implicit K: KeySerDes[F, K], V: ValueSerDes[F, V]): Consumed[K, V] = {
      val x = serdeForSchema[F,K,V]
      Consumed.`with`(x._1, x._2)
    }

    def groupedForVulcan[F[_]: Effect, K, V](implicit K: KeySerDes[F, K], V: ValueSerDes[F, V]): Grouped[K, V] = {
      val x = serdeForSchema[F,K,V]
      Grouped.`with`(x._1, x._2)
    }

    import cats.effect.implicits._
    class VulcanSchemaSerdeCreation[F[_]: Effect, K, V](implicit K: KeySerDes[F, K], V: ValueSerDes[F, V]) {
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
  }

  trait AvroSerDes[F[_], Key, Value] {
    def key: KeySerDes[F, Key]
    def value: ValueSerDes[F, Value]
  }

  object AvroSerDes {
    def apply[F[_], K, V](keyF: KeySerDes[F, K], valueF: ValueSerDes[F, V]): AvroSerDes[F, K, V] = new AvroSerDes[F, K, V] {
      val key = keyF
      val value = valueF
    }
  }

  trait KeySerDes[F[_], Key] {
    def serializer: F[Serializer[F, Key]]
    def deserializer: F[Deserializer[F, Key]]
  }

  object KeySerDes {
    def apply[F[_], K](serializerF: F[Serializer[F, K]], deserializerF: F[Deserializer[F, K]]): KeySerDes[F, K] = new KeySerDes[F, K] {
      val serializer = serializerF
      val deserializer = deserializerF
    }
  }

  trait ValueSerDes[F[_], Value] {
    def serializer: F[Serializer[F, Value]]
    def deserializer: F[Deserializer[F, Value]]
  }

  object ValueSerDes {
    def apply[F[_], V](serializerF: F[Serializer[F, V]], deserializerF: F[Deserializer[F, V]]): ValueSerDes[F, V] = new ValueSerDes[F, V] {
      val serializer = serializerF
      val deserializer = deserializerF
    }
  }


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
      keyF = KeySerDes(keySer, keyDes),
      valueF = ValueSerDes(valueSer, valueDes)
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
    def instance[F[_], A](forKeyF: F[Serializer[F, A]], forValueF: F[Serializer[F, A]]): KVSerializer[F, A] = {
      new KVSerializer[F, A] {
        val forKey = forKeyF
        val forValue = forValueF
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
        forKeyF = createSerializer(true),
        forValueF = createSerializer(false)
      )
    }
  }

  sealed abstract class KVDeserializer[F[_], A] {
    def forKey: F[Deserializer[F, A]]
    def forValue: F[Deserializer[F, A]]
  }

  object KVDeserializer {
    def instance[F[_], A](forKeyF: F[Deserializer[F, A]], forValueF: F[Deserializer[F, A]]): KVDeserializer[F, A] = {
      new KVDeserializer[F, A] {
        val forKey = forKeyF
        val forValue = forValueF
      }
    }

    def const[F[_], A](value: F[Deserializer[F, A]]): KVDeserializer[F, A] =
      KVDeserializer.instance(
        forKeyF = value,
        forValueF = value
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

}
