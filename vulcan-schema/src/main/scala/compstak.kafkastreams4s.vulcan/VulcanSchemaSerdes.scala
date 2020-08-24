package compstak.kafkastreams4s.vulcan.schema

import cats.effect.Effect
import cats.effect.implicits._
import org.apache.kafka.common.serialization
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.{Consumed, Grouped, Materialized, Produced}
import org.apache.kafka.streams.state.KeyValueStore
import cats.implicits._

object VulcanSchemaSerdes {

  def serdeForSchema[F[_]: Effect, K, V](implicit
    K: SerDesFor[F, K, Key],
    V: SerDesFor[F, V, Value]
  ): (Serde[K], Serde[V]) =
    (createKeySerDe, createValueSerDe)

  def producedForVulcan[F[_]: Effect, K, V](implicit
    K: SerDesFor[F, K, Key],
    V: SerDesFor[F, V, Value]
  ): Produced[K, V] = {
    val x = serdeForSchema[F, K, V]
    Produced.`with`(x._1, x._2)
  }

  def materializedForVulcan[F[_]: Effect, K, V](implicit
    K: SerDesFor[F, K, Key],
    V: SerDesFor[F, V, Value]
  ): Materialized[K, V, KeyValueStore[Bytes, Array[Byte]]] = {
    val x = serdeForSchema[F, K, V]
    Materialized.`with`(x._1, x._2)
  }

  def consumedForVulcan[F[_]: Effect, K, V](implicit
    K: SerDesFor[F, K, Key],
    V: SerDesFor[F, V, Value]
  ): Consumed[K, V] = {
    val x = serdeForSchema[F, K, V]
    Consumed.`with`(x._1, x._2)
  }

  def groupedForVulcan[F[_]: Effect, K, V](implicit
    K: SerDesFor[F, K, Key],
    V: SerDesFor[F, V, Value]
  ): Grouped[K, V] = {
    val x = serdeForSchema[F, K, V]
    Grouped.`with`(x._1, x._2)
  }
  def createKeySerDe[F[_]: Effect, K](implicit K: SerDesFor[F, K, Key]): Serde[K] =
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

  def createValueSerDe[F[_]: Effect, V](implicit V: SerDesFor[F, V, Value]): Serde[V] =
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
