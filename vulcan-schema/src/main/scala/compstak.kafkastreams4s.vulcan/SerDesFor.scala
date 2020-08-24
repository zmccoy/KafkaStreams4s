package compstak.kafkastreams4s.vulcan.schema

sealed abstract class KV
trait Key extends KV
trait Value extends KV

trait SerDesFor[F[_], T, P <: KV] {
  def serializer: F[Serializer[F, T]]
  def deserializer: F[Deserializer[F, T]]
}

object SerDesFor {

  def forKey[F[_], T](serializerF: F[Serializer[F, T]], deserializerF: F[Deserializer[F, T]]): SerDesFor[F, T, Key] =
    new SerDesFor[F, T, Key] {
      val serializer = serializerF
      val deserializer = deserializerF
    }

  def forValue[F[_], T](
    serializerF: F[Serializer[F, T]],
    deserializerF: F[Deserializer[F, T]]
  ): SerDesFor[F, T, Value] =
    new SerDesFor[F, T, Value] {
      val serializer = serializerF
      val deserializer = deserializerF
    }
}
