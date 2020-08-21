package compstak.kafkastreams4s.vulcan.schema

sealed abstract class Deserializer[F[_], A] {
  def deserialize(topic: String, bytes: Array[Byte]): F[A]
}

object Deserializer {
  def instance[F[_], A](f: (String, Array[Byte]) => F[A]): Deserializer[F, A] =
    new Deserializer[F, A] {
      override def deserialize(topic: String, bytes: Array[Byte]): F[A] = f(topic, bytes)
    }
}
