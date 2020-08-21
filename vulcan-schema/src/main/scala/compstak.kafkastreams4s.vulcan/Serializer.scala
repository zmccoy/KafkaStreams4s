package compstak.kafkastreams4s.vulcan.schema

sealed abstract class Serializer[F[_], A] {
  def serialize(topic: String, a: A): F[Array[Byte]]
}
object Serializer {
  def instance[F[_], A](f: (String, A) => F[Array[Byte]]): Serializer[F, A] =
    new Serializer[F, A] {
      override def serialize(topic: String, a: A): F[Array[Byte]] = f(topic, a)
    }
}
