package compstak.kafkastreams4s.vulcan.schema

sealed abstract class KVSerializer[F[_], A] {
  def forKey: F[Serializer[F, A]]
  def forValue: F[Serializer[F, A]]
}
object KVSerializer {
  def instance[F[_], A](forKeyF: F[Serializer[F, A]], forValueF: F[Serializer[F, A]]): KVSerializer[F, A] =
    new KVSerializer[F, A] {
      val forKey = forKeyF
      val forValue = forValueF
    }
}
