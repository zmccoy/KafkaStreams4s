package compstak.kafkastreams4s.vulcan.schema

sealed abstract class KVDeserializer[F[_], A] {
  def forKey: F[Deserializer[F, A]]
  def forValue: F[Deserializer[F, A]]
}

object KVDeserializer {
  def instance[F[_], A](forKeyF: F[Deserializer[F, A]], forValueF: F[Deserializer[F, A]]): KVDeserializer[F, A] =
    new KVDeserializer[F, A] {
      val forKey = forKeyF
      val forValue = forValueF
    }

  def const[F[_], A](value: F[Deserializer[F, A]]): KVDeserializer[F, A] =
    KVDeserializer.instance(
      forKeyF = value,
      forValueF = value
    )
}
