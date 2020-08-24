package compstak.kafkastreams4s.vulcan.schema

trait AvroSerDes[F[_], K, V] {
  def key: SerDesFor[F, K, Key]
  def value: SerDesFor[F, V, Value]
}

object AvroSerDes {
  def apply[F[_], K, V](keyF: SerDesFor[F, K, Key], valueF: SerDesFor[F, V, Value]): AvroSerDes[F, K, V] =
    new AvroSerDes[F, K, V] {
      val key = keyF
      val value = valueF
    }
}
