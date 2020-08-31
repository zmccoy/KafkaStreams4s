package compstak.kafkastreams4s.vulcan.schema

trait AvroSerDes[F[_], K, V] {
  def key: SerDesForKey[F, K]
  def value: SerDesForValue[F, V]
}

object AvroSerDes {
  def apply[F[_], K, V](keyF: SerDesForKey[F, K], valueF: SerDesForValue[F, V]): AvroSerDes[F, K, V] =
    new AvroSerDes[F, K, V] {
      val key = keyF
      val value = valueF
    }
}
