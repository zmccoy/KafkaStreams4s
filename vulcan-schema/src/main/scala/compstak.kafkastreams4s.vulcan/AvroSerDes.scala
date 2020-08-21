package compstak.kafkastreams4s.vulcan.schema

trait AvroSerDes[F[_], Key, Value] {
  def key: KeySerDes[F, Key]
  def value: ValueSerDes[F, Value]
}

object AvroSerDes {
  def apply[F[_], K, V](keyF: KeySerDes[F, K], valueF: ValueSerDes[F, V]): AvroSerDes[F, K, V] =
    new AvroSerDes[F, K, V] {
      val key = keyF
      val value = valueF
    }
}
