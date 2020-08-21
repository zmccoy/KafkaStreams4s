package compstak.kafkastreams4s.vulcan.schema

trait ValueSerDes[F[_], Value] {
  def serializer: F[Serializer[F, Value]]
  def deserializer: F[Deserializer[F, Value]]
}

object ValueSerDes {
  def apply[F[_], V](serializerF: F[Serializer[F, V]], deserializerF: F[Deserializer[F, V]]): ValueSerDes[F, V] =
    new ValueSerDes[F, V] {
      val serializer = serializerF
      val deserializer = deserializerF
    }
}
