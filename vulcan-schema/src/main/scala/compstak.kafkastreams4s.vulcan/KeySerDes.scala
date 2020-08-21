package compstak.kafkastreams4s.vulcan.schema

trait KeySerDes[F[_], Key] {
  def serializer: F[Serializer[F, Key]]
  def deserializer: F[Deserializer[F, Key]]
}

object KeySerDes {
  def apply[F[_], K](serializerF: F[Serializer[F, K]], deserializerF: F[Deserializer[F, K]]): KeySerDes[F, K] =
    new KeySerDes[F, K] {
      val serializer = serializerF
      val deserializer = deserializerF
    }
}
