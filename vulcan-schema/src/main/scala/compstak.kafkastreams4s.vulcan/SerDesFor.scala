package compstak.kafkastreams4s.vulcan.schema
import vulcan.{Codec => VCodec}

trait SerDesFor[F[_], T] {
  def vcodec: VCodec[T]
  def serializer: F[Serializer[F, T]]
  def deserializer: F[Deserializer[F, T]]
}

trait SerDesForKey[F[_],T] {
  def vcodec: VCodec[T]
  def serializer: F[Serializer[F, T]]
  def deserializer: F[Deserializer[F, T]]
}

trait SerDesForValue[F[_], T] {
  def vcodec: VCodec[T]
  def serializer: F[Serializer[F, T]]
  def deserializer: F[Deserializer[F, T]]
}

object SerDesFor {

  def key[F[_], T](vCodec: VCodec[T], serializerF: F[Serializer[F, T]], deserializerF: F[Deserializer[F, T]]): SerDesForKey[F, T] =
    new SerDesForKey[F, T] {
      val vcodec = vCodec
      val serializer = serializerF
      val deserializer = deserializerF
    }

  def value[F[_], T](
    vCodec: VCodec[T],
    serializerF: F[Serializer[F, T]],
    deserializerF: F[Deserializer[F, T]]
  ): SerDesForValue[F, T] =
    new SerDesForValue[F, T] {
      val vcodec = vCodec
      val serializer = serializerF
      val deserializer = deserializerF
    }

}