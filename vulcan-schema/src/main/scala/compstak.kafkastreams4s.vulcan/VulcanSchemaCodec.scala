package compstak.kafkastreams4s.vulcan.schema

import cats.effect.Effect
import org.apache.kafka.common.serialization.Serde
import vulcan.{Codec => VCodec}

trait VulcanSchemaCodec[A] {
  implicit def vcodec: VCodec[A]
  def serde: Serde[A]
}

object VulcanSchemaCodec {
  def apply[A: VulcanSchemaCodec]: VulcanSchemaCodec[A] = implicitly

  import compstak.kafkastreams4s.Codec

  implicit def vulcanSchemaCodec[F[_], A]: Codec[VulcanSchemaCodec] =
    new Codec[VulcanSchemaCodec] {
      def serde[A: VulcanSchemaCodec]: Serde[A] = apply[A].serde

      def optionSerde[A: VulcanSchemaCodec]: VulcanSchemaCodec[Option[A]] = ???
    }

  implicit def vulcanCodecFromVCodec[F[_]: Effect, A](implicit V: VCodec[A], A: KeySerDes[F, A]): VulcanSchemaCodec[A] =
    new VulcanSchemaCodec[A] {
      implicit def vcodec: VCodec[A] = V
      def serde: Serde[A] = VulcanSchemaSerdes.createKeySerDe[F, A]
    }

  implicit def vulcanCodecFromVCodec[F[_]: Effect, A](implicit
    V: VCodec[A],
    A: ValueSerDes[F, A]
  ): VulcanSchemaCodec[A] =
    new VulcanSchemaCodec[A] {
      implicit def vcodec: VCodec[A] = V
      def serde: Serde[A] = VulcanSchemaSerdes.createValueSerDe[F, A]
    }
}
