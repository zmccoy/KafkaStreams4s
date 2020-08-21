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

  implicit val vulcanSchemaCodec(implicit XYZ): Codec[VulcanSchemaCodec] =
    new Codec[VulcanSchemaCodec] {
      def serde[A: VulcanSchemaCodec]: Serde[A] = apply[A].serde

      def optionSerde[A: VulcanSchemaCodec]: VulcanSchemaCodec[Option[A]] =
        new VulcanSchemaCodec[Option[A]] {
          implicit def vcodec: VCodec[Option[A]] = VCodec.option(apply[A].vcodec)
          def serde: Serde[Option[A]] = ??? //We'll need to recreate the AvroSerDes here then(?)
        }
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
