package compstak.kafkastreams4s.vulcan.schema

import org.apache.kafka.common.serialization.Serde
import vulcan.{Codec => VCodec}

object CDec {

  trait VulcanSchemaCodec[A] {
    implicit def vcodec: VCodec[A]

    def serde: Serde[A] = ???
  }

  object VulcanSchemaCodec {
    def apply[A: VulcanSchemaCodec]: VulcanSchemaCodec[A] = implicitly
    import compstak.kafkastreams4s.Codec
    implicit val vulcanSchemaCodec: Codec[VulcanSchemaCodec] =
      new Codec[VulcanSchemaCodec] {
        def serde[A: VulcanSchemaCodec]: Serde[A] = apply[A].serde
        def optionSerde[A: VulcanSchemaCodec]: VulcanSchemaCodec[Option[A]] =
          new VulcanSchemaCodec[Option[A]] {
            implicit def vcodec: VCodec[Option[A]] = VCodec.option(apply[A].vcodec)
          }
      }

    implicit def vulcanCodecFromVCodec[A](implicit V: VCodec[A]): VulcanSchemaCodec[A] =
      new VulcanSchemaCodec[A] {
        implicit def vcodec: VCodec[A] = V
      }
  }
}
