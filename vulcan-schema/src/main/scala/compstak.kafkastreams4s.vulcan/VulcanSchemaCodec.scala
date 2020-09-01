package compstak.kafkastreams4s.vulcan.schema

import org.apache.kafka.common.serialization.Serde
import vulcan.{Codec => VCodec}

trait VulcanSchemaKeyCodec[A] {
  implicit def vcodec: VCodec[A]
  def serde: Serde[A]
}

object VulcanSchemaKeyCodec {
  def apply[A: VulcanSchemaKeyCodec]: VulcanSchemaKeyCodec[A] = implicitly
}

trait VulcanSchemaValueCodec[A] {
  implicit def vcodec: VCodec[A]
  def serde: Serde[A]
}

object VulcanSchemaValueCodec {
  def apply[A: VulcanSchemaValueCodec]: VulcanSchemaValueCodec[A] = implicitly
}