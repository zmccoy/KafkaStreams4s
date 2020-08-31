package compstak.kafkastreams4s.vulcan.schema

import org.apache.kafka.common.serialization.Serde
import vulcan.{Codec => VCodec}

trait VulcanSchemaCodec[A] {
  implicit def vcodec: VCodec[A]
  def serde: Serde[A]
}

object VulcanSchemaCodec {
  def apply[A: VulcanSchemaCodec]: VulcanSchemaCodec[A] = implicitly
}
