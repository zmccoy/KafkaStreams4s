package compstak.kafkastreams4s

import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import _root_.vulcan.{Codec => VCodec}
import cats.effect.Effect
import compstak.kafkastreams4s.vulcan.schema.{KeySerDes, ValueSerDes, VulcanSchemaCodec, VulcanSchemaSerdes}

package object vulcan {
  type VulcanTable[K, V] = STable[VulcanSchemaCodec, K, V]

  object VulcanTable {

    def fromKTable[F[_]: Effect, K, V](
      ktable: KTable[K, V]
    )(implicit
      K: KeySerDes[F, K],
      VS: ValueSerDes[F, V],
      VSC: VulcanSchemaCodec[K],
      VSCC: VulcanSchemaCodec[V]
    ): VulcanTable[K, V] =
      new VulcanTable[K, V](ktable)

    def apply[F[_]: Effect, K, V](sb: StreamsBuilder, topicName: String)(implicit
      K: KeySerDes[F, K],
      VS: ValueSerDes[F, V],
      VSC: VulcanSchemaCodec[K],
      VSCC: VulcanSchemaCodec[V]
    ): VulcanTable[K, V] =
      fromKTable(sb.table(topicName, VulcanSchemaSerdes.consumedForVulcan[F, K, V]))

  }
}
