package compstak.kafkastreams4s

import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.StreamsBuilder
import cats.effect.Effect
import compstak.kafkastreams4s.vulcan.schema.{AvroSerDes, VulcanSchemaKeyCodec, VulcanSchemaSerdes}

package object vulcan {
  type VulcanTable[K, V] = STable[VulcanSchemaKeyCodec, K, V]

  object VulcanTable {

    def fromKTable[F[_]: Effect, K, V](
      ktable: KTable[K, V]
    )(implicit akv: AvroSerDes[F,K,V]
    ): VulcanTable[K, V] =
      new VulcanTable[K, V](ktable)

    def apply[F[_]: Effect, K, V](sb: StreamsBuilder, topicName: String)(implicit akv: AvroSerDes[F,K,V]
    ): VulcanTable[K, V] =
      fromKTable(sb.table(topicName, VulcanSchemaSerdes.consumedForVulcan[F, K, V]))

  }
}
