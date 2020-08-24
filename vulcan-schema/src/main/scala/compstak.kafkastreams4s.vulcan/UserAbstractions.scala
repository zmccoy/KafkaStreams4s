package compstak.kafkastreams4s.vulcan

import cats.effect.Effect
import compstak.kafkastreams4s.vulcan.schema.{
  Key,
  SerDesFor,
  SerDesHelper,
  Value,
  VulcanSchemaCodec,
  VulcanSchemaSerdes
}
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.kafka.common.serialization.Serde
import vulcan.Codec

class AvroMethodsForT[F[_]: Effect, T: Codec](
  schemaRegistryClient: SchemaRegistryClient,
  properties: Map[String, String]
) {
  private val x = SerDesHelper.createAvroSerDes[F, T, T](schemaRegistryClient, properties)

  def asKeyCodec: VulcanSchemaCodec[T] =
    new VulcanSchemaCodec[T] {
      def vcodec = implicitly[Codec[T]]
      def serde: Serde[T] = VulcanSchemaSerdes.createKeySerDe[F, T](implicitly[Effect[F]], x.key)
    }

  def asValueCodec: VulcanSchemaCodec[T] =
    new VulcanSchemaCodec[T] {
      def vcodec = implicitly[Codec[T]]
      def serde: Serde[T] = VulcanSchemaSerdes.createValueSerDe[F, T](implicitly[Effect[F]], x.value)
    }

  def keySerdes: SerDesFor[F, T, Key] = x.key
  def valueSerdes: SerDesFor[F, T, Value] = x.value

}

//Want something to take in the schemaREg + properties and then hide that away only exposing ways to create

/* val schemaHolder = new AvroMethodsForT[F, MyType](sr, prop)
val schemaHolderForY = new AvroMethodsForT[F, YourType](sr, prop)
implicit val myTypeCodec = schemaHolder.asKey //For regular operations that require a Codec
implicit val yourTypeCodec = schemaHolderForY.asValue
implicit val keySerde = schemaHolder.keySerdes
implicit val valueSerde = schemaHolderForY.valueSerdes //Needed for the Stable exposure since it keeps the key and value separate.
 */
