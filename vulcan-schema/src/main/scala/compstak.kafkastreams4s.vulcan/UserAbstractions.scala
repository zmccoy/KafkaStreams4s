package compstak.kafkastreams4s.vulcan

import cats.effect.Effect
import compstak.kafkastreams4s.Codec
import compstak.kafkastreams4s.vulcan.schema.{Key, SerDesFor, SerDesHelper, Value, VulcanSchemaCodec, VulcanSchemaSerdes}
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.kafka.common.serialization.Serde
import vulcan.{Codec => VCodec}

class AvroMethodsForT[F[_]: Effect, T: VCodec](
  schemaRegistryClient: SchemaRegistryClient,
  properties: Map[String, String]
) {
  private val x = SerDesHelper.createAvroSerDes[F, T, T](schemaRegistryClient, properties)

  //Move type T and implicit VCodec down to these defs.
  def asCodecKey: Codec[VulcanSchemaCodec] = new Codec[VulcanSchemaCodec] {
    override def optionSerde[A: VulcanSchemaCodec]: VulcanSchemaCodec[Option[A]] = new VulcanSchemaCodec[Option[A]] {
      override implicit def vcodec: VCodec[Option[A]] = VCodec.option(implicitly[VulcanSchemaCodec[A]].vcodec)
      override def serde: Serde[Option[A]] = VulcanSchemaSerdes.createKeySerDe[F, Option[A]](implicitly[Effect[F]], SerDesHelper.createAvroSerDes[F, Option[A], Option[A]](schemaRegistryClient, properties).key)
    }
    def serde[A: VulcanSchemaCodec]: Serde[A] = implicitly[VulcanSchemaCodec[A]].serde
  }

  def asCodecValue: Codec[VulcanSchemaCodec] = new Codec[VulcanSchemaCodec] {
    override def optionSerde[A: VulcanSchemaCodec]: VulcanSchemaCodec[Option[A]] = new VulcanSchemaCodec[Option[A]] {
      override implicit def vcodec: VCodec[Option[A]] = VCodec.option(implicitly[VulcanSchemaCodec[A]].vcodec)
      override def serde: Serde[Option[A]] = VulcanSchemaSerdes.createValueSerDe[F, Option[A]](implicitly[Effect[F]], SerDesHelper.createAvroSerDes[F, Option[A], Option[A]](schemaRegistryClient, properties).value)
    }
    def serde[A: VulcanSchemaCodec]: Serde[A] = implicitly[VulcanSchemaCodec[A]].serde
  }

  def asKeyCodec: VulcanSchemaCodec[T] =
    new VulcanSchemaCodec[T] {
      def vcodec = implicitly[VCodec[T]]
      def serde: Serde[T] = VulcanSchemaSerdes.createKeySerDe[F, T](implicitly[Effect[F]], x.key)
    }

  //Method to produce a Codec[VulcanSchemaCodec[T]] (?)
  def asValueCodec: VulcanSchemaCodec[T] =
    new VulcanSchemaCodec[T] {
      def vcodec = implicitly[VCodec[T]]
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
