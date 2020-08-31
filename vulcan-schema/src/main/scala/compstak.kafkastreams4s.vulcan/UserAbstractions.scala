package compstak.kafkastreams4s.vulcan

import cats.effect.{Effect, Sync}
import compstak.kafkastreams4s.Codec
import compstak.kafkastreams4s.vulcan.schema.{AvroSerDes, SerDesHelper, VulcanSchemaCodec, VulcanSchemaSerdes}
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.kafka.common.serialization.Serde
import vulcan.{Codec => VCodec}


class AvroImplicits[F[_]: Effect](
  schemaRegistryClient: SchemaRegistryClient,
  properties: Map[String, String]
) {


  //Process: VCodec => VulcanSchemaCodec => Codec[VulcanSchemaCodec]

  def avroSerdes[K: VCodec,V: VCodec]: AvroSerDes[F,K,V] = SerDesHelper.createAvroSerDes[F, K, V](schemaRegistryClient, properties)
  //User makes this^^ implicit.
  def toCodecVulcanSchema[K,V](a: AvroSerDes[F,K,V]): (Codec[VulcanSchemaCodec[K]], Codec[VulcanSchemaCodec[V]]) = {
    //Start here and produce that

    ???
  }

  def asCodecKey: Codec[VulcanSchemaCodec] =
    new Codec[VulcanSchemaCodec] {
      override def optionSerde[A: VulcanSchemaCodec]: VulcanSchemaCodec[Option[A]] =
        new VulcanSchemaCodec[Option[A]] {
          implicit override def vcodec: VCodec[Option[A]] = VCodec.option(implicitly[VulcanSchemaCodec[A]].vcodec)
          override def serde: Serde[Option[A]] =
            VulcanSchemaSerdes.createKeySerDe[F, Option[A]](
              implicitly[Effect[F]],
              SerDesHelper.createAvroSerDes[F, Option[A], Option[A]](schemaRegistryClient, properties).key
            )
        }
      def serde[A: VulcanSchemaCodec]: Serde[A] =
        VulcanSchemaSerdes.createKeySerDe[F, A](implicitly[Effect[F]], SerDesHelper.createAvroSerDes[F, A, A](schemaRegistryClient, properties)(implicitly[Sync[F]], implicitly[schema.VulcanSchemaCodec[A]].vcodec,implicitly[schema.VulcanSchemaCodec[A]].vcodec).key)
    }

  def asCodecValue: Codec[VulcanSchemaCodec] =
    new Codec[VulcanSchemaCodec] {
      override def optionSerde[A: VulcanSchemaCodec]: VulcanSchemaCodec[Option[A]] =
        new VulcanSchemaCodec[Option[A]] {
          implicit override def vcodec: VCodec[Option[A]] = VCodec.option(implicitly[VulcanSchemaCodec[A]].vcodec)
          override def serde: Serde[Option[A]] =
            VulcanSchemaSerdes.createValueSerDe[F, Option[A]](
              implicitly[Effect[F]],
              SerDesHelper.createAvroSerDes[F, Option[A], Option[A]](schemaRegistryClient, properties).value
            )
        }
      def serde[A: VulcanSchemaCodec]: Serde[A] =
        VulcanSchemaSerdes.createValueSerDe[F, A](implicitly[Effect[F]],
          SerDesHelper.createAvroSerDes[F, A, A](schemaRegistryClient, properties)(implicitly[Sync[F]], implicitly[schema.VulcanSchemaCodec[A]].vcodec,implicitly[schema.VulcanSchemaCodec[A]].vcodec).value)
    }

  def asKeyCodec[T: VCodec]: VulcanSchemaCodec[T] =
    new VulcanSchemaCodec[T] {
      def vcodec = implicitly[VCodec[T]]
      def serde: Serde[T] = VulcanSchemaSerdes.createKeySerDe[F, T](implicitly[Effect[F]], SerDesHelper.createAvroSerDes[F, T, T](schemaRegistryClient, properties).key)
    }

  def asValueCodec[T: VCodec]: VulcanSchemaCodec[T] =
    new VulcanSchemaCodec[T] {
      def vcodec = implicitly[VCodec[T]]
      def serde: Serde[T] = VulcanSchemaSerdes.createValueSerDe[F, T](implicitly[Effect[F]], SerDesHelper.createAvroSerDes[F, T, T](schemaRegistryClient, properties).value)
    }

}

