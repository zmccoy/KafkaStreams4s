package compstak.kafkastreams4s.vulcan.schema
import cats.effect.Sync
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import vulcan.{Codec => VCodec}

object SerDesHelper {

  def createAvroSerDes[F[_]: Sync, Key, Value](
                                                schemaRegistryClient: SchemaRegistryClient,
                                                properties: Map[String, String]
                                              )(implicit codec: VCodec[Key], codecV: VCodec[Value]): AvroSerDes[F, Key, Value] = {
    val keyDes: F[Deserializer[F, Key]] = avroDeserializer[Key].using[F](schemaRegistryClient, properties).forKey
    val keySer: F[Serializer[F, Key]] = avroSerializer[Key].using[F](schemaRegistryClient, properties).forKey

    val valueDes: F[Deserializer[F, Value]] =
      avroDeserializer[Value].using[F](schemaRegistryClient, properties).forValue
    val valueSer: F[Serializer[F, Value]] = avroSerializer[Value].using[F](schemaRegistryClient, properties).forValue

    AvroSerDes(
      keyF = KeySerDes(keySer, keyDes),
      valueF = ValueSerDes(valueSer, valueDes)
    )
  }

  def avroDeserializer[A](implicit codec: VCodec[A]): AvroDeserializer[A] =
    new AvroDeserializer(codec)

  def avroSerializer[A](implicit codec: VCodec[A]): AvroSerializer[A] =
    new AvroSerializer(codec)
}
