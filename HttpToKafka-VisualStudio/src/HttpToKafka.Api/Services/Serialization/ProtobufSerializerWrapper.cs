using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Google.Protobuf;
using System.Reflection;
using System.Text.Json;

namespace HttpToKafka.Api.Services.Serialization;

public sealed class ProtobufSerializerWrapper : IValueSerializer
{
    private readonly ISchemaRegistryClient _sr;
    private readonly HttpToKafka.Api.Models.ProtobufSettings? _settings;
    private readonly Type? _messageType;

    public ProtobufSerializerWrapper(ISchemaRegistryClient sr, HttpToKafka.Api.Models.ProtobufSettings? settings)
    {
        _sr = sr; _settings = settings;
        if (!string.IsNullOrWhiteSpace(settings?.TypeName))
            _messageType = Type.GetType(settings.TypeName!, throwOnError: true);
    }

    public async Task<byte[]> SerializeAsync(string topic, JsonElement? jsonValue, string? base64Value, CancellationToken ct)
    {
        if (_messageType is null) throw new ArgumentException("Protobuf TypeName is required.");
        if (!typeof(IMessage).IsAssignableFrom(_messageType)) throw new InvalidOperationException("Type must implement IMessage.");

        IMessage msg;
        if (jsonValue is { } je)
        {
            var json = je.ToString();
            // Use static Parser.ParseJson on generated message type via reflection
            var parserProp = _messageType.GetProperty("Parser", BindingFlags.Public | BindingFlags.Static);
            if (parserProp?.GetValue(null) is null) throw new InvalidOperationException("Parser not found on protobuf type.");
            var parseJson = parserProp.PropertyType.GetMethod("ParseJson", new[] { typeof(string) });
            if (parseJson is null) throw new InvalidOperationException("ParseJson not found on Parser.");
            msg = (IMessage)parseJson.Invoke(parserProp.GetValue(null), new object[] { json })!;
        }
        else if (!string.IsNullOrEmpty(base64Value))
        {
            msg = (IMessage)Activator.CreateInstance(_messageType)!;
            msg.MergeFrom(Convert.FromBase64String(base64Value));
        }
        else
        {
            throw new ArgumentException("Provide ValueJson or ValueBase64 for Protobuf.");
        }

        var genericSerializerType = typeof(ProtobufSerializer<>).MakeGenericType(_messageType);
        var serializer = (dynamic)Activator.CreateInstance(genericSerializerType, _sr, new ProtobufSerializerConfig
        {
            AutoRegisterSchemas = _settings?.AutoRegisterSchemas ?? true,
            SubjectNameStrategy = SubjectNameStrategy.TopicName
        })!;

        var context = new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Value, topic);
        byte[] bytes = await serializer.SerializeAsync((dynamic)msg, context, ct);
        return bytes;
    }
}
