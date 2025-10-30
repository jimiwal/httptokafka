using System.Text.Json.Nodes;
using Avro;
using Avro.Generic;

namespace HttpToKafka.Kafka.Serialization;

internal static class AvroUtils
{
    public static GenericRecord BuildGenericRecord(string schemaJson, JsonObject payload)
    {
        var schema = Schema.Parse(schemaJson) as RecordSchema
            ?? throw new ArgumentException("Avro schema must be a record schema");
        var record = new GenericRecord(schema);
        foreach (var field in schema.Fields)
        {
            if (!payload.TryGetPropertyValue(field.Name, out var node))
            {
                if (field.DefaultValue != null) continue;
                if (IsNullable(field.Schema)) { record.Add(field.Name, null); continue; }
                throw new ArgumentException($"Payload missing required field '{field.Name}'");
            }
            record.Add(field.Name, ConvertNode(node, field.Schema));
        }
        return record;
    }

    private static bool IsNullable(Schema schema) =>
        schema.Tag == Schema.Type.Null ||
        (schema is UnionSchema u && u.Schemas.Any(s => s.Tag == Schema.Type.Null));

    private static object? ConvertNode(JsonNode? node, Schema fieldSchema)
    {
        if (node is null || node.GetValueKind() == System.Text.Json.JsonValueKind.Null) return null;
        switch (fieldSchema.Tag)
        {
            case Schema.Type.Null: return null;
            case Schema.Type.Boolean: return node.GetValue<bool>();
            case Schema.Type.Int: return node.GetValue<int>();
            case Schema.Type.Long: return node.GetValue<long>();
            case Schema.Type.Float: return node.GetValue<float>();
            case Schema.Type.Double: return node.GetValue<double>();
            case Schema.Type.String: return node.GetValue<string>();
            case Schema.Type.Bytes: return Convert.FromBase64String(node.GetValue<string>());
            case Schema.Type.Record:
                var recSchema = (RecordSchema)fieldSchema;
                var rec = new GenericRecord(recSchema);
                var obj = node.AsObject();
                foreach (var f in recSchema.Fields)
                {
                    obj.TryGetPropertyValue(f.Name, out var n);
                    rec.Add(f.Name, ConvertNode(n, f.Schema));
                }
                return rec;
            case Schema.Type.Array:
                var arrSchema = (ArraySchema)fieldSchema;
                var jarr = node.AsArray();
                var list = new List<object?>();
                foreach (var n in jarr) list.Add(ConvertNode(n, arrSchema.ItemSchema));
                return list;
            case Schema.Type.Map:
                var mapSchema = (MapSchema)fieldSchema;
                var obj2 = node.AsObject();
                var dict = new Dictionary<string, object?>();
                foreach (var kv in obj2) dict[kv.Key] = ConvertNode(kv.Value, mapSchema.ValueSchema);
                return dict;
            case Schema.Type.Enumeration: return node.GetValue<string>();
            case Schema.Type.Fixed:
                var fixedSchema = (FixedSchema)fieldSchema;
                var bytes = Convert.FromBase64String(node.GetValue<string>());
                if (bytes.Length != fixedSchema.Size) throw new ArgumentException($"Fixed size mismatch for {fixedSchema.Name}");
                return new GenericFixed(fixedSchema, bytes);
            case Schema.Type.Union:
                var union = (UnionSchema)fieldSchema;
                foreach (var s in union.Schemas) { try { return ConvertNode(node, s); } catch { } }
                if (union.Schemas.Any(s => s.Tag == Schema.Type.Null)) return null;
                throw new ArgumentException("Unable to match union schema branch");
            default:
                throw new NotSupportedException($"Unsupported Avro schema type {fieldSchema.Tag}");
        }
    }
}
