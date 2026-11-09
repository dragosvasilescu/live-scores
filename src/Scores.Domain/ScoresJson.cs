using System.Text.Json;
using System.Text.Json.Serialization;

namespace Scores.Domain;

public static class ScoresJson
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.SnakeCaseUpper) },
    };
}
