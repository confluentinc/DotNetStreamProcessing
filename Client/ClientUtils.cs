using Confluent.Kafka;


namespace TplKafka.Client;

public static class ClientUtils
{
    public static ConsumerConfig ConsumerConfig(string configFilePath)
    {
        return new ConsumerConfig(BaseConfigs(configFilePath));
    }

    public static ProducerConfig ProducerConfig(string configFilePath)
    {
        return new ProducerConfig(BaseConfigs(configFilePath));
    }

    private static Dictionary<string, string> BaseConfigs(string configFilePath)
    {
        Dictionary<string, string> baseConfigs = new();
        var lines = LoadPropertiesFile(configFilePath);
        foreach (var line in lines)
        {
            var trimmedLine = line.Trim();
            if (trimmedLine.Trim().StartsWith("#") || string.IsNullOrEmpty(trimmedLine)) continue;
            Console.WriteLine("property [" + trimmedLine +"]");
            var parts = line.Split("=");
            baseConfigs.Add(parts[0].Trim(), parts[1].Trim());

        }

        return baseConfigs;
    }

    private static string[] LoadPropertiesFile(string configFilePath)
    {
        return  File.ReadAllLines(configFilePath);
    }

}