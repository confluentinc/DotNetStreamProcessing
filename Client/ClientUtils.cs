using Confluent.Kafka;
using TplKafka.Data;

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
            if (!line.StartsWith("#") && !String.IsNullOrEmpty(line))
            {
                Console.WriteLine("property [" + line +"]");
                var parts = line.Split("=");
                baseConfigs.Add(parts[0], parts[1]);
            }
            
        }

        return baseConfigs;
    }

    private static string[] LoadPropertiesFile(string configFilePath)
    {
        return  File.ReadAllLines(configFilePath);
    }

}