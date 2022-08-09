using Confluent.Kafka;
using TplKafka.Data;

namespace TplKafka.Client;

public class CommitObserver : IObserver<Record<byte[],byte[]>>
{
    private long _latestTimestamp = Int64.MinValue;
    private long _lastCommitTime = 0;
    private readonly long _commitInterval = 30_000L;
    private readonly IConsumer<byte[], byte[]> _commitHandler;
    private Dictionary<TopicPartition, TopicPartitionOffset> _commitDictionary = new();

    public CommitObserver(IConsumer<byte[], byte[]> commitHandler)
    {
        _commitHandler = commitHandler;
    }

    public void OnCompleted()
    {
        throw new NotImplementedException();
    }

    public void OnError(Exception error)
    {
        throw new NotImplementedException();
    }

    public void OnNext(Record<byte[], byte[]> processedRecord)
    {
        Console.WriteLine($"Record delivered now up for commit {processedRecord.TopicPartitionOffset}");
        if (processedRecord.Timestamp.UnixTimestampMs > _latestTimestamp)
        {
            _latestTimestamp = processedRecord.Timestamp.UnixTimestampMs;
        }

        if (_lastCommitTime == 0)
        {
            _lastCommitTime = _latestTimestamp;
        }

        var tp = processedRecord.TopicPartitionOffset.TopicPartition;
        var tpo = processedRecord.TopicPartitionOffset;

        if (!_commitDictionary.TryAdd(tp, tpo))
        {
            _commitDictionary[tp] = tpo;
        }

        if (_latestTimestamp - _lastCommitTime >= _commitInterval)
        {
            Console.WriteLine("!!Time to commit!! Current Time " + _latestTimestamp + " Last commit " + _lastCommitTime);
            _commitHandler.Commit(_commitDictionary.Values);
            _commitDictionary.Clear();
            _lastCommitTime = _latestTimestamp;
        }

    }
}