using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

class Program
{
    static string IP_and_PORT = "10.10.9.17:9092";
    //----------------------- +++ ------------------------------
    static string message = string.Empty;
    static Random myRandom = new Random();
    public static void Main(string[] args)
    {
        Console.WriteLine("0 - producer 1 - consumer");
        string readed = Console.ReadLine();
        if (readed == "0")
            Loop();
        else MainConsumer(DateTime.Now.Ticks.ToString());
    }

    public static void Loop()
    {
        while (true)
        {
            Console.Write("Enter producer message: ");
            message = Console.ReadLine();
            if(message == "exit")
                break;
            if (message == "")
                continue;
            var t = Task.Run(() => MainProducer(message));
            t.Wait();
        }
    }

    //-----------------------***----------------------
    public static void MainConsumer(string randomGroupID) //-> always reads all messages from beginning!
    {
        var conf = new ConsumerConfig
        {
            GroupId = randomGroupID,
            BootstrapServers = IP_and_PORT,
            // Note: The AutoOffsetReset property determines the start offset in the event
            // there are not yet any committed offsets for the consumer group for the
            // topic/partitions of interest. By default, offsets are committed
            // automatically, so in this example, consumption will only start from the
            // earliest message in the topic 'my-topic' the first time you run the program.
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
        {
            c.Subscribe("test-topic");

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            try
            {
                while (true)
                {
                    try
                    {
                        var cr = c.Consume(cts.Token);
                        Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occured: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                c.Close();
            }
        }
    }

    public static async Task MainProducer(string msg)
    {
        var config = new ProducerConfig { BootstrapServers = IP_and_PORT };

        // If serializers are not specified, default serializers from
        // `Confluent.Kafka.Serializers` will be automatically used where
        // available. Note: by default strings are encoded as UTF8.
        using (var p = new ProducerBuilder<Null, string>(config).Build())
        {
            try
            {
                var dr = await p.ProduceAsync("test-topic", new Message<Null, string> { Value = msg });
                Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
            }
            catch (ProduceException<Null, string> e)
            {
                Console.WriteLine($"Delivery failed: {e.Error.Reason}");
            }
        }
    }
}