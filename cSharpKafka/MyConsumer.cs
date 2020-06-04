using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace cSharpKafka
{
    class MyConsumer
    {
        private string[] ConsumTopics = new string[2];
        private string myNo;
        private string myGroup;
        private Thread myThread;

        public MyConsumer(string number,string group)
        {
            myNo = number;
            myGroup = group;

            Console.Write($"Enter the first topic for consumer {myNo}: ");
            ConsumTopics[0] = Console.ReadLine();
            Console.WriteLine();
            Console.Write($"Enter the second topic for consumer {myNo}: ");
            ConsumTopics[1] = Console.ReadLine();
            Console.WriteLine();
            //MainConsumer(DateTime.Now.Ticks.ToString());
        }

        private string MyTopics()
        {
            return "Cons "+myNo+" {"+ConsumTopics[0] + "|" + ConsumTopics[1]+"}";
        }

        public void ConsumeFromSeverInit()
        {
            myThread = new Thread(ConsumeFromServer);
            myThread.Start();
        }

        private void ConsumeFromServer() //-> always reads all messages from beginning!
        {
            var conf = new ConsumerConfig
            {
                GroupId = myGroup,
                BootstrapServers = Program.IP_and_PORT,
                // Note: The AutoOffsetReset property determines the start offset in the event
                // there are not yet any committed offsets for the consumer group for the
                // topic/partitions of interest. By default, offsets are committed
                // automatically, so in this example, consumption will only start from the
                // earliest message in the topic 'my-topic' the first time you run the program.
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                c.Subscribe(ConsumTopics);

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
                            Console.WriteLine($"{MyTopics()} '{cr.Topic}' --> {cr.Message.Value}"); //Consumed message cr.TopicPartitionOffset
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
    }
}
