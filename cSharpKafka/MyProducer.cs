using System;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace cSharpKafka
{
    class MyProducer
    {
        private string ProdTopic = string.Empty;
        private string message = string.Empty;
        private short myNo;

        public MyProducer(short number)
        {
            myNo = number;
            Console.Write($"Enter the topic for producer {myNo}: ");
            ProdTopic = Console.ReadLine();
            Console.WriteLine();
        }

        public async Task ProduceToServer(string msg)
        {
            var config = new ProducerConfig { BootstrapServers = Program.IP_and_PORT };

            // If serializers are not specified, default serializers from
            // `Confluent.Kafka.Serializers` will be automatically used where
            // available. Note: by default strings are encoded as UTF8.
            using (var p = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    var dr = await p.ProduceAsync(ProdTopic, new Message<Null, string> { Value = msg });
                    Console.WriteLine($"Prod: {myNo} Delivered '{dr.Value}' --> {dr.Topic}"); //dr.TopicPartitionOffset
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }
        }
    }
}
