using System;
using System.Threading;
using System.Threading.Tasks;

namespace cSharpKafka
{
    class Program
    {
        public static string IP_and_PORT = "10.10.9.17:9092";
        static MyProducer[] MPs;
        static MyConsumer[] MCs;

        static string reader = string.Empty;

        public static void Main(string[] args)
        {
            Console.WriteLine("Kafka Client by Pablo 44003");
            Console.WriteLine("0 - producers 1 - consumers");
            reader = Console.ReadLine();
            if (reader == "0")
                ProducersGeneration();
            else ConsumentsGeneration();
        }

        public static void ConsumentsGeneration()
        {
            Console.Write(Environment.NewLine + "How many consumers? ");
            reader = Console.ReadLine();
            Console.WriteLine();

            int number;
            int.TryParse(reader, out number);
            if (number == 0) number = 1;
            MCs = new MyConsumer[number];
            for (int i = 0; i < number; i++)
            {
                Console.Write($"Consumer {i} name: ");
                reader =  Console.ReadLine();
                Console.WriteLine();
                MCs[i] = new MyConsumer(reader, DateTime.Now.Ticks.ToString()+i);
            }
            ConsumersLoop();
        }

        static void ConsumersLoop()
        {
            foreach (MyConsumer MC in MCs)
                MC.ConsumeFromSeverInit();
        }

        //---------------------------------------------------------------------------

        public static void ProducersGeneration()
        {
            Console.Write(Environment.NewLine + "How many producers? ");
            reader = Console.ReadLine();
            Console.WriteLine();

            int number;
            int.TryParse(reader, out number);
            if (number == 0) number = 1;
            MPs = new MyProducer[number];
            for(int i = 0; i < number; i++)
            {
                MPs[i] = new MyProducer(Convert.ToInt16(i));
            }
            ProducersLoop();
        }

        static void ProducersLoop()
        {
            string message = string.Empty;
            while (true)
            {
                Console.Write("Enter producers message: ");
                message = Console.ReadLine();
                Console.WriteLine();
                foreach (MyProducer MP in MPs)
                {
                    var t = Task.Run(() => MP.ProduceToServer(message));
                    t.Wait();
                }
            }
        }
    }
}