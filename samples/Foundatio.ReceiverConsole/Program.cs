using System;
using Foundatio.Messaging;
using Foundatio.Queues;
using System.Threading.Tasks;

namespace Foundatio.ReceiverConsole {
    class Program {
        static void Main(string[] args) {
           

            try {
                var o = new Receiver();
                o.Run(args).GetAwaiter().GetResult();
            }
            catch (Exception e) {
                Console.WriteLine(e);
            }
        }
    }
}
