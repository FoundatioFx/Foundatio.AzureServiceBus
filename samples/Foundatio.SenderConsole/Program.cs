using System;

namespace Foundatio.SenderConsole {
    class Program {
    static void Main(string[] args) {
            Console.WriteLine("Type your message....\r\n");

            try {
                var o = new Sender();
                o.Run(args).GetAwaiter().GetResult();
            }
            catch (Exception e) {
                Console.WriteLine(e);
            }
        }
    }
}
