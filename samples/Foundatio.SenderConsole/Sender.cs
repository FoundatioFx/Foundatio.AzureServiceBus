using System;
using Foundatio.Messaging;
using Foundatio.Queues;
using System.Threading.Tasks;

namespace Foundatio.SenderConsole {
    class Sender {
        public async Task Run(string[] args) {
            IMessageBus messageBus;
            string message;
            if (args[0].Equals("topic")) {
                messageBus = 
                new AzureServiceBusMessageBus(new AzureServiceBusMessageBusOptions() {
                    Topic = "Topic1",
                    SubscriptionName = "Subscriber1",
                    ConnectionString = "Endpoint=sb://namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=lpgwMXDswO3SgJkfEPoF/pZ/HAizlMgkWnNDaAvfuug=",
                    SubscriptionId = "guid",
                    ResourceGroupName = "resourcegroup",
                    NameSpaceName = "namespace",
                    Token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsInsdfdfsdfsdfsssssVrT3E1USIsImtpZCI6IlZXVkljMVdEMVRrc2JiMzAxc2FzTTVrT3E1USJ9.eyJhdWQiOiJodHRwczovL21hbmFnZW1lbnQuY29yZS53aW5kb3dzLm5ldC8iLCJpc3MiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC83MzBjNTliZC05NTIyLTRkZTEtYjE1Yy05NzU2YTA1NTA2MGMvIiwiaWF0IjoxNTAyMjk4ODk1LCJuYmYiOjE1MDIyOTg4OTUsImV4cCI6MTUwMjMwMjc5NSwiYWlvIjoiWTJGZ1lGaWoyV0wrZEU3YlhybnNqSTVqRnczMkFBQT0iLCJhcHBpZCI6ImYxODkyY2U0LTg5MjQtNGU1Yi05Y2E4LWNiNWRiM2I3NWE1OCIsImFwcGlkYWNyIjoiMSIsImVfZXhwIjoyNjI4MDAsImlkcCI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0LzczMGM1OWJkLTk1MjItNGRlMS1iMTVjLTk3NTZhMDU1MDYwYy8iLCJvaWQiOiIxMjVkN2MzYi0zYTVjLTQ0ZWYtOTk2OS1iZTJlMDg5NWM4YmMiLCJzdWIiOiIxMjVkN2MzYi0zYTVjLTQ0ZWYtOTk2OS1iZTJlMDg5NWM4YmMiLCJ0aWQiOiI3MzBjNTliZC05NTIyLTRkZTEtYjE1Yy05NzU2YTA1NTA2MGMiLCJ2ZXIiOiIxLjAifQ.rWbOooQ7Jrdjblj0pBDy07hIe3IAcktu-2apM8DZztG6fZMhJLCts_PsKMh0-u3j2boG_bfFFxI9OwgxYqDWonQMVAhHlANgM3KEmEoKv_4WgG7rawamipi6JAGTejfVOQeaoUZFCHbC0N02hMNeaReyhj8gukwmQMruUOTA_u9STM5ra13BSiZZMH8OSS3HwvnI5-nkIzbQqdOD5KaR1PDPMMHKR104lX43zhK9HyJvPlv1d16BwT8ejoAUfIrJ0zxgFpsPoJg1XdVkS7FA5XB_Js0bCt5Kto0EXltx8H5bcpTB5kjZvPVsv9TmakwwutOv9dPBDU36euSNMaLjEQ"
                });
                do {
                    message = Console.ReadLine();
                    await messageBus.PublishAsync(message);
                } while (message != null);

                messageBus.Dispose();

            }
            else if (args[0].Equals("queue")) {
                IQueue<object> queue = new AzureServiceBusQueue<object>(new AzureServiceBusQueueOptions<object>() {
                    Name = "queue1",
                    ConnectionString = "Endpoint=sb://namesapce.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=lpgwMXDswO3SgJkfEPoF/pZ/HAizlMgkWnNDaAvfuug=",
                    SubscriptionId = "guid",
                    ResourceGroupName = "resourcegroup",
                    NameSpaceName = "namespace",
                    Token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJxdfsdgdgdfgsdfsdfIng1dCI6IlZXVkljMVdEMVRrc2JiMzAxc2FzTTVrT3E1USIsImtpZCI6IlZXVkljMVdEMVRrc2JiMzAxc2FzTTVrT3E1USJ9.eyJhdWQiOiJodHRwczovL21hbmFnZW1lbnQuY29yZS53aW5kb3dzLm5ldC8iLCJpc3MiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC83MzBjNTliZC05NTIyLTRkZTEtYjE1Yy05NzU2YTA1NTA2MGMvIiwiaWF0IjoxNTAyNjY0MDg1LCJuYmYiOjE1MDI2NjQwODUsImV4cCI6MTUwMjY2Nzk4NSwiYWlvIjoiWTJGZ1lOaXJjSXl4MmFYay9MNVZqNzFYcm41VEJBQT0iLCJhcHBpZCI6IjBhNWVhZTc1LTVlZDQtNDAyYy1hODk0LTI4ZTQ1MDI1YmFkOCIsImFwcGlkYWNyIjoiMSIsImVfZXhwIjoyNjI4MDAsImlkcCI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0LzczMGM1OWJkLTk1MjItNGRlMS1iMTVjLTk3NTZhMDU1MDYwYy8iLCJvaWQiOiJlZTFkZGZhZC05MzFkLTRlMTAtODUxYy0zODIxMGFkNzg0NjkiLCJzdWIiOiJlZTFkZGZhZC05MzFkLTRlMTAtODUxYy0zODIxMGFkNzg0NjkiLCJ0aWQiOiI3MzBjNTliZC05NTIyLTRkZTEtYjE1Yy05NzU2YTA1NTA2MGMiLCJ2ZXIiOiIxLjAifQ.H64tQ09ohrreySGGGjKkiGdXiBOv5Y2WybnuVK6Bo1LLPf9I-Y7JYar_7Exup81-lIRV1PbRIjYD7XHkZwKT2IX-vH_YN_JIzwXZOV1CsQR9m-TsyfKnNLqc0NB9xayVp9FXlzXAJEBUAld3sU47_rtSgrMeDpKoT_VTJ1z1IWa1Hxy58MFt67UHX1cbn1sEfcJxJz4nkaBSKtvQ1_iKpRsbIqCbBeLddDjVh0rIkhoIwRnxiz4pB5JX5ki0mdm5p8-hw-M_3rgnnQsTZVLbeynmKKWwBFgXuhC8QD7JNLbwqTewH4_tH_X6l3dXAfd_heHiG-su9e197cJno2mSsQ"
                });

                do {
                    message = Console.ReadLine();
                    await queue.EnqueueAsync(message);
                } while (message != null);
            }

        }
    }
}
