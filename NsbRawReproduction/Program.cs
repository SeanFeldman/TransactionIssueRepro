using System;

namespace NsbRawReproduction
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using DummyMessages;
    using NServiceBus;
    using NServiceBus.Raw;
    using NServiceBus.Routing;
    using NServiceBus.Transport;

    class Program
    {
        static async Task Main(string[] args)
        {
            var senderConfig = RawEndpointConfiguration.Create("DummyHandler", OnMessage, "error");
            var transport = senderConfig.UseTransport<AzureServiceBusTransport>();
            transport.ConnectionString(Environment.GetEnvironmentVariable("AzureServiceBus_ConnectionString"));
            senderConfig.LimitMessageProcessingConcurrencyTo(10);
            
            var sender = await RawEndpoint.Start(senderConfig).ConfigureAwait(false);
            Console.ReadLine();
        }

        static async Task OnMessage(MessageContext context, IDispatchMessages dispatcher)
        {
            Console.WriteLine($"Received FooEvent with ID {context.MessageId}, sleeping 30ms to simulate normal usage");

            await Task.Delay(30);

            var numberOfEventsToPublish = 10;
            var events = Enumerable.Range(1, numberOfEventsToPublish).Select(_ => new BarEvent { Id = Guid.NewGuid().ToString() });

            var tasks = new List<TransportOperation>(numberOfEventsToPublish);
            tasks.AddRange(events.Select(@event => new TransportOperation(new OutgoingMessage(@event.Id, new Dictionary<string, string>(), new byte []{1, 2, 3}), new MulticastAddressTag(typeof(BarEvent)))));

            await dispatcher.Dispatch(new TransportOperations(tasks.ToArray()), context.TransportTransaction, context.Extensions);

            Console.WriteLine($"Published {numberOfEventsToPublish} BarEvent events.");
        }
    }
}
