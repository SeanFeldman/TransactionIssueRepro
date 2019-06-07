using System;

namespace Sender
{
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Azure.ServiceBus;
    using Microsoft.Azure.ServiceBus.Core;
    using Microsoft.Azure.ServiceBus.Management;

    class Program
    {
        static async Task Main(string[] args)
        {
            var connectionString = Environment.GetEnvironmentVariable("AzureServiceBus_ConnectionString");

            var client = new ManagementClient(connectionString);

            if (! await client.TopicExistsAsync(Constants.TopicName))
            {
                await client.CreateTopicAsync(new TopicDescription(Constants.TopicName)
                {
                    MaxSizeInMB = 5 * 1024,
                    EnableBatchedOperations = true
                });
            }

            var numberOfMessages = 2000;

            var events = Enumerable.Range(1, numberOfMessages).Select(x => new Message
            {
                MessageId = Guid.NewGuid().ToString(),
                Label = $"FooMessage #{x}"
            });

            var sender = new MessageSender(connectionString, Constants.ReceiverQueueName);

            var tasks = events.Select(e => sender.SendAsync(e));

            Console.WriteLine($"Sending {numberOfMessages} messages...");
            await Task.WhenAll(tasks).ConfigureAwait(false);
            Console.WriteLine("Done.");

            await client.CloseAsync();
            await sender.CloseAsync();
        }
    }
}
