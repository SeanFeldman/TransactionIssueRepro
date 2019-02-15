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

            if (!await client.TopicExistsAsync(Constants.TopicName))
            {
                throw new Exception($"Topic '{Constants.TopicName}' should be created first and seeded.");
            }

            if (!await client.QueueExistsAsync(Constants.ReceiverQueueName))
            {
                await client.CreateQueueAsync(new QueueDescription(Constants.ReceiverQueueName)
                {
                    MaxDeliveryCount = int.MaxValue,
                    LockDuration = TimeSpan.FromMinutes(5),
                    MaxSizeInMB = 5 * 1024,
                    EnableBatchedOperations = true
                });
            }

            var subscription = new SubscriptionDescription(Constants.TopicName, Constants.SubscriptionName)
            {
                EnableBatchedOperations = true,
                MaxDeliveryCount = int.MaxValue,
                ForwardTo = Constants.ReceiverQueueName
            };

            var rule = new RuleDescription(Constants.ReceiverQueueName, new SqlFilter("sys.Label LIKE 'FooEvent%'"));

            await client.CreateSubscriptionAsync(subscription, rule);

            await client.CloseAsync();
        }
    }

}
