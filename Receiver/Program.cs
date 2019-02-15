﻿using System;

namespace Sender
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using System.Transactions;
    using Microsoft.Azure.ServiceBus;
    using Microsoft.Azure.ServiceBus.Core;
    using Microsoft.Azure.ServiceBus.Management;
    using Serilog;

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

            if (!await client.SubscriptionExistsAsync(Constants.TopicName, Constants.SubscriptionName))
            {

                var subscription = new SubscriptionDescription(Constants.TopicName, Constants.SubscriptionName)
                {
                    EnableBatchedOperations = true,
                    MaxDeliveryCount = int.MaxValue,
                    ForwardTo = Constants.ReceiverQueueName
                };

                var rule = new RuleDescription(Constants.ReceiverQueueName,
                    new SqlFilter("sys.Label LIKE 'FooEvent%'"));

                await client.CreateSubscriptionAsync(subscription, rule);
            }

            await client.CloseAsync();

            var connection = new ServiceBusConnection(connectionString);
            var receiver = new MessageReceiver(connection, Constants.ReceiverQueueName, ReceiveMode.PeekLock);
            receiver.PrefetchCount = 0;
            var sender = new MessageSender(connection, Constants.TopicName, Constants.ReceiverQueueName);

            var log = new LoggerConfiguration()
                .WriteTo.Console()
                .WriteTo.RollingFile("log-{Date}.txt")
                .CreateLogger();


            while (true)
            {
                var incoming = await receiver.ReceiveAsync();

                log.Information("Received FooEvent with ID {ID}, sleeping 30ms to simulate normal usage", incoming.MessageId);

                using (var tx = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
                {

                    await Task.Delay(30);

                    var numberOfEventsToPublish = 10;

                    var events = Enumerable.Range(1, numberOfEventsToPublish).Select(x => new Message
                    {
                        MessageId = Guid.NewGuid().ToString(),
                        Label = $"BarEvent #{x}"
                    });

                    try
                    {
                        var tasks = new List<Task>(numberOfEventsToPublish);
                        tasks.AddRange(events.Select(@event => sender.SendAsync(@event)));

                        await Task.WhenAll(tasks).ConfigureAwait(false);

                        await receiver.CompleteAsync(incoming.SystemProperties.LockToken);

                        tx.Complete();

                        log.Information($"Published {numberOfEventsToPublish} BarEvent events.");
                    }
                    catch (Exception exception)
                    {
                        log.Error(exception, "Handler failed");

                        try
                        {
                            await receiver.AbandonAsync(incoming.SystemProperties.LockToken);
                        }
                        catch (Exception e)
                        {
                            log.Debug(e, "Failed to complete message with ID {ID}", incoming.MessageId);
                        }
                    }
                }
            }
        }
    }
}
