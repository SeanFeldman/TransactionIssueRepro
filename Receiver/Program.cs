using System;

namespace Sender
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Transactions;
    using Microsoft.Azure.ServiceBus;
    using Microsoft.Azure.ServiceBus.Core;
    using Microsoft.Azure.ServiceBus.Management;
    using Serilog;
    using Serilog.Core;
    using Constants = Constants;

    class Program
    {
        private static Logger log;
        private static MessageSenderPool sendersPool;
        private static MessageReceiver receiver;
        private const int MaxConcurrency = 10;
        private static SemaphoreSlim semaphore = new SemaphoreSlim(MaxConcurrency, MaxConcurrency);

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

            receiver = new MessageReceiver(connectionString, Constants.ReceiverQueueName, ReceiveMode.PeekLock, default, 0);
            var connectionStringBuilder = new ServiceBusConnectionStringBuilder(connectionString);
            sendersPool = new MessageSenderPool(connectionStringBuilder, null);

            log = new LoggerConfiguration()
                .WriteTo.Console()
                .WriteTo.RollingFile("log-{Date}.txt")
                .CreateLogger();


            while (true)
            {
                await semaphore.WaitAsync();

                var receiveTask = receiver.ReceiveAsync();

                ProcessMessage(receiveTask)
                    .ContinueWith((t, s) =>
                    {
                        ((SemaphoreSlim) s).Release();
                    }, semaphore, TaskContinuationOptions.ExecuteSynchronously).Ignore();
            }
        }

        private static async Task ProcessMessage(Task<Message> receiveTask)
        {
            var incoming = await receiveTask;

            if (incoming == null)
            {
                return;
            }

            log.Information("Received FooMessage with ID {ID}, sleeping 30ms to simulate normal usage", incoming.MessageId);

            using (var tx = CreateTransactionScope())
            {
                await Task.Delay(30);

                const int numberOfEventsToPublish = 10;

                var events = Enumerable.Range(1, numberOfEventsToPublish).Select(x => new Message
                {
                    MessageId = Guid.NewGuid().ToString(),
                    Label = $"BarEvent #{x}"
                });

                var sender = sendersPool.GetMessageSender(Constants.TopicName, (receiver.ServiceBusConnection, receiver.Path));

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
                finally
                {
                    sendersPool.ReturnMessageSender(sender);
                }
            }
        }

        static TransactionScope CreateTransactionScope()
        {
            return new TransactionScope(TransactionScopeOption.RequiresNew, new TransactionOptions
            {
                IsolationLevel = IsolationLevel.Serializable,
                Timeout = TransactionManager.MaximumTimeout
            }, TransactionScopeAsyncFlowOption.Enabled);
        }
    }
}
