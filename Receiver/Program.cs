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

            await CreateInfrastructure(connectionString).ConfigureAwait(false);

            receiver = new MessageReceiver(connectionString, Constants.ReceiverQueueName, ReceiveMode.PeekLock, default, 0);
            var connectionStringBuilder = new ServiceBusConnectionStringBuilder(connectionString);
            sendersPool = new MessageSenderPool(connectionStringBuilder, null);

            log = new LoggerConfiguration()
                .WriteTo.Console()
                .WriteTo.RollingFile("log-{Date}.txt")
                .CreateLogger();


            while (true)
            {
                await semaphore.WaitAsync().ConfigureAwait(false);

                var receiveTask = receiver.ReceiveAsync();

                ProcessMessage(receiveTask)
                    .ContinueWith((t, s) =>
                    {
                        ((SemaphoreSlim) s).Release();
                    }, semaphore, TaskContinuationOptions.ExecuteSynchronously).Ignore();
            }
        }

        static async Task CreateInfrastructure(string connectionString)
        {
            var client = new ManagementClient(connectionString);

            if (!await client.TopicExistsAsync(Constants.TopicName).ConfigureAwait(false))
            {
                throw new Exception($"Topic '{Constants.TopicName}' should be created first and seeded.");
            }

            if (!await client.QueueExistsAsync(Constants.ReceiverQueueName).ConfigureAwait(false))
            {
                await client.CreateQueueAsync(new QueueDescription(Constants.ReceiverQueueName)
                {
                    MaxDeliveryCount = int.MaxValue,
                    LockDuration = TimeSpan.FromMinutes(5),
                    MaxSizeInMB = 5 * 1024,
                    EnableBatchedOperations = true
                }).ConfigureAwait(false);
            }

            if (!await client.SubscriptionExistsAsync(Constants.TopicName, Constants.SubscriptionName).ConfigureAwait(false))
            {
                var subscription = new SubscriptionDescription(Constants.TopicName, Constants.SubscriptionName)
                {
                    EnableBatchedOperations = true,
                    MaxDeliveryCount = int.MaxValue,
                    ForwardTo = Constants.ReceiverQueueName
                };

                var rule = new RuleDescription(Constants.ReceiverQueueName, new SqlFilter("sys.Label LIKE 'FooEvent%'"));

                await client.CreateSubscriptionAsync(subscription, rule).ConfigureAwait(false);
            }

            await client.CloseAsync().ConfigureAwait(false);
        }

        private static async Task ProcessMessage(Task<Message> receiveTask)
        {
            const int numberOfEventsToPublish = 10;

            var incoming = await receiveTask;

            if (incoming == null)
            {
                return;
            }

            log.Information("Received FooMessage with ID {ID}, sleeping 30ms to simulate normal usage", incoming.MessageId);

            using (var tx = CreateTransactionScope())
            {
                IEnumerable<Message> events;

                using (var suppress = new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled))
                {
                    await Task.Delay(30).ConfigureAwait(false);

                    events = Enumerable.Range(1, numberOfEventsToPublish).Select(x => new Message
                    {
                        MessageId = Guid.NewGuid().ToString(),
                        Label = $"BarEvent #{x}"
                    });

                    suppress.Complete();
                }

                var sender = sendersPool.GetMessageSender(Constants.TopicName, (receiver.ServiceBusConnection, receiver.Path));

                try
                {
                    var tasks = new List<Task>(numberOfEventsToPublish);
                    tasks.AddRange(events.Select(@event => sender.SendAsync(@event)));

                    await Task.WhenAll(tasks).ConfigureAwait(false);

                    await receiver.CompleteAsync(incoming.SystemProperties.LockToken).ConfigureAwait(false);

                    tx.Complete();

                    log.Information($"Published {numberOfEventsToPublish} BarEvent events.");
                }
                catch (Exception exception)
                {
                    log.Error(exception, "Handler failed");

                    try
                    {
                        await receiver.AbandonAsync(incoming.SystemProperties.LockToken).ConfigureAwait(false);
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
