namespace Sender
{
    using System;
    using System.Collections.Concurrent;
    using Microsoft.Azure.ServiceBus;
    using Microsoft.Azure.ServiceBus.Core;

    class MessageSenderPool
    {
        public MessageSenderPool()
        {
            senders = new ConcurrentDictionary<(string, (ServiceBusConnection, string)), ConcurrentQueue<MessageSender>>();
        }

        public MessageSender GetMessageSender(string destination, (ServiceBusConnection connection, string path) receiverConnectionAndPath)
        {
            var sendersForDestination = senders.GetOrAdd((destination, receiverConnectionAndPath), _ => new ConcurrentQueue<MessageSender>());

            if (!sendersForDestination.TryDequeue(out var sender) || sender.IsClosedOrClosing)
            {
                // Send-Via case
                sender = new MessageSender(receiverConnectionAndPath.connection, destination, receiverConnectionAndPath.path);
                Console.WriteLine($"!!!!!!!!!!!!!!!!!!!! {sender.ClientId} !!!!!!!!!!!!!!!!!!!!");
            }

            return sender;
        }

        public void ReturnMessageSender(MessageSender sender)
        {
            if (sender.IsClosedOrClosing)
            {
                return;
            }

            // TODO: Path and TransferDestinationPath are swapped because of the https://github.com/Azure/azure-service-bus-dotnet/issues/569 issue
            if (senders.TryGetValue((sender.TransferDestinationPath, (sender.ServiceBusConnection, sender.Path)), out var sendersForDestination))
            {
                sendersForDestination.Enqueue(sender);
            }
        }

        ConcurrentDictionary<(string destination, (ServiceBusConnection connnection, string incomingQueue)), ConcurrentQueue<MessageSender>> senders;
    }
}