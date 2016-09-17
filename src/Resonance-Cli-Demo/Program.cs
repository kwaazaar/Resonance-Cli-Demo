using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.PlatformAbstractions;
using Resonance;
using Resonance.Models;
using Resonance.Repo.Database;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace Resonance_Cli_Demo
{
    public class Program
    {
        private static IServiceProvider serviceProvider;

        public static void Main(string[] args)
        {
            // Configfile is used to read connectionstring from
            var builder = new ConfigurationBuilder()
                .SetBasePath(PlatformServices.Default.Application.ApplicationBasePath)
                //.SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
            //builder.AddEnvironmentVariables();
            var config = builder.Build();

            // Set up DB connection
            var connectionString = config.GetConnectionString("Resonance");
            var repoFactory = new MsSqlEventingRepoFactory(connectionString);

            // Get a resonance publisher and consumer. DI takes care of creating the instances, passing required args to ctors, etc.
            var publisher = new EventPublisher(repoFactory);
            var consumer = new EventConsumer(repoFactory);

            // Make sure the topic exists
            var topic = publisher.GetTopicByName("Resonance Demo Topic");
            if (topic == null)
                topic = publisher.AddOrUpdateTopic(new Topic
                {
                    Name = "Resonance Demo Topic",
                    Notes = "This topic is for demo purposes. Nothing to see here, move along!",
                });

            // Create a subscription
            var subscription = consumer.GetSubscriptionByName("Resonance Demo Subscription");
            if (subscription == null)
                subscription = consumer.AddOrUpdateSubscription(new Subscription
                {
                    Name = "Resonance Demo Subscription",
                    MaxDeliveries = 2,
                    Ordered = true, // Order on functional key (THIS is what you want, but never get with the commercial tools!)
                    TopicSubscriptions = new List<TopicSubscription> // A subscription can subscribe to multiple topics
                    {
                        new TopicSubscription // Subscribe to the topic created above
                        {
                            TopicId = topic.Id,
                            Enabled = true,
                        },
                    },
                });

            // Now publish an event to the topic
            publisher.Publish(
                topicName: "Resonance Demo Topic", functionalKey: "ABC123",
                headers: new Dictionary<string, string> { { "EventName", "PaymentReceived" }, { "MessageId", "12345" } },
                payload: new Tuple<string, int, string>("Robert", 40, "Holland")); // Publish typed object (publisher takes care of serialization)

            // Let consume it
            var consEvent = consumer.ConsumeNext<Tuple<string, int, string>>( // Consume typed object (consumer takes care of deserialization)
                "Resonance Demo Subscription", // Name of the subscription, remember: a subscription can subscribe to multiple topics, these will all be delivered together, ordered and all.
                60) // Visibility timeout (seconds): the event is 'locked' for us during this time and cannot be consumed (eg by another thread). When not marked consumed/failed, it will be redelivered again after this timeout expires.
                .FirstOrDefault(); // We only consume the first result (we didn't ask for more)

            if (consEvent != null) // Found it!
            {
                try
                {
                    // Handle the event
                    Console.WriteLine($"Hello {consEvent.Payload.Item1}, aged {consEvent.Payload.Item2}.");

                    // Mark it consumed/complete/successfully-processed.
                    consumer.MarkConsumed(consEvent.Id, consEvent.DeliveryKey);
                }
                catch (Exception ex)
                {
                    // Warning: when marking an event as failed, it will NOT be redelivered/retried!
                    // Doing nothing here is fine too: when the lock is released, it will be delivered again
                    consumer.MarkFailed(consEvent.Id, consEvent.DeliveryKey, Reason.Other(ex.ToString()));
                }
            }

            // Topic and subscription are removed. However, all published and consumed events are not removed.
            consumer.DeleteSubscription(subscription.Id);
            publisher.DeleteTopic(topic.Id, true);
        }
    }
}
