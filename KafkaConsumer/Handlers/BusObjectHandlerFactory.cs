using CommonModels.Contracts;
using System.Text.Json;

namespace KafkaConsumer.Handlers
{
    public class BusObjectHandlerFactory : IBusObjectHandlerFactory
    {
        private readonly IEnumerable<IBusObjectHandler> _handlers;

        public BusObjectHandlerFactory(IEnumerable<IBusObjectHandler> handlers)
        {
            _handlers = handlers;
        }

        public IBusObjectHandler GetHandler(string rawJson)
        {
            // Parse JSON to find the MessageType field
            using var doc = JsonDocument.Parse(rawJson);
            if (doc.RootElement.TryGetProperty("MessageType", out var typeProperty))
            {
                string messageType = typeProperty.GetString() ?? string.Empty;

                // looking for a processor who can work with this type
                var handler = _handlers.FirstOrDefault(h => h.CanHandle(messageType));
                if (handler != null) return handler;
            }

            throw new NotSupportedException("No suitable handler was found for this message type.");
        }
    }
}
