using CommonModels.Contracts;

namespace KafkaConsumer.Handlers
{
    public interface IBusObjectHandlerFactory
    {
        IBusObjectHandler GetHandler(string rawJson);
    }
}
