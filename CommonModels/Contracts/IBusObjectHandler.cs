using CommonModels.BusEntity;
using System.Text.Json;

namespace CommonModels.Contracts
{
    public interface IBusObjectHandler
    {
        bool CanHandle(string messageType);
        Task HandleAsync(string jsonMessage);
    }

    // Абстрактний базовий клас для типізованих обробників
    public abstract class BusObjectHandlerBase<T> : IBusObjectHandler
    {
        private readonly string _messageType;

        protected BusObjectHandlerBase()
        {
            _messageType = typeof(T).Name;
        }

        public bool CanHandle(string messageType) =>
            string.Equals(_messageType, messageType, StringComparison.OrdinalIgnoreCase);

        public async Task HandleAsync(string jsonMessage)
        {
            // Deserialized for a specific generic type
            var busObject = JsonSerializer.Deserialize<BusObject<T>>(jsonMessage);
            if (busObject != null)
            {
                await HandleInternalAsync(busObject);
            }
        }

        // This method will be implemented by specific services
        protected abstract Task HandleInternalAsync(BusObject<T> busObject);
    }
}
