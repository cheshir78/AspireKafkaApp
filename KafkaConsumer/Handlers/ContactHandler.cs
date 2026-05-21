using CommonModels.BusEntity;
using CommonModels.Contracts;
using Microsoft.Extensions.Logging;

namespace KafkaConsumer.Handlers
{
    // Employee handler
    public class ContactEmployeeHandler : BusObjectHandlerBase<ContactEmployee>
    {
        private readonly ILogger<ContactEmployeeHandler> _logger;

        public ContactEmployeeHandler(ILogger<ContactEmployeeHandler> logger)
        {
            _logger = logger;
        }

        protected override Task HandleInternalAsync(BusObject<ContactEmployee> busObject)
        {
            _logger.LogInformation($"[Employee Handler] phoneNumber: {busObject.KafkaObject?.Telephone1}");
            // Ваша бізнес-логіка (запис в БД тощо)
            return Task.CompletedTask;
        }
    }

    // Customer handler
    public class ContactPartnerHandler : BusObjectHandlerBase<ContactCustomer>
    {
        private readonly ILogger<ContactPartnerHandler> _logger;

        public ContactPartnerHandler(ILogger<ContactPartnerHandler> logger) => _logger = logger;

        protected override Task HandleInternalAsync(BusObject<ContactCustomer> busObject)
        {
            _logger.LogInformation($"[Customer Handler] phone number: {busObject.KafkaObject?.Telephone1}, mono: {busObject.KafkaObject?.IsMono}");
            return Task.CompletedTask;
        }
    }
}
