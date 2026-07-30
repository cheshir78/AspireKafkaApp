namespace CommonModels.Contracts
{
    public interface IBusObjectHandlerFactory
    {
        IBusObjectHandler GetHandler(string rawJson);
    }
}
