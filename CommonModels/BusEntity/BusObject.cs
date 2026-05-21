namespace CommonModels.BusEntity
{
    public class BusObject<T>
    {
        private BusObject() { }
        public BusObject(DateTime onCreate, T kafkaObject)
        {
            this.OnCreate = onCreate;
            this.KafkaObject = kafkaObject;
            this.MessageType = typeof(T).Name;
        }

        public DateTime OnCreate { get; set; }
        public T? KafkaObject {  get; set; }
        public string MessageType { get; set; } = string.Empty;
    }
}
