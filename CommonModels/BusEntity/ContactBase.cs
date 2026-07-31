using System.Runtime.Serialization;

namespace CommonModels.BusEntity
{
    public class ContactBase
    {
        public ContactBase(string telephone1)
        {
            Telephone1 = telephone1;
        }

        [DataMember] public string Telephone1 { get; set; }

    }
}
