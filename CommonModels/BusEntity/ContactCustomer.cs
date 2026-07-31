using System.Runtime.Serialization;

namespace CommonModels.BusEntity
{
    public class ContactCustomer : ContactBase
    {

        [DataMember] public bool? IsEmployee { get; set; }
        public bool? IsMono { get; set; }

        private ContactCustomer() : base(string.Empty)
        {
        }

        public ContactCustomer(string telephone1) : base(telephone1)
        {
        }
    }
}
