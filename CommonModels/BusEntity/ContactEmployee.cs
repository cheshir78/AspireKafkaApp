using System.Runtime.Serialization;

namespace CommonModels.BusEntity
{
    public class ContactEmployee : ContactBase
    {

        [DataMember] public bool? IsEmployee { get; set; }

        private ContactEmployee() : base(string.Empty)
        {
        }

        public ContactEmployee(string telephone1) : base(telephone1)
        {
        }
    }
}
