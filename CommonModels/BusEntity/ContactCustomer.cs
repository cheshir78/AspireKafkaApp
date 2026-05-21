using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

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
