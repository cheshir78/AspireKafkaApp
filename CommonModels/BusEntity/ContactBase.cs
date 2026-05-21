using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

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
