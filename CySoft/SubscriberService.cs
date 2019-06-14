using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DotNetCore.CAP;

namespace CySoft
{
    public interface ISubscriberService
    {
        void CheckReceivedMessage(DateTime datetime);
    }

    public class SubscriberService : ISubscriberService, ICapSubscribe
    {
        [CapSubscribe("xxx.services.show.time")]
        public void CheckReceivedMessage(DateTime datetime)
        {
            var dt_now = DateTime.Now;

            //return Ok(new {
            //    UniqueID = Generate_19.Generate(),
            //    Time = "{0:yyyy-MM-dd HH:mm:ss.fff}".ToFormat(dt_now),
            //    TimeSpan = dt_now.ToUnixTimestamp(TimestampAccuracy.Milliseconds),
            //});
        }
    }
}
