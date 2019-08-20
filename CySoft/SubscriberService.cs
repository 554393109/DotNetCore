using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DotNetCore.CAP;

namespace CySoft
{
    public interface ISubscriberService
    {
        Task CheckReceivedMessage(DateTime datetime);
    }

    public class SubscriberService : ISubscriberService, ICapSubscribe
    {
        [CapSubscribe("#.Order.UnifiedPay")]
        public async Task CheckReceivedMessage(DateTime time)
        {
            var dt_now = DateTime.Now;

            //return Ok(new {
            //    UniqueID = Generate_19.Generate(),
            //    Time = "{0:yyyy-MM-dd HH:mm:ss.fff}".ToFormat(dt_now),
            //    TimeSpan = dt_now.ToUnixTimestamp(TimestampAccuracy.Milliseconds),
            //});
            await Console.Out.WriteLineAsync($@"{dt_now}, Subscriber invoked, Sent time:{time}");
        }
    }
}
