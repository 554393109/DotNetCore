using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using CySoft.Base;
using CySoft.Utility;
using CySoft.Utility.Extension;
using CySoft.Utility.UniqueID;
using DotNetCore.CAP;

namespace CySoft.Controllers
{
    public class UnifiedPayController : BaseApiController
    {
        [HttpGet]
        public IActionResult Index()
        {
            //return base.CallBack(new { ServerTime = $"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")}" });
            return Ok(new {
                ServerTimeSpan = DateTime.Now.ToUnixTimestamp(TimestampAccuracy.Milliseconds),
                UniqueID = Generate_19.Generate()
            });
        }

        [HttpGet, HttpPost]
        public IActionResult Gateway()
        {
            var param = base.GetParameters();
            var strStream = base.GetStreamParameters();

            return Ok(new { param, Stream = strStream });
            return Ok(new { Id = 99, Name = "我是名称", Member_Sex = "1" });
        }




        private readonly ICapPublisher _capBus;

        public UnifiedPayController(ICapPublisher capPublisher)
        {
            _capBus = capPublisher;
        }


        [HttpGet]
        [Route("~/without/transaction")]
        public IActionResult Publish()
        {
            var dt_now = DateTime.Now;
            //_capBus.Publish("xxx.services.show.time", dt_now);
            _capBus.PublishAsync("sample.rabbitmq.sqlserver", dt_now);

            return Ok(new {
                UniqueID = Generate_19.Generate(),
                Time = "{0:yyyy-MM-dd HH:mm:ss.fff}".ToFormat(dt_now),
                TimeSpan = dt_now.ToUnixTimestamp(TimestampAccuracy.Milliseconds),
            });
        }


        [NonAction]
        [CapSubscribe("#.rabbitmq.sqlserver")]
        public void Subscriber(DateTime time)
        {
            Console.WriteLine($@"{DateTime.Now}, Subscriber invoked, Sent time:{time}");
        }

        //[HttpGet, HttpPost]
        //[CapSubscribe("YZQ.services.show.time")]
        //public IActionResult CheckReceivedMessage(DateTime datetime)
        //{
        //    var dt_now = DateTime.Now;

        //    return Ok(new {
        //        UniqueID = Generate_19.Generate(),
        //        Time = "{0:yyyy-MM-dd HH:mm:ss.fff}".ToFormat(dt_now),
        //        TimeSpan = dt_now.ToUnixTimestamp(TimestampAccuracy.Milliseconds),
        //    });
        //}
    }
}
