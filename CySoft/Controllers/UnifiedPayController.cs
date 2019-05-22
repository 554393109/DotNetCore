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
    }
}
