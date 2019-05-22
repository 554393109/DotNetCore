using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using CySoft.Base;

namespace CySoft.Controllers
{
    public class MchController : BaseApiController
    {
        [HttpGet]
        public IActionResult Index()
        {
            return base.CallBack(new { TimeSpan = $"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")}" });
        }

        [HttpGet, HttpPost]
        public IActionResult Gateway()
        {
            var param = base.GetParameters();
            return Ok(new { param });
        }
    }
}
