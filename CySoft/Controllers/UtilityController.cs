using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CySoft.Base;
using CySoft.Model;
using CySoft.Model.Enums;
using CySoft.Utility.UniqueID;
using Microsoft.AspNetCore.Mvc;

namespace CySoft.Controllers
{
    public class UtilityController : BaseApiController
    {
        [HttpGet, HttpPost]
        public IActionResult Generate_16()
        {
            return Ok(new {
                state = StateEnum.SUCCESS.GetName(),
                UniqueID = Utility.UniqueID.Generate_16.Generate()
            });
        }

        [HttpGet, HttpPost]
        public IActionResult Generate_19()
        {
            return Ok(new {
                state = StateEnum.SUCCESS.GetName(),
                UniqueID = Utility.UniqueID.Generate_19.Generate()
            });
        }

    }
}
