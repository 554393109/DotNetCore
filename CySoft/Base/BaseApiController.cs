using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.AspNetCore.Mvc;
using CySoft.Utility;
using CySoft.Utility.Extension;
using System.IO;

namespace CySoft.Base
{
    [ApiController]
    [Route("[controller]/[action]")]
    public class BaseApiController : ControllerBase
    {
        /// <summary>
        /// 获得所有请求参数
        /// </summary>
        /// <param name="NeedUrlDecode">是否需要解码，默认：false</param>
        /// <returns></returns>
        protected Hashtable GetParameters(bool NeedUrlDecode = true)
        {
            var param = new Hashtable();

            var Method = Request.Method.ValueOrEmpty("GET");
            var list_key = default(List<string>);

            try
            {
                if ("GET".Equals(Method, StringComparison.OrdinalIgnoreCase)
                && Request.Query?.Keys?.Count > 0)
                    list_key = Request.Query.Keys.ToList();
                else if ("POST".Equals(Method, StringComparison.OrdinalIgnoreCase)
                    && Request.Form?.Keys?.Count > 0)
                    list_key = Request.Form.Keys.ToList();
            }
            catch { }

            if (list_key is IList)
            {
                foreach (string key in list_key)
                {
                    try
                    {
                        if (!key.IsNullOrWhiteSpace())
                        {
                            var val = default(string);
                            if ("GET".Equals(Method, StringComparison.OrdinalIgnoreCase))
                                val = Request.Query[key].ValueOrEmpty();
                            else if ("POST".Equals(Method, StringComparison.OrdinalIgnoreCase))
                                val = Request.Form[key].ValueOrEmpty();

                            param[key] = NeedUrlDecode
                                ? val.Trim().UrlUnescape()
                                : val.Trim();
                        }
                    }
                    catch { }
                }
            }

            return param;
        }

        protected string GetParameter(string key, string defaultValue = "")
        {
            var value = default(string);

            var Method = Request.Method.ValueOrEmpty("GET");

            if ("GET".Equals(Method, StringComparison.OrdinalIgnoreCase)
                && Request.Query?.Keys?.Count > 0)
                value = Request.Query[key];

            else if ("POST".Equals(Method, StringComparison.OrdinalIgnoreCase)
                && Request.Form?.Keys?.Count > 0)
                value = Request.Form[key];

            if (value == null)
                value = defaultValue;

            return value;
        }

        /// <summary>
        /// 获得请求参数
        /// </summary>
        protected string GetStreamParameters()
        {
            var str_param = string.Empty;

            if (Request.Body.CanRead /*&& Request.Body.Length > 0*/)
            {
                // 接收从微信后台POST过来的数据
                using (Stream inputStream = Request.Body)
                {
                    var count = 0;
                    var buffer = new byte[1024];
                    var builder = new StringBuilder();

                    try
                    {
                        while ((count = inputStream.Read(buffer, 0, 1024)) > 0)
                            builder.Append(Encoding.UTF8.GetString(buffer, 0, count));

                        str_param = builder.ToString();         //读出报文
                    }
                    catch (Exception ex)
                    {
                        // LogHelper.Error(ex);
                    }
                    finally
                    {
                        inputStream.Flush();
                        inputStream.Close();
                        inputStream.Dispose();
                    }
                }
            }

            return str_param;
        }

        protected IActionResult CallBack(object response, string type = "json")
        {
            string result;
            string contentType = "text/plain";
            string _type = string.IsNullOrWhiteSpace(type)
                ? "json"
                : type.ToLower();

            if (response == null)
                result = string.Empty;
            else if (response.GetType().Equals(typeof(string)))
                result = response.ToString();
            else
            {
                switch (_type)
                {
                    case "json":
                    result = JSON.Serialize(response);
                    break;
                    case "xml":
                    result = XML.Serialize(response);
                    break;
                    case "text":
                    default:
                    result = JSON.Serialize(response);
                    break;
                }
            }

            switch (_type)
            {
                case "json":
                contentType = "application/json";
                break;
                case "xml":
                contentType = "application/xml";
                break;
                case "text":
                default:
                contentType = "text/plain";
                break;
            }

            return Content(
                content: result,
                contentType: contentType,
                contentEncoding: Encoding.UTF8);
        }
    }
}
