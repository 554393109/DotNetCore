using System;
using System.Collections;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Web;
//using CySoft.MessgeQueue.Client.Model;
using CySoft.Utility.Extension;

namespace CySoft.Utility
{
    public class LogHelper
    {

        /// <summary>
        /// 错误
        /// </summary>
        /// <param name="info"></param>
        /// <param name="se"></param>
        public static void Error(string error)
        {
            //if (logerror.IsErrorEnabled)
            //    logerror.Error(error);
        }
        /// <summary>
        /// 错误
        /// </summary>
        /// <param name="info"></param>
        /// <param name="se"></param>
        public static void Error(Exception ex)
        {
            //if (logerror.IsErrorEnabled)
            //    logerror.Error(ex.Message, ex);
        }
        /// <summary>
        /// 错误
        /// </summary>
        /// <param name="info"></param>
        /// <param name="se"></param>
        public static void Error(string error, Exception ex)
        {
            //if (logerror.IsErrorEnabled)
            //    logerror.Error(error, ex);
        }

        /// <summary>
        /// 通过队列写入日志
        /// </summary>
        /// <param name="_RuntimeLog">CySoft.Model.Other.RuntimeLog</param>
        public static void RuntimeLog(dynamic _RuntimeLog)
        {
            if (_RuntimeLog != null)
            {
                //string ReqId = HttpContext.Current?.Session.SessionID ?? $"{Guid.NewGuid().ToString("N").Substring(8, 24)}";
                //var mb = new StackTrace().GetFrame(1).GetMethod();
                //var target = string.Format("/{0}/{1}", mb.DeclaringType, mb.Name);

                //try
                //{
                //    Hashtable RuntimeLog = JSON.ConvertToType<Hashtable>(_RuntimeLog);
                //    RuntimeLog["ReqId"] = RuntimeLog["ReqId"].ValueOrEmpty(ReqId);
                //    RuntimeLog["target"] = RuntimeLog["target"].ValueOrEmpty(target);
                //    var dt_log = default(DateTime);
                //    if (!RuntimeLog["rq_log"].IsDateTime()
                //        || !DateTime.TryParse(RuntimeLog["rq_log"].ToString(), out dt_log)
                //        || dt_log == new DateTime(1900, 1, 1)
                //        || dt_log == DateTime.MinValue)
                //        dt_log = DateTime.Now;

                //    if (!RuntimeLog["ServerIP"].IsIP())
                //        RuntimeLog["ServerIP"] = Globals.ServerIP;

                //    RuntimeLog["rq_log"] = dt_log;
                //    RuntimeLog.Remove("rq_create");

                //    var mq_msg = new MQMsg(UnifiedPayKey.MQ_Consumer_Log_Runtime_Uri) { id = ReqId, lable = RuntimeLog["target"].ToString(), data = JSON.Serialize(RuntimeLog.UrlEscape()) };
                //    RabbitMQHelper.PublishAsync(UnifiedPayKey.MQ_RuntimeLog, mq_msg);
                //}
                //catch (Exception ex)
                //{
                //    LogHelper.Error(ex);

                //    var mq_msg_dead = new MQMsg(string.Empty) { id = ReqId, lable = target, data = JSON.Serialize(new { param = _RuntimeLog, info = ex.Message.UrlEscape() }) };
                //    RabbitMQHelper.PublishAsync(UnifiedPayKey.MQ_Dead, mq_msg_dead);
                //}

            }
        }
    }
}