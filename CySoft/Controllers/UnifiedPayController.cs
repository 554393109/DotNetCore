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
using MongoDB.Driver;
using MongoDB.Bson;
using System.Collections;

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

        public IActionResult MongoDB()
        {
            // MongoClient需要单例（相同的连接串会共享一个连接池）
            var client = new MongoClient("mongodb://127.0.0.1:27017");

            #region Demo

            //var database = client.GetDatabase("foo");
            //var collection = database.GetCollection<BsonDocument>("bar");

            //var document = new BsonDocument
            //{
            //    { "name", "SqlServer" },
            //    { "type", "Database" },
            //    { "count", 5 },
            //    { "info", new BsonDocument
            //        {
            //            { "x", 111 },
            //            { "y", 222 }
            //        }}
            //};

            //collection.InsertOne(document);

            #endregion Demo

            var database = client.GetDatabase("db_YZQ");
            var collection = database.GetCollection<BsonDocument>("collection_YZQ");

            var dt_begin = default(DateTime);
            var dt_end = default(DateTime);

            #region BulkWrite

            dt_begin = DateTime.Now;

            var len = 10000;
            var models = new WriteModel<BsonDocument>[len];
            for (int i = 0; i < len; i++)
            {
                var hash = new Hashtable() {
                { "Id", "{0:N}".ToFormat(Guid.NewGuid()) },
                { "MchId", "{0}".ToFormat(Generate_19.Generate()) },
                { "count", i },
                { "info", new BsonDocument
                    {
                        { "x", 111 },
                        { "y", 222 }
                    }
                }};

                var model = new InsertOneModel<BsonDocument>(new BsonDocument(hash));

                models.SetValue(model, i);
            }

            collection.BulkWrite(models);

            dt_end = DateTime.Now;
            var ts_BulkWrite = (dt_end - dt_begin).TotalMilliseconds.ToInt32();

            #endregion BulkWrite

            #region InsertMany

            dt_begin = DateTime.Now;

            var list_doc = new List<BsonDocument>();
            for (int i = 0; i < len; i++)
            {
                var hash = new Hashtable() {
                { "Id", "{0:N}".ToFormat(Guid.NewGuid()) },
                { "MchId", "{0}".ToFormat(Generate_19.Generate()) },
                { "count", i },
                { "info", new BsonDocument
                    {
                        { "x", 111 },
                        { "y", 222 }
                    }
                }};
                var document = new BsonDocument(hash);

                list_doc.Add(document);
            }

            collection.InsertMany(list_doc.ToArray());

            dt_end = DateTime.Now;
            var ts_InsertMany = (dt_end - dt_begin).TotalMilliseconds.ToInt32();

            #endregion InsertMany

            // 使用BulkWrite，可以在与mongoDB的单个连接中执行许多操作。在内部，InsertMany使用BulkWrite，所以没有区别，只是为了方便。如果单纯插入直接使用InsertMany

            var count = collection.EstimatedDocumentCount();

            var result = collection.Find(new BsonDocument()).ToList();

            return Ok(new { ts_BulkWrite, ts_InsertMany, total = count });
        }
    }
}
