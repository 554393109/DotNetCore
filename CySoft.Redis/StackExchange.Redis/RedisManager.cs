/************************************************************************
 * 文件标识：  a96d84d0-405a-41c7-a545-b5af70ae5f26
 * 项目名称：  CySoft.Redis  
 * 项目描述：  
 * 类 名 称：  RedisManager
 * 版 本 号：  v1.0.0.0 
 * 说    明：  ConnectionMultiplexer管理器
 *             StackExchange.Redis 不建议使用连接池等阻塞和等待方式。
 *             StackExchange.Redis 这里使用管道和多路复用的技术来实现减少连接。
 *             
 *             ★ StackExchange.Redis 只能使用v1.2.6.0，更高版本需要升级.Net Framework 4.6.1 ★
 *             
 * 作    者：  尹自强
 * 创建时间：  2018/6/8 13:36:09
 * 更新时间：  2018/6/8 13:36:09
************************************************************************
 * Copyright @ 尹自强 2018. All rights reserved.
************************************************************************/

using System.IO;
using System.Text;
using CySoft.Utility;
using StackExchange.Redis;

namespace CySoft.Redis
{
    public class RedisManager
    {
        private static readonly object locker = new object();

        /// <summary>
        /// Redis连接配置字符串
        /// </summary>
        public static string ConfigurationOption { get; set; } = "127.0.0.1:6379";

        #region 单例ConnectionMultiplexer

        /// <summary>
        /// ConnectionMultiplexer 单例
        /// </summary>
        private static ConnectionMultiplexer Instance
        {
            get {
                return NestedRedis.instance;
            }
        }

        /// <summary>
        /// 嵌套类；限制ConnectionMultiplexer为readonly
        /// </summary>
        private class NestedRedis
        {
            //由RedisCacheStrategy.RedisCacheStrategy()静态构造方法调用，不可删除
            static NestedRedis()
            {
            }

            // 将instance设为一个初始化的ConnectionMultiplexer新实例
            internal protected static readonly ConnectionMultiplexer instance = GetManager();
        }


        /// <summary>
        /// 实例化ConnectionMultiplexer；
        /// 无需DCL双重检查锁定
        /// </summary>
        /// <param name="connectionString">Redis连接配置</param>
        /// <returns></returns>
        private static ConnectionMultiplexer GetManager()
        {
            ConnectionMultiplexer connection = null;

            lock (locker)
            {
                var _ConfigurationOptions = ConfigurationOptions.Parse(ConfigurationOption);

                #region ConfigurationOptions 配置

                /*
                 * abortConnect ： 当为true时，当没有可用的服务器时则不会创建一个连接
                 * allowAdmin ： 当为true时 ，可以使用一些被认为危险的命令
                 * channelPrefix：所有pub/sub渠道的前缀
                 * connectRetry ：重试连接的次数
                 * connectTimeout：超时时间
                 * configChannel： Broadcast channel name for communicating configuration changes
                 * configCheckSeconds：检查配置的时间(秒)。如果它被支持，它可以作为交互式套接字的keep-alive。
                 * defaultDatabase ： 默认0到-1
                 * keepAlive ： 保存x秒的活动连接
                 * name:ClientName
                 * password:password
                 * proxy:代理 比如 Twemproxy
                 * resolveDns : 指定dns解析
                 * serviceName ： Not currently implemented (intended for use with sentinel)
                 * ssl={bool} ： 使用sll加密
                 * sslHost={string}	： 强制服务器使用特定的ssl标识
                 * syncTimeout={int} ： 异步超时时间
                 * tiebreaker={string}：Key to use for selecting a server in an ambiguous master scenario
                 * version={string} ： Redis version level (useful when the server does not make this available)
                 * writeBuffer={int} ： 输出缓存区的大小
                 */

                #endregion ConfigurationOptions 配置

                connection = ConnectionMultiplexer.Connect(_ConfigurationOptions, new RedisInfoLog());

                #region 注册事件

                connection.ConfigurationChanged += Connection_ConfigurationChanged;
                connection.ConfigurationChangedBroadcast += Connection_ConfigurationChangedBroadcast;
                connection.ConnectionFailed += Connection_ConnectionFailed;
                connection.ConnectionRestored += Connection_ConnectionRestored;
                connection.ErrorMessage += Connection_ErrorMessage;
                connection.HashSlotMoved += Connection_HashSlotMoved;
                connection.InternalError += Connection_InternalError;

                #endregion 注册事件
            }

            return connection;
        }

        #endregion 单例ConnectionMultiplexer

        /// <summary>
        /// 返回 ConnectionMultiplexer实例
        /// </summary>
        public static ConnectionMultiplexer Manager
        {
            get {
                return RedisManager.Instance;
            }
        }



        #region LogWriter 

        /// <summary>
        /// 为redis重写一套LogWriter
        /// </summary>
        private class RedisInfoLog : TextWriter
        {
            public override Encoding Encoding
            {
                get {
                    return Encoding.UTF8;
                }
            }

            public override void WriteLine(string format, object arg0)
            {
                if (string.IsNullOrWhiteSpace(format)) return;

                LogHelper.RuntimeLog(new {
                    ReqId = string.Empty,
                    flag_type = 88,
                    target = string.Format("/{0}/{1}", System.Reflection.MethodBase.GetCurrentMethod().ReflectedType.FullName, new System.Diagnostics.StackTrace().GetFrame(0).GetMethod().Name),
                    LogContent = $"{string.Format(format, arg0)}"
                });
            }

            public override void WriteLine(string format, object arg0, object arg1)
            {
                if (string.IsNullOrWhiteSpace(format)) return;

                LogHelper.RuntimeLog(new {
                    ReqId = string.Empty,
                    flag_type = 88,
                    target = string.Format("/{0}/{1}", System.Reflection.MethodBase.GetCurrentMethod().ReflectedType.FullName, new System.Diagnostics.StackTrace().GetFrame(0).GetMethod().Name),
                    LogContent = $"{string.Format(format, arg0, arg1)}"
                });
            }

            public override void WriteLine(string format, object arg0, object arg1, object arg2)
            {
                if (string.IsNullOrWhiteSpace(format)) return;

                LogHelper.RuntimeLog(new {
                    ReqId = string.Empty,
                    flag_type = 88,
                    target = string.Format("/{0}/{1}", System.Reflection.MethodBase.GetCurrentMethod().ReflectedType.FullName, new System.Diagnostics.StackTrace().GetFrame(0).GetMethod().Name),
                    LogContent = $"{string.Format(format, arg0, arg1, arg2)}"
                });
            }

            public override void WriteLine(string format, params object[] arg)
            {
                if (string.IsNullOrWhiteSpace(format)) return;

                LogHelper.RuntimeLog(new {
                    ReqId = string.Empty,
                    flag_type = 88,
                    target = string.Format("/{0}/{1}", System.Reflection.MethodBase.GetCurrentMethod().ReflectedType.FullName, new System.Diagnostics.StackTrace().GetFrame(0).GetMethod().Name),
                    LogContent = $"{string.Format(format, arg)}"
                });
            }

            public override void WriteLine(string value)
            {
                if (string.IsNullOrWhiteSpace(value)) return;

                LogHelper.RuntimeLog(new {
                    ReqId = string.Empty,
                    flag_type = 88,
                    target = string.Format("/{0}/{1}", System.Reflection.MethodBase.GetCurrentMethod().ReflectedType.FullName, new System.Diagnostics.StackTrace().GetFrame(0).GetMethod().Name),
                    LogContent = $"{value}"
                });
            }
        }

        #endregion LogWriter

        #region Redis 事件

        private static void Connection_ConfigurationChanged(object sender, EndPointEventArgs e)
        {
            LogHelper.RuntimeLog(new {
                ReqId = string.Empty,
                flag_type = 88,
                target = string.Format("/{0}/{1}", System.Reflection.MethodBase.GetCurrentMethod().ReflectedType.FullName, new System.Diagnostics.StackTrace().GetFrame(0).GetMethod().Name),
                LogContent = $"RedisManager ConfigurationChanged 配置更改，EndPoint：【{e.EndPoint}】"
            });
        }

        private static void Connection_ConfigurationChangedBroadcast(object sender, EndPointEventArgs e)
        {
            LogHelper.RuntimeLog(new {
                ReqId = string.Empty,
                flag_type = 88,
                target = string.Format("/{0}/{1}", System.Reflection.MethodBase.GetCurrentMethod().ReflectedType.FullName, new System.Diagnostics.StackTrace().GetFrame(0).GetMethod().Name),
                LogContent = $"RedisManager ConfigurationChangedBroadcast 通过发布订阅更新配置，EndPoint：【{e.EndPoint}】"
            });
        }

        private static void Connection_ConnectionFailed(object sender, ConnectionFailedEventArgs e)
        {
            LogHelper.RuntimeLog(new {
                ReqId = string.Empty,
                flag_type = 4,
                target = string.Format("/{0}/{1}", System.Reflection.MethodBase.GetCurrentMethod().ReflectedType.FullName, new System.Diagnostics.StackTrace().GetFrame(0).GetMethod().Name),
                LogContent = $"RedisManager ConnectionFailed 连接失败(如果重连成功将不会触发该事件) EndPoint：【{e.EndPoint}】，FailureType：【{e.FailureType}】，Exception：【{JSON.Serialize(e.Exception)}】"
            });
        }

        private static void Connection_ConnectionRestored(object sender, ConnectionFailedEventArgs e)
        {
            LogHelper.RuntimeLog(new {
                ReqId = string.Empty,
                flag_type = 4,
                target = string.Format("/{0}/{1}", System.Reflection.MethodBase.GetCurrentMethod().ReflectedType.FullName, new System.Diagnostics.StackTrace().GetFrame(0).GetMethod().Name),
                LogContent = $"RedisManager ConnectionRestored 重连原节点，EndPoint：【{e.EndPoint}】，FailureType：【{e.FailureType}】，Exception：【{JSON.Serialize(e.Exception)}】"
            });
        }

        private static void Connection_ErrorMessage(object sender, RedisErrorEventArgs e)
        {
            LogHelper.RuntimeLog(new {
                ReqId = string.Empty,
                flag_type = 4,
                target = string.Format("/{0}/{1}", System.Reflection.MethodBase.GetCurrentMethod().ReflectedType.FullName, new System.Diagnostics.StackTrace().GetFrame(0).GetMethod().Name),
                LogContent = $"RedisManager ErrorMessage 错误，EndPoint：【{e.EndPoint}】，Message：【{e.Message}】"
            });
        }

        private static void Connection_HashSlotMoved(object sender, HashSlotMovedEventArgs e)
        {
            LogHelper.RuntimeLog(new {
                ReqId = string.Empty,
                flag_type = 4,
                target = string.Format("/{0}/{1}", System.Reflection.MethodBase.GetCurrentMethod().ReflectedType.FullName, new System.Diagnostics.StackTrace().GetFrame(0).GetMethod().Name),
                LogContent = $"RedisManager HashSlotMoved 更改集群，HashSlot：【{e.HashSlot}】，OldEndPoint：【{e.OldEndPoint}】，NewEndPoint：【{e.NewEndPoint}】"
            });
        }

        private static void Connection_InternalError(object sender, InternalErrorEventArgs e)
        {
            LogHelper.RuntimeLog(new {
                ReqId = string.Empty,
                flag_type = 4,
                target = string.Format("/{0}/{1}", System.Reflection.MethodBase.GetCurrentMethod().ReflectedType.FullName, new System.Diagnostics.StackTrace().GetFrame(0).GetMethod().Name),
                LogContent = $"RedisManager InternalError 内部异常，EndPoint：【{e.EndPoint}】，ConnectionType：【{e.ConnectionType}】，Origin：【{e.Origin}】，Exception：【{JSON.Serialize(e.Exception)}】"
            });
        }

        #endregion Redis 事件
    }
}
