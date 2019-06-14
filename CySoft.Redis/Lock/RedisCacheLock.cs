/************************************************************************
 * 文件标识：  c941526b-4ffa-4b0f-bb53-718e32d79288
 * 项目名称：  CySoft.Redis
 * 项目描述：  
 * 类 名 称：  RedisCacheLock
 * 版 本 号：  v1.0.0.0 
 * 说    明：  Redis锁
 *             可用不同分布式锁方案对该锁进行重构
 * 作    者：  尹自强
 * 创建时间：  2018/6/12 17:55:38
 * 更新时间：  2018/6/12 17:55:38
************************************************************************
 * Copyright @ 尹自强 2018. All rights reserved.
************************************************************************/

using System;
using System.Collections.Generic;
using System.Threading;
using CySoft.Utility;
using CySoft.Utility.CacheUtils;

namespace CySoft.Redis
{
    public class RedisCacheLock : BaseCacheLock
    {
        /// <summary>
        /// 实例化Redis锁实例并立即抢夺锁
        /// </summary>
        /// <param name="resourceName">锁名称</param>
        /// <param name="key">{resourceName}-{key}</param>
        /// <param name="retryCount">失败重试获取锁次数 默认：0</param>
        /// <param name="retryDelay">失败重试每次获取锁最大延迟 默认：10ms</param>
        public RedisCacheLock(string resourceName, string key, int? retryCount = null, TimeSpan? retryDelay = null)
            : base(resourceName, key, retryCount ?? 0, retryDelay ?? TimeSpan.FromMilliseconds(10))
        {
            LockNow();//立即等待并抢夺锁
        }

        /// <summary>
        /// 锁存放容器
        /// </summary>
        private static Dictionary<string, object> LockPool = new Dictionary<string, object>();
        /// <summary>
        /// 随机数
        /// </summary>
        private static Random _rnd = new Random();
        /// <summary>
        /// 读取LockPool时的锁
        /// </summary>
        private static object lookPoolLock = new object();

        public override bool Lock(string resourceName)
        {
            return Lock(resourceName, 999999999 /*暂时不限制*/, new TimeSpan(0, 0, 0, 0, 10));
        }

        public override bool Lock(string resourceName, int retryCount, TimeSpan retryDelay)
        {
            if (SystemKey.RedisLock_Enable)
            {
                int currentRetry = 0;
                int maxRetryDelay = (int)retryDelay.TotalMilliseconds;
                while (currentRetry++ < retryCount)
                {
                    #region 尝试获得锁

                    var getLock = false;
                    try
                    {
                        lock (lookPoolLock)
                        {
                            if (LockPool.ContainsKey(resourceName))
                            {
                                getLock = false;//已被别人锁住，没有取得锁
                            }
                            else
                            {
                                LockPool.Add(resourceName, new object());//创建锁
                                getLock = true;//取得锁
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        LogHelper.RuntimeLog(new {
                            ReqId = string.Empty,
                            flag_type = 4,
                            target = string.Format("/{0}/{1}", System.Reflection.MethodBase.GetCurrentMethod().ReflectedType.FullName, new System.Diagnostics.StackTrace().GetFrame(0).GetMethod().Name),
                            LogContent = $"【本地同步锁异常】：【{JSON.Serialize(ex)}】"
                        });
                        getLock = false;
                    }

                    #endregion 尝试获得锁

                    if (getLock)
                    {
                        return true;//取得锁
                    }
                    Thread.Sleep(_rnd.Next(maxRetryDelay));
                }
                return false;
            }
            else
                return true;
        }

        public override void UnLock(string resourceName)
        {
            if (SystemKey.RedisLock_Enable)
            {
                lock (lookPoolLock)
                {
                    LockPool.Remove(resourceName);
                }
            }
        }
    }
}
