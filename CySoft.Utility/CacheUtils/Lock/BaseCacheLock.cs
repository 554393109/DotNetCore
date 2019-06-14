/************************************************************************
 * 文件标识：  c941526b-4ffa-4b0f-bb53-718e32d79291
 * 项目名称：  CySoft.Utility.CacheUtils  
 * 项目描述：  
 * 类 名 称：  BaseCacheLock
 * 版 本 号：  v1.0.0.0 
 * 说    明：  本地锁
 * 作    者：  尹自强
 * 创建时间：  2018/5/23 17:55:38
 * 更新时间：  2018/5/23 17:55:38
************************************************************************
 * Copyright @ 尹自强 2018. All rights reserved.
************************************************************************/

using System;

namespace CySoft.Utility.CacheUtils
{
    /// <summary>
    /// 缓存同步锁基类
    /// </summary>
    public abstract class BaseCacheLock : ICacheLock
    {
        protected string _resourceName;
        protected int _retryCount;
        protected TimeSpan _retryDelay;
        public bool LockSuccessful { get; set; }

        /// <summary>
        /// 后期移除IBaseCacheStrategy strategy参数
        /// </summary>
        /// <param name="strategy"></param>
        /// <param name="resourceName"></param>
        /// <param name="key"></param>
        /// <param name="retryCount"></param>
        /// <param name="retryDelay"></param>
        protected BaseCacheLock(string resourceName, string key, int retryCount, TimeSpan retryDelay)
        {
            /* 加上Key可以针对某个Key加锁 */
            _resourceName = resourceName + (string.IsNullOrWhiteSpace(key) ? string.Empty : string.Format("-{0}", key));

            _retryCount = retryCount;
            _retryDelay = retryDelay;
        }

        /// <summary>
        /// 立即开始锁定，需要在子类的构造函数中执行
        /// </summary>
        /// <returns></returns>
        protected ICacheLock LockNow()
        {
            if (_retryCount != 0 && _retryDelay.Ticks != 0)
            {
                LockSuccessful = Lock(_resourceName, _retryCount, _retryDelay);
            }
            else
            {
                LockSuccessful = Lock(_resourceName);
            }
            return this;
        }

        public void Dispose()
        {
            UnLock(_resourceName);
        }

        public abstract bool Lock(string resourceName);

        public abstract bool Lock(string resourceName, int retryCount, TimeSpan retryDelay);

        public abstract void UnLock(string resourceName);
    }
}
