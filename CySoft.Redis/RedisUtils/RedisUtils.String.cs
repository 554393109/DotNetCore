/************************************************************************
 * 文件标识：  511ec32b-b2b9-4228-84fc-1f114fe535eb
 * 项目名称：  CySoft.Redis  
 * 项目描述：  
 * 类 名 称：  RedisUtils.String
 * 版 本 号：  v1.0.0.0 
 * 说    明：  
 * 作    者：  尹自强
 * 创建时间：  2018/6/8 13:47:13
 * 更新时间：  2018/6/8 13:47:13
************************************************************************
 * Copyright @ 尹自强 2018. All rights reserved.
************************************************************************/

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CySoft.Utility;
using CySoft.Utility.Extension;
using StackExchange.Redis;

namespace CySoft.Redis
{
    partial class RedisUtils
    {
        public sealed class String
        {
            #region Insert & Increment & Decrement

            /// <summary>
            /// 添加指定key的字符串
            /// </summary>
            /// <param name="key">缓存键</param>
            /// <param name="val">字符串</param>
            /// <param name="expire">TTL 小于等于零则永不过期 单位：秒</param>
            /// <param name="isFullKey">是否已经是完整的Key，如果不是，则会调用一次GetFinalKey()方法</param>
            /// <param name="db_num">db_num为-1时，使用UnifiedPayKey.Redis_DB_Num</param>
            public static void Insert(string key, string val, int expire = 10, bool isFullKey = false, int db_num = -1)
            {
                if (key.IsNullOrWhiteSpace() || val.IsNullOrWhiteSpace())
                    throw new ArgumentException("参数错误");

                var cacheKey = GetFinalKey(key, isFullKey);
                using (BeginCacheLock(LockResourcePrefix, cacheKey))
                {
                    try
                    {
                        var db = GetDatabase(db_num);
                        db.StringSet(cacheKey, val
                            , expiry: expire > 0 ? (Nullable<TimeSpan>)TimeSpan.FromSeconds(expire) : null
                            , when: When.Always
                            , flags: CommandFlags.FireAndForget);

#if DEBUG
                        var _value = db.StringGet(cacheKey);
#endif
                    }
                    catch (Exception ex)
                    {
                        LogHelper.RuntimeLog(new {
                            ReqId = string.Empty,
                            flag_type = 4,
                            target = string.Format("/{0}/{1}", System.Reflection.MethodBase.GetCurrentMethod().ReflectedType.FullName, new System.Diagnostics.StackTrace().GetFrame(0).GetMethod().Name),
                            LogContent = $"【异常】：【{JSON.Serialize(ex)}】"
                        });
                    }
                }
            }

            /// <summary>
            /// 添加指定key的字符串
            /// </summary>
            /// <param name="key">缓存键</param>
            /// <param name="obj">缓存值</param>
            /// <param name="expire">TTL 小于等于零则永不过期 单位：秒</param>
            /// <param name="isFullKey">是否已经是完整的Key，如果不是，则会调用一次GetFinalKey()方法</param>
            /// <param name="db_num">db_num为-1时，使用UnifiedPayKey.Redis_DB_Num</param>
            public static void Insert(string key, object obj, int expire = 10, bool isFullKey = false, int db_num = -1)
            {
                if (key.IsNullOrWhiteSpace() || obj.IsNullOrWhiteSpace())
                    throw new ArgumentException("参数错误");

                var val = default(string);
                if (obj.GetType().Equals(typeof(string)))
                    val = obj.ToString();
                else
                    val = JSON.Serialize(obj);

                Insert(key, val: val, expire: expire, isFullKey: isFullKey, db_num: db_num);
            }

            /// <summary>
            /// Double类型增量，
            /// 【注意】指定键首次初始化val类型后，只能继续使用相同类型进行增量减量
            /// </summary>
            /// <param name="key">缓存键</param>
            /// <param name="val">Double类型增量值</param>
            /// <param name="isFullKey">是否已经是完整的Key，如果不是，则会调用一次GetFinalKey()方法</param>
            /// <param name="db_num">db_num为-1时，使用UnifiedPayKey.Redis_DB_Num</param>
            public static void Increment(string key, double val, bool isFullKey = false, int db_num = -1)
            {
                if (key.IsNullOrWhiteSpace() || !val.IsDecimal())
                    throw new ArgumentException("参数错误");

                var cacheKey = GetFinalKey(key, isFullKey);
                using (BeginCacheLock(LockResourcePrefix, cacheKey))
                {
                    try
                    {
                        var db = GetDatabase(db_num);
#if DEBUG
                        var _valueOld = db.StringGet(cacheKey);
#endif

                        db.StringIncrement(cacheKey, val, CommandFlags.FireAndForget);

#if DEBUG
                        var _valueNew = db.StringGet(cacheKey);
#endif
                    }
                    catch (Exception ex)
                    {
                        LogHelper.RuntimeLog(new {
                            ReqId = string.Empty,
                            flag_type = 4,
                            target = string.Format("/{0}/{1}", System.Reflection.MethodBase.GetCurrentMethod().ReflectedType.FullName, new System.Diagnostics.StackTrace().GetFrame(0).GetMethod().Name),
                            LogContent = $"【异常】：【{JSON.Serialize(ex)}】"
                        });
                    }
                }
            }

            /// <summary>
            /// Double类型减量，
            /// 【注意】指定键首次初始化val类型后，只能继续使用相同类型进行增量减量
            /// </summary>
            /// <param name="key">缓存键</param>
            /// <param name="val">Double类型增量值</param>
            /// <param name="isFullKey">是否已经是完整的Key，如果不是，则会调用一次GetFinalKey()方法</param>
            /// <param name="db_num">db_num为-1时，使用UnifiedPayKey.Redis_DB_Num</param>
            public static void Decrement(string key, double val, bool isFullKey = false, int db_num = -1)
            {
                if (key.IsNullOrWhiteSpace() || !val.IsDecimal())
                    throw new ArgumentException("参数错误");

                var cacheKey = GetFinalKey(key, isFullKey);
                using (BeginCacheLock(LockResourcePrefix, cacheKey))
                {
                    try
                    {
                        var db = GetDatabase(db_num);

#if DEBUG
                        var _valueOld = db.StringGet(cacheKey);
#endif

                        db.StringDecrement(cacheKey, val, CommandFlags.FireAndForget);

#if DEBUG
                        var _valueNew = db.StringGet(cacheKey);
#endif
                    }
                    catch (Exception ex)
                    {
                        LogHelper.RuntimeLog(new {
                            ReqId = string.Empty,
                            flag_type = 4,
                            target = string.Format("/{0}/{1}", System.Reflection.MethodBase.GetCurrentMethod().ReflectedType.FullName, new System.Diagnostics.StackTrace().GetFrame(0).GetMethod().Name),
                            LogContent = $"【异常】：【{JSON.Serialize(ex)}】"
                        });
                    }
                }
            }

            /// <summary>
            /// Long类型增量，
            /// 【注意】指定键首次初始化val类型后，只能继续使用相同类型进行增量减量
            /// </summary>
            /// <param name="key">缓存键</param>
            /// <param name="val">Double类型增量值</param>
            /// <param name="isFullKey">是否已经是完整的Key，如果不是，则会调用一次GetFinalKey()方法</param>
            /// <param name="db_num">db_num为-1时，使用UnifiedPayKey.Redis_DB_Num</param>
            public static void Increment(string key, long val, bool isFullKey = false, int db_num = -1)
            {
                if (key.IsNullOrWhiteSpace() || !val.IsLong())
                    throw new ArgumentException("参数错误");

                var cacheKey = GetFinalKey(key, isFullKey);
                using (BeginCacheLock(LockResourcePrefix, cacheKey))
                {
                    try
                    {
                        var db = GetDatabase(db_num);

#if DEBUG
                        var _valueOld = db.StringGet(cacheKey);
#endif

                        db.StringIncrement(cacheKey, val, CommandFlags.FireAndForget);

#if DEBUG
                        var _valueNew = db.StringGet(cacheKey);
#endif
                    }
                    catch (Exception ex)
                    {
                        LogHelper.RuntimeLog(new {
                            ReqId = string.Empty,
                            flag_type = 4,
                            target = string.Format("/{0}/{1}", System.Reflection.MethodBase.GetCurrentMethod().ReflectedType.FullName, new System.Diagnostics.StackTrace().GetFrame(0).GetMethod().Name),
                            LogContent = $"【异常】：【{JSON.Serialize(ex)}】"
                        });
                    }
                }
            }

            /// <summary>
            /// Long类型减量，
            /// 【注意】指定键首次初始化val类型后，只能继续使用相同类型进行增量减量
            /// </summary>
            /// <param name="key">缓存键</param>
            /// <param name="val">Double类型增量值</param>
            /// <param name="isFullKey">是否已经是完整的Key，如果不是，则会调用一次GetFinalKey()方法</param>
            /// <param name="db_num">db_num为-1时，使用UnifiedPayKey.Redis_DB_Num</param>
            public static void Decrement(string key, long val, bool isFullKey = false, int db_num = -1)
            {
                if (key.IsNullOrWhiteSpace() || !val.IsLong())
                    throw new ArgumentException("参数错误");

                var cacheKey = GetFinalKey(key, isFullKey);
                using (BeginCacheLock(LockResourcePrefix, cacheKey))
                {
                    try
                    {
                        var db = GetDatabase(db_num);

#if DEBUG
                        var _valueOld = db.StringGet(cacheKey);
#endif

                        db.StringDecrement(cacheKey, val, CommandFlags.FireAndForget);

#if DEBUG
                        var _valueNew = db.StringGet(cacheKey);
#endif
                    }
                    catch (Exception ex)
                    {
                        LogHelper.RuntimeLog(new {
                            ReqId = string.Empty,
                            flag_type = 4,
                            target = string.Format("/{0}/{1}", System.Reflection.MethodBase.GetCurrentMethod().ReflectedType.FullName, new System.Diagnostics.StackTrace().GetFrame(0).GetMethod().Name),
                            LogContent = $"【异常】：【{JSON.Serialize(ex)}】"
                        });
                    }
                }
            }

            #endregion Insert & Increment & Decrement

            #region Remove

            #endregion Remove

            #region Get

            /// <summary>
            /// 获取指定缓存键的字符串
            /// </summary>
            /// <param name="key">缓存键</param>
            /// <param name="isFullKey"></param>
            /// <param name="db_num">db_num为-1时，使用UnifiedPayKey.Redis_DB_Num</param>
            /// <returns></returns>
            public static string Get(string key, bool isFullKey = false, int db_num = -1)
            {
                if (key.IsNullOrWhiteSpace())
                    throw new ArgumentException("key不能为空");

                var str = default(string);
                var cacheKey = GetFinalKey(key, isFullKey);
                using (BeginCacheLock(LockResourcePrefix, cacheKey))
                {
                    try
                    {
                        var db = GetDatabase(db_num);
                        var _RedisType = db.KeyType(cacheKey);
                        if (_RedisType == RedisType.String)
                            str = db.StringGet(cacheKey);
                    }
                    catch (Exception ex)
                    {
                        LogHelper.RuntimeLog(new {
                            ReqId = string.Empty,
                            flag_type = 4,
                            target = string.Format("/{0}/{1}", System.Reflection.MethodBase.GetCurrentMethod().ReflectedType.FullName, new System.Diagnostics.StackTrace().GetFrame(0).GetMethod().Name),
                            LogContent = $"【异常】：【{JSON.Serialize(ex)}】"
                        });
                    }
                }

                return str;
            }

            /// <summary>
            /// 取缓存 为null返回default(T)
            /// </summary>
            /// <typeparam name="T">对象类型，此处限定class, new()，若需取得string类型，直接使用非泛型方法</typeparam>
            /// <param name="key">缓存键</param>
            /// <param name="isFullKey"></param>
            /// <param name="db_num"></param>
            /// <returns></returns>
            public static T Get<T>(string key, bool isFullKey = false, int db_num = -1)
                where T : class, new()
            {
                if (key.IsNullOrWhiteSpace())
                    throw new ArgumentException("key不能为空");

                var result = Get(key, isFullKey, db_num);
                if (string.IsNullOrWhiteSpace(result))
                    return default(T);

                var obj = default(T);
                try
                {
                    if (result.GetType().Equals(typeof(T)))
                        obj = result as T;
                    else
                        obj = JSON.Deserialize<T>(result);
                }
                catch (Exception ex)
                {
                    LogHelper.RuntimeLog(new {
                        ReqId = string.Empty,
                        flag_type = 4,
                        target = string.Format("/{0}/{1}", System.Reflection.MethodBase.GetCurrentMethod().ReflectedType.FullName, new System.Diagnostics.StackTrace().GetFrame(0).GetMethod().Name),
                        LogContent = $"【异常】：【{JSON.Serialize(ex)}】"
                    });
                }

                return obj;
            }

            /// <summary>
            /// 获取所有Hash对象
            /// </summary>
            /// <param name="key">缓存键 默认：*</param>
            /// <param name="isFullKey">是否已经是完整的Key，如果不是，则会调用一次GetFinalKey()方法</param>
            /// <param name="db_num">db_num为-1时，使用UnifiedPayKey.Redis_DB_Num</param>
            /// <returns></returns>
            public static Hashtable GetAll(string key = "*", bool isFullKey = false, int db_num = -1)
            {
                var cacheKey = GetFinalKey(key, isFullKey);
                Hashtable hashs = null;

                using (BeginCacheLock(LockResourcePrefix, cacheKey))
                {
                    try
                    {
                        #region Lua

                        var script = new StringBuilder();
                        script.Append(" local ks = redis.call('KEYS', @keypattern) ");                  // local ks为定义一个局部变量，其中用于存储获取到的keys
                        script.Append(" return ks ");

                        var db = GetDatabase(db_num);

                        //Redis的keys模糊查询
                        var redisResult = db.ScriptEvaluate(
                            script: LuaScript.Prepare(script.ToString()),
                            parameters: new { keypattern = cacheKey });

                        if (!redisResult.IsNull)
                        {
                            hashs = new Hashtable();
                            string[] Keys = (string[])redisResult;
                            foreach (var _Key in Keys)
                            {
                                var _RedisType = db.KeyType(_Key);
                                if (_RedisType == RedisType.String)
                                {
                                    var val = db.StringGet(_Key);
                                    hashs.Add(_Key, val);
                                }
                                else if (_RedisType == RedisType.Hash)
                                {
                                    try
                                    {
                                        var val = db.HashGetAll(_Key);
                                        if (val is IList)
                                        {
                                            var dic = val.ToList().ToDictionary(k => k.Name, v => v.Value);
                                            hashs.Add(_Key, JSON.Serialize(new Hashtable(dic)));
                                        }
                                    }
                                    catch { }
                                }
                            }
                        }

                        #endregion Lua
                    }
                    catch (Exception ex)
                    {
                        LogHelper.RuntimeLog(new {
                            ReqId = string.Empty,
                            flag_type = 4,
                            target = string.Format("/{0}/{1}", System.Reflection.MethodBase.GetCurrentMethod().ReflectedType.FullName, new System.Diagnostics.StackTrace().GetFrame(0).GetMethod().Name),
                            LogContent = $"【异常】：【{JSON.Serialize(ex)}】"
                        });
                    }
                }

                return hashs;
            }

            #endregion Get
        }
    }
}
