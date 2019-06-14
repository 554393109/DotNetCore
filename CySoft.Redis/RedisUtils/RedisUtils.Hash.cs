/************************************************************************
 * 文件标识：  b1266e42-55d5-4670-b519-7bf025afd799
 * 项目名称：  CySoft.Redis  
 * 项目描述：  
 * 类 名 称：  RedisUtils.Hash
 * 版 本 号：  v1.0.0.0 
 * 说    明：  
 * 作    者：  尹自强
 * 创建时间：  2018/6/8 13:47:02
 * 更新时间：  2018/6/8 13:47:02
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
        public sealed class Hash
        {
            #region Exists

            /// <summary>
            /// 检查Hash中是否存在Field及对象
            /// </summary>
            /// <param name="key">缓存键</param>
            /// <param name="field">项名</param>
            /// <param name="isFullKey">是否已经是完整的Key，如果不是，则会调用一次GetFinalKey()方法</param>
            /// <param name="db_num">db_num为-1时，使用UnifiedPayKey.Redis_DB_Num</param>
            /// <returns></returns>
            public static bool ExistsField(string key, string field, bool isFullKey = false, int db_num = -1)
            {
                if (key.IsNullOrWhiteSpace() || field.IsNullOrWhiteSpace())
                    throw new ArgumentException("key和field不能为空");

                bool exists = false;
                var cacheKey = GetFinalKey(key, isFullKey);
                using (BeginCacheLock(LockResourcePrefix, cacheKey))
                {
                    try
                    {
                        var db = GetDatabase(db_num);
                        var _RedisType = db.KeyType(cacheKey);
                        if (_RedisType == RedisType.Hash)
                            exists = db.HashExists(key, field);
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

                return exists;
            }

            #endregion Exists

            #region Insert

            /// <summary>
            /// 添加指定key的Hash对象
            /// </summary>
            /// <param name="key">缓存键</param>
            /// <param name="hash">Hash对象</param>
            /// <param name="expire">TTL 小于等于零则永不过期 单位：秒</param>
            /// <param name="isFullKey">是否已经是完整的Key，如果不是，则会调用一次GetFinalKey()方法</param>
            /// <param name="db_num">db_num为-1时，使用UnifiedPayKey.Redis_DB_Num</param>
            public static void Insert(string key, IDictionary hash, int expire = 10, bool isFullKey = false, int db_num = -1)
            {
                if (key.IsNullOrWhiteSpace() /*|| expire <= 0 */|| !(hash is IDictionary))
                    throw new ArgumentException("参数错误");

                #region 构造储存结构

                var _hash = hash as IDictionary;
                var list_field = new List<HashEntry>();
                foreach (string Key in _hash.Keys)
                {
                    if (Key.IsNullOrWhiteSpace())
                        throw new ArgumentNullException(paramName: "hash", message: string.Format("Hash中Key不能为空，hash：{0}", JSON.Serialize(hash)));

                    list_field.Add(new HashEntry(
                        Key.ToString(),
                        _hash[Key].ValueOrEmpty()));
                }

                if (list_field == null || list_field.Count == 0)
                    //throw new ArgumentNullException(paramName: "hash", message: string.Format("Hash中合法项为空，hash：{0}", JSON.Serialize(hash)));
                    return;

                #endregion 构造储存结构

                var cacheKey = GetFinalKey(key, isFullKey);
                using (BeginCacheLock(LockResourcePrefix, cacheKey))
                {
                    try
                    {
                        var db = GetDatabase(db_num);
                        db.HashSet(cacheKey, list_field.ToArray(), flags: CommandFlags.FireAndForget);
                        if (expire > 0)
                            db.KeyExpire(cacheKey, DateTime.Now.AddSeconds(expire), flags: CommandFlags.FireAndForget);

#if DEBUG
                        var _value = db.HashValues(cacheKey);
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
            /// 批量插入多个Hash
            /// K：Key
            /// V：Hashtable
            /// </summary>
            /// <param name="hashs">Hash对象集合</param>
            /// <param name="expire">TTL 小于等于零则永不过期 单位：秒</param>
            /// <param name="isFullKey">是否已经是完整的Key，如果不是，则会调用一次GetFinalKey()方法</param>
            /// <param name="db_num">db_num为-1时，使用UnifiedPayKey.Redis_DB_Num</param>
            public static void Insert(Hashtable hashs, int expire = 10, bool isFullKey = false, int db_num = -1)
            {
                if (hashs == null/* || expire <= 0*/)
                    throw new ArgumentException(message: "参数错误");

                #region 构造储存结构

                var dic_hash = new Dictionary<string, HashEntry[]>();
                foreach (string Key in hashs.Keys)
                {
                    if (Key.IsNullOrWhiteSpace() || !(hashs[Key] is IDictionary))
                        throw new ArgumentException(message: "hashs参数错误");

                    var hash_field = hashs[Key] as IDictionary;
                    var list_field = new List<HashEntry>();
                    foreach (string key_field in hash_field.Keys)
                    {
                        if (key_field.IsNullOrWhiteSpace())
                            throw new ArgumentNullException(paramName: "key_field", message: $"Hash中KEY不能为空，hash_field：{JSON.Serialize(hash_field)}");

                        list_field.Add(new HashEntry(
                            key_field,
                            hash_field[key_field].ValueOrEmpty()));
                    }

                    if (list_field == null || list_field.Count == 0)
                        throw new ArgumentNullException(paramName: "list_field", message: $"Hash中合法项为空，Key：{Key}，hash_field：{JSON.Serialize(hash_field)}");

                    var cacheKey = GetFinalKey(Key, isFullKey);
                    dic_hash.Add(cacheKey, list_field.ToArray());
                }

                #endregion 构造储存结构

                using (BeginCacheLock(LockResourcePrefix, "HashInsert"))
                {
                    try
                    {
                        var db = GetDatabase(db_num);
                        var batch = db.CreateBatch();

                        foreach (string cacheKey in dic_hash.Keys)
                        {
                            batch.HashSetAsync(key: cacheKey, hashFields: dic_hash[cacheKey], flags: CommandFlags.FireAndForget);
                            if (expire > 0)
                                batch.KeyExpireAsync(key: cacheKey, expiry: DateTime.Now.AddSeconds(expire), flags: CommandFlags.FireAndForget);
                        }

                        batch.Execute();
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

            #endregion Insert

            #region Remove

            /// <summary>
            /// 移除Hash对象指定Field项
            /// </summary>
            /// <param name="key">缓存键</param>
            /// <param name="field">项名</param>
            /// <param name="isFullKey">是否已经是完整的Key，如果不是，则会调用一次GetFinalKey()方法</param>
            /// <param name="db_num">db_num为-1时，使用UnifiedPayKey.Redis_DB_Num</param>
            public static void RemoveField(string key, string field, bool isFullKey = false, int db_num = -1)
            {
                if (key.IsNullOrWhiteSpace() || field.IsNullOrWhiteSpace())
                    throw new ArgumentException("key和field不能为空");

                var cacheKey = GetFinalKey(key, isFullKey);
                using (BeginCacheLock(LockResourcePrefix, cacheKey))
                {
                    try
                    {
                        var db = GetDatabase(db_num);
                        var _RedisType = db.KeyType(cacheKey);
                        if (_RedisType == RedisType.Hash)
                            db.HashDelete(cacheKey, hashField: field, flags: CommandFlags.FireAndForget);//删除项
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
            /// 移除Hash对象指定Field项集合
            /// </summary>
            /// <param name="key">缓存键</param>
            /// <param name="arr_field">项名集合</param>
            /// <param name="isFullKey">是否已经是完整的Key，如果不是，则会调用一次GetFinalKey()方法</param>
            /// <param name="db_num">db_num为-1时，使用UnifiedPayKey.Redis_DB_Num</param>
            public static void RemoveField(string key, string[] arr_field, bool isFullKey = false, int db_num = -1)
            {
                if (key.IsNullOrWhiteSpace() || null == arr_field || 0 == arr_field.Length)
                    throw new ArgumentException("key和arr_field不能为空");

                var list_field = new List<RedisValue>();
                foreach (string field in arr_field)
                    list_field.Add(field);

                var cacheKey = GetFinalKey(key, isFullKey);
                using (BeginCacheLock(LockResourcePrefix, cacheKey))
                {
                    try
                    {
                        var db = GetDatabase(db_num);
                        var _RedisType = db.KeyType(cacheKey);
                        if (_RedisType == RedisType.Hash)
                            db.HashDelete(cacheKey, hashFields: list_field.ToArray<RedisValue>(), flags: CommandFlags.FireAndForget);//删除项
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

            #endregion Remove

            #region Get

            /// <summary>
            /// 获取指定缓存键的Hash对象
            /// </summary>
            /// <param name="key">缓存键</param>
            /// <param name="isFullKey">是否已经是完整的Key，如果不是，则会调用一次GetFinalKey()方法</param>
            /// <param name="db_num">db_num为-1时，使用UnifiedPayKey.Redis_DB_Num</param>
            /// <returns></returns>
            public static Hashtable Get(string key, bool isFullKey = false, int db_num = -1)
            {
                if (key.IsNullOrWhiteSpace())
                    throw new ArgumentException("key不能为空");

                if (!KeyExists(key, isFullKey, db_num))
                    return null;

                var hash = default(Hashtable);
                var cacheKey = GetFinalKey(key, isFullKey);
                using (BeginCacheLock(LockResourcePrefix, cacheKey))
                {
                    try
                    {
                        var db = GetDatabase(db_num);
                        var _RedisType = db.KeyType(cacheKey);
                        if (_RedisType == RedisType.Hash)
                        {
                            var _hash = db.HashGetAll(cacheKey);
                            if (_hash != null)
                            {
                                hash = new Hashtable();
                                foreach (var hashEntry in _hash)
                                {
                                    if (hashEntry.Name.IsNullOrWhiteSpace())
                                        continue;

                                    hash.Add(hashEntry.Name.ToString(), hashEntry.Value.ValueOrEmpty());
                                }
                            }
                        }
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

                return hash;
            }

            /// <summary>
            /// 取缓存 为null返回default(T)
            /// </summary>
            /// <typeparam name="T">对象类型</typeparam>
            /// <param name="key">缓存键</param>
            /// <returns></returns>
            public static T Get<T>(string key, bool isFullKey = false, int db_num = -1)
                where T : class, new()
            {
                if (key.IsNullOrWhiteSpace())
                    throw new ArgumentException("key不能为空");

                var hash = Get(key, isFullKey, db_num);
                if (null == hash)
                    return default(T);

                var obj = default(T);
                try
                {
                    if (!hash["_type"].IsNullOrWhiteSpace() && !hash["_data"].IsNullOrWhiteSpace())
                        obj = JSON.Deserialize<T>(hash["_data"].ToString());
                    else
                        obj = JSON.ConvertToType<T>(hash);
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
            /// 获取Hash对象指定项的值
            /// </summary>
            /// <param name="key">缓存键</param>
            /// <param name="field">项名</param>
            /// <param name="isFullKey">是否已经是完整的Key，如果不是，则会调用一次GetFinalKey()方法</param>
            /// <param name="db_num">db_num为-1时，使用UnifiedPayKey.Redis_DB_Num</param>
            /// <returns></returns>
            public static string GetFieldValue(string key, string field, bool isFullKey = false, int db_num = -1)
            {
                if (key.IsNullOrWhiteSpace() || field.IsNullOrWhiteSpace())
                    throw new ArgumentException("key和field不能为空");

                if (!KeyExists(key, isFullKey, db_num))
                    return null;

                string val = null;
                var cacheKey = GetFinalKey(key, isFullKey);
                using (BeginCacheLock(LockResourcePrefix, cacheKey))
                {
                    try
                    {
                        var db = GetDatabase(db_num);
                        var _RedisType = db.KeyType(cacheKey);
                        if (_RedisType == RedisType.Hash)
                            val = db.HashGet(cacheKey, field).ToString();
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

                return val;
            }

            /// <summary>
            /// 获取Hash对象全部项的值
            /// </summary>
            /// <param name="key">缓存键</param>
            /// <param name="isFullKey">是否已经是完整的Key，如果不是，则会调用一次GetFinalKey()方法</param>
            /// <param name="db_num">db_num为-1时，使用UnifiedPayKey.Redis_DB_Num</param>
            /// <returns></returns>
            public static string[] GetFieldValues(string key, bool isFullKey = false, int db_num = -1)
            {
                if (key.IsNullOrWhiteSpace())
                    throw new ArgumentException("key不能为空");

                if (!KeyExists(key, isFullKey, db_num))
                    return null;

                List<string> list_value = null;
                var cacheKey = GetFinalKey(key, isFullKey);
                using (BeginCacheLock(LockResourcePrefix, cacheKey))
                {
                    try
                    {
                        var db = GetDatabase(db_num);
                        var _RedisType = db.KeyType(cacheKey);
                        if (_RedisType == RedisType.Hash)
                        {
                            var arr_value = db.HashValues(cacheKey)?.ToArray();
                            if (arr_value != null)
                            {
                                list_value = new List<string>();

                                foreach (var val in arr_value)
                                    list_value.Add(val.ValueOrEmpty());
                            }
                        }
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

                return list_value?.ToArray();
            }

            /// <summary>
            /// 获取Hash对象全部项名
            /// </summary>
            /// <param name="key">缓存键</param>
            /// <param name="isFullKey">是否已经是完整的Key，如果不是，则会调用一次GetFinalKey()方法</param>
            /// <param name="db_num">db_num为-1时，使用UnifiedPayKey.Redis_DB_Num</param>
            /// <returns></returns>
            public static string[] GetFieldNames(string key, bool isFullKey = false, int db_num = -1)
            {
                if (key.IsNullOrWhiteSpace())
                    throw new ArgumentException("key不能为空");

                if (!KeyExists(key, isFullKey, db_num))
                    return null;

                var list_field = default(List<string>);
                var cacheKey = GetFinalKey(key, isFullKey);
                using (BeginCacheLock(LockResourcePrefix, cacheKey))
                {
                    try
                    {
                        var db = GetDatabase(db_num);
                        var _RedisType = db.KeyType(cacheKey);
                        if (_RedisType == RedisType.Hash)
                        {
                            var arr_field = db.HashKeys(cacheKey)?.ToArray();
                            if (arr_field != null)
                            {
                                list_field = new List<string>();

                                foreach (var field in arr_field)
                                    list_field.Add(field.ToString());
                            }
                        }
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

                return list_field?.ToArray();
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
                        var db = GetDatabase(db_num);

                        #region 方法1

                        //var hashKeys = GetServer().Keys(database: UnifiedPayKey.Redis_DB_Num, pattern: cacheKey, pageSize: int.MaxValue);

                        //foreach (var _hashKey in hashKeys)
                        //{
                        //    var _hash = db.HashGetAll(_hashKey);
                        //    var hash_sub = new Hashtable();

                        //    foreach (var hashEntry in _hash)
                        //    {
                        //        if (hashEntry.Name.IsNullOrWhiteSpace())
                        //            continue;

                        //        hash_sub.Add(hashEntry.Name.ToString(), hashEntry.Value.ValueOrEmpty());
                        //    }

                        //    hashs.Add(_hashKey, hash_sub);
                        //}

                        #endregion 方法1

                        #region 方法2

                        var script = new StringBuilder();
                        script.Append(" local ks = redis.call('KEYS', @keypattern) ");                  // local ks为定义一个局部变量，其中用于存储获取到的keys
                        script.Append(" return ks ");

                        //Redis的keys模糊查询
                        var redisResult = db.ScriptEvaluate(
                            script: LuaScript.Prepare(script.ToString()),
                            parameters: new { keypattern = cacheKey });

                        if (!redisResult.IsNull)
                        {
                            hashs = new Hashtable();
                            string[] hashKeys = (string[])redisResult;
                            foreach (var _hashKey in hashKeys)
                            {
                                var _RedisType = db.KeyType(_hashKey);
                                if (_RedisType == RedisType.Hash)
                                {
                                    var _hash = db.HashGetAll(_hashKey);
                                    var hash_sub = new Hashtable();

                                    foreach (var hashEntry in _hash)
                                    {
                                        if (hashEntry.Name.IsNullOrWhiteSpace())
                                            continue;

                                        hash_sub.Add(hashEntry.Name.ToString(), hashEntry.Value.IsNullOrWhiteSpace() ? string.Empty : hashEntry.Value.ToString());
                                    }

                                    hashs.Add(_hashKey, hash_sub);
                                }
                            }
                        }

                        #endregion 方法2
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
