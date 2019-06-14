/************************************************************************
 * 文件标识：  97998bd8-6bc9-4521-8686-101496697677
 * 项目名称：  CySoft.Redis  
 * 项目描述：  
 * 类 名 称：  RedisUtils
 * 版 本 号：  v1.0.0.0 
 * 说    明：  
 * 作    者：  尹自强
 * 创建时间：  2018/6/8 13:38:47
 * 更新时间：  2018/6/8 13:38:47
************************************************************************
 * Copyright @ 尹自强 2018. All rights reserved.
************************************************************************/

#define xxxxxxxxxxxxxxxxxxxxxxxxxxRedisError

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CySoft.Utility;
using CySoft.Utility.CacheUtils;
using CySoft.Utility.Extension;
using CySoft.Utility.SignUtils;
using StackExchange.Redis;

namespace CySoft.Redis
{
    public sealed partial class RedisUtils
    {
        private const string LockResourcePrefix = "LOCK";
        private static ConnectionMultiplexer _client;
        private const int RuntimeLog_flag_type_Info = 2;
        private const int RuntimeLog_flag_type_Error = 4;
        private const int RuntimeLog_flag_type_Redis = 88;

        ~RedisUtils()
        {
            LogHelper.RuntimeLog(new {
                ReqId = string.Empty,
                flag_type = RuntimeLog_flag_type_Redis,
                target = string.Format("/{0}/{1}", System.Reflection.MethodBase.GetCurrentMethod().ReflectedType.FullName, new System.Diagnostics.StackTrace().GetFrame(0).GetMethod().Name),
                LogContent = $"~RedisUtils"
            });

            //if (_client != null)
            //{
            //    _client.Dispose();  //释放
            //}
        }

        /// <summary>
        /// 静态构造
        /// 在当前类首次执行时
        /// 触发RedisManager.Manager创建static readonly ConnectionMultiplexer实例
        /// 可省略单例创建ConnectionMultiplexer实例时的程序锁
        /// </summary>
        static RedisUtils()
        {
            _client = RedisManager.Manager;
            // 此处使用配置默认DB_Num进行检查Redis是否正常
            var database = _client.GetDatabase(SystemKey.Redis_DB_Num);

            #region 通过读写操作检测Redis是否可用

            var testKey = $"TEST-{Guid.NewGuid().ToString("N")}";
            var testValue = $"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fffff")}";

            database.StringSet(testKey, testValue, expiry: TimeSpan.FromSeconds(300), flags: CommandFlags.None);       // 5分钟失效
            var storeValue = database.StringGet(testKey);

            if (!testValue.Equals(storeValue, StringComparison.OrdinalIgnoreCase))
                throw new Exception("RedisUtils失效，没有计入缓存！");

            database.KeyDelete(testKey, CommandFlags.FireAndForget);

            #endregion 通过读写操作检测Redis是否可用
        }

        #region 私有方法

        /// <summary>
        /// 获取 Server 对象
        /// </summary>
        /// <returns></returns>
        private static IServer GetServer()
        {
            IServer server = null;

            try
            {
                //https://github.com/StackExchange/StackExchange.Redis/blob/master/Docs/KeysScan.md
                server = _client?.GetServer(_client?.GetEndPoints()?[0]);

#if DEBUG && RedisError

                server = null;
                throw new Exception("模拟Redis连接错误，手动抛出异常");

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

            return server;
        }

        /// <summary>
        /// 获取 DataBase 对象
        /// </summary>
        /// <param name="db_num">db_num为-1时，使用UnifiedPayKey.Redis_DB_Num</param>
        /// <returns></returns>
        private static IDatabase GetDatabase(int db_num)
        {
            IDatabase db = null;
            if (db_num < 0)
                db_num = SystemKey.Redis_DB_Num;

            try
            {
                //db = _client?.GetDatabase(UnifiedPayKey.Redis_DB_Num);
                db = _client?.GetDatabase(db_num);

#if DEBUG && RedisError

                db = null;
                throw new Exception("模拟Redis连接错误，手动抛出异常");

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

            return db;
        }

        private static ICacheLock BeginCacheLock(string resourceName, string key, int retryCount = 0, TimeSpan retryDelay = new TimeSpan())
        {
            return new RedisCacheLock(resourceName, key, retryCount: retryCount, retryDelay: retryDelay);
        }

        /// <summary>
        /// 获取拼装后的FinalKey
        /// {UnifiedPayKey.Redis_KeyPrefix}:{key}
        /// </summary>
        /// <param name="key">键</param>
        /// <param name="isFullKey">是否已经是经过拼接的FullKey</param>
        /// <returns></returns>
        private static string GetFinalKey(string key, bool isFullKey = false)
        {
            var finalFullKey = isFullKey
                ? key
                : string.Format("{0}-{1}", SystemKey.Redis_KeyPrefix, key);

            return finalFullKey;
        }

        /// <summary>
        /// 获取拼装后的FinalKey
        /// {UnifiedPayKey.Redis_KeyPrefix}:{key}
        /// </summary>
        /// <param name="arr_key">键数组</param>
        /// <param name="isFullKey">是否已经是经过拼接的FullKey</param>
        /// <returns></returns>
        private static string[] GetFinalKey(string[] arr_key, bool isFullKey = false)
        {
            var list_finalFullKey = new List<string>();
            foreach (var key in arr_key)
            {
                var finalFullKey = isFullKey
                ? key
                : string.Format("{0}-{1}", SystemKey.Redis_KeyPrefix, key);

                list_finalFullKey.Add(finalFullKey);
            }

            return list_finalFullKey.ToArray();
        }

        #endregion 私有方法


        #region Info

        public static Hashtable Info()
        {
            var hash = new Hashtable();
            using (BeginCacheLock(LockResourcePrefix, "SYSTEM"))
            {
                try
                {
                    var list_info = GetServer()
                        .Info("ALL")
                        ?.ToList();

                    list_info?.ForEach(item => {
                        var dic = item.ToDictionary(k => InfoFormatter(k.Key), v => v.Value);
                        var sdic = new SortedDictionary<string, string>(dic);
                        hash.Add(InfoFormatter(item.Key), sdic);
                    });
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

        private static readonly Hashtable hash_Key_Formatter = new Hashtable(StringComparer.Create(System.Globalization.CultureInfo.CurrentCulture, true)) {
            { "Stats", "【一般信息】" },
                { "active_defrag_hits", "【主动碎片整理命中次数】" },
                { "active_defrag_key_hits", "【主动碎片整理key命中次数】" },
                { "active_defrag_key_misses", "【主动碎片整理key未命中次数】" },
                { "active_defrag_misses", "【主动碎片整理未命中次数】" },
                { "evicted_keys", "【运行以来剔除(超过了maxmemory后)的key的数量】" },
                { "expired_keys", "【运行以来过期的key的数量】" },
                { "expired_stale_perc", "【过期的比率】" },
                { "expired_time_cap_reached_count", "【过期计数】" },
                { "instantaneous_input_kbps", "【redis网络入口kps】" },
                { "instantaneous_ops_per_sec", "【redis当前的qps，redis内部较实时的每秒执行的命令数】" },
                { "instantaneous_output_kbps", "【redis网络出口kps】" },
                { "keyspace_hits", "【命中次数】" },
                { "keyspace_misses", "【没命中次数】" },
                { "latest_fork_usec", "【最近一次fork操作阻塞redis进程的耗时数，单位微秒】" },
                { "migrate_cached_sockets", "【是否已经缓存了到该地址的连接】" },
                { "pubsub_channels", "【当前使用中的频道数量】" },
                { "pubsub_patterns", "【当前使用的模式的数量】" },
                { "rejected_connections", "【拒绝的连接个数，redis连接个数达到maxclients限制，拒绝新连接的个数】" },
                { "slave_expires_tracked_keys", "【从实例到期key数量】" },
                { "sync_full", "【主从完全同步成功次数】" },
                { "sync_partial_err", "【主从部分同步失败次数】" },
                { "sync_partial_ok", "【主从部分同步成功次数】" },
                { "total_commands_processed", "【redis处理的命令数】" },
                { "total_connections_received", "【新创建连接个数；如果新创建连接过多，过度地创建和销毁连接对性能有影响，说明短连接严重或连接池使用有问题，需调研代码的连接设置】" },
                { "total_net_input_bytes", "【redis网络入口流量字节数】" },
                { "total_net_output_bytes", "【redis网络出口流量字节数】" },
            //------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            { "CPU", "【CPU计算量信息】" },
                { "used_cpu_sys", "【将所有redis主进程在核心态所占用的CPU时求和累计起来】" },
                { "used_cpu_sys_children", "【将后台进程在核心态所占用的CPU时求和累计起来】" },
                { "used_cpu_user", "【将所有redis主进程在用户态所占用的CPU时求和累计起来】" },
                { "used_cpu_user_children", "【将后台进程在用户态所占用的CPU时求和累计起来】" },
            //------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            { "Memory", "【内存信息】" },
                { "active_defrag_running", "【表示没有活动的defrag任务正在运行，1：表示有活动的defrag任务正在运行(defrag：表示内存碎片整理)】" },
                { "lazyfree_pending_objects", "【0：表示不存在延迟释放的挂起对象】" },
                { "maxmemory", "【Redis实例的最大内存配置】" },
                { "maxmemory_human", "【显示Redis实例的最大内存配置】" },
                { "maxmemory_policy", "【当达到maxmemory时的淘汰策略】" },
                { "mem_allocator", "【内存分配器】" },
                { "mem_fragmentation_ratio", "【used_memory_rss和used_memory之间的比率(内存碎片的比率)；当mem_fragmentation_ratio小于1时，说明used_memory大于used_memory_rss，这时Redis已经在使用SWAP，运行性能会受很大影响】" },
                { "total_system_memory", "【整个系统内存】" },
                { "total_system_memory_human", "【显示整个系统内存】" },
                { "used_memory", "【由Redis分配器分配的内存总量，单位：字节byte】" },
                { "used_memory_human", "【显示由Redis分配器分配的内存总量】" },
                { "used_memory_lua", "【Lua脚本存储占用的内存】" },
                { "used_memory_lua_human", "【显示Lua脚本存储占用的内存】" },
                { "used_memory_peak", "【Redis的内存消耗峰值，单位：字节byte】" },
                { "used_memory_peak_human", "【显示Redis的内存消耗峰值】" },
                { "used_memory_peak_perc", "【使用内存达到峰值内存的百分比】" },
                { "used_memory_rss", "【从操作系统的角度，返回Redis已分配的内存总量(俗称常驻集大小)。这个值和top、ps等命令的输出一致】" },
                { "used_memory_rss_human", "【显示从操作系统的角度，返回Redis已分配的内存总量(俗称常驻集大小)。这个值和top、ps等命令的输出一致】" },

                { "used_memory_dataset", "【数据占用的内存大小，即used_memory减used_memory_overhead】" },
                { "used_memory_dataset_perc", "【数据占用的内存大小的百分比，100% * (used_memory_dataset/(used_memory-used_memory_startup))】" },
                { "used_memory_startup", "【Redis服务器启动时消耗的内存】" },
                { "used_memory_overhead", "【Redis为了维护数据集的内部机制所需的内存开销，包括所有客户端输出缓冲区、查询缓冲区、AOF重写缓冲区和主从复制的backlog】" },
            //------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            { "Clients", "【已连接客户端信息】" },
                { "blocked_clients", "【正在等待阻塞命令(BLPOP、BRPOP、BRPOPLPUSH)的客户端的数量】" },
                { "client_biggest_input_buf", "【当前连接的客户端当中，最大输入缓存】" },
                { "client_longest_output_list", "【当前连接的客户端当中，最长的输出列表】" },
                { "connected_clients", "【已连接客户端的数量(不包括通过从属服务器连接的客户端)】" },
            //------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            { "Commandstats", "【命令使用信息：calls-调用次数、usec-总耗费CPU时间、usec_per_call-每个命令平均耗费CPU】" },
                { "cmdstat_auth", "【auth命令】" },
                { "cmdstat_client", "【client命令】" },
                { "cmdstat_cluster", "【cluster命令】" },
                { "cmdstat_config", "【config命令】" },
                { "cmdstat_del", "【del命令】" },
                { "cmdstat_echo", "【echo命令】" },
                { "cmdstat_eval", "【eval命令】" },
                { "cmdstat_evalsha", "【evalsha命令】" },
                { "cmdstat_exists", "【exists命令】" },
                { "cmdstat_expire", "【expire命令】" },
                { "cmdstat_flushdb", "【flushdb命令】" },
                { "cmdstat_get", "【get命令】" },
                { "cmdstat_hexists", "【hexists命令】" },
                { "cmdstat_hget", "【hget命令】" },
                { "cmdstat_hgetall", "【hgetall命令】" },
                { "cmdstat_hlen", "【hlen命令】" },
                { "cmdstat_hmset", "【hmset命令】" },
                { "cmdstat_hscan", "【hscan命令】" },
                { "cmdstat_hset", "【hset命令】" },
                { "cmdstat_info", "【info命令】" },
                { "cmdstat_keys", "【keys命令】" },
                { "cmdstat_ping", "【ping命令】" },
                { "cmdstat_psetex", "【psetex命令】" },
                { "cmdstat_scan", "【scan命令】" },
                { "cmdstat_script", "【script命令】" },
                { "cmdstat_select", "【select命令】" },
                { "cmdstat_set", "【set命令】" },
                { "cmdstat_setex", "【setex命令】" },
                { "cmdstat_slowlog", "【slowlog命令】" },
                { "cmdstat_subscribe", "【subscribe命令】" },
                { "cmdstat_ttl", "【ttl命令】" },
                { "cmdstat_type", "【type命令】" },
                { "cmdstat_unsubscribe", "【unsubscribe命令】" },
            //------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            { "Keyspace", "【数据库相关信息：keys-键数量、expires-有生存期的键数量、avg_ttl-平均存活时间】" },
            //------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            { "Server", "【Redis服务器信息】" },
                { "arch_bits", "【架构(32或64位)】" },
                { "atomicvar_api", "【原子处理api】" },
                { "config_file", "【配置文件路径】" },
                { "executable", "【执行文件】" },
                { "gcc_version", "【编译Redis时所使用的GCC版本】" },
                { "hz", "【Redis内部调度(进行关闭timeout的客户端，删除过期key等等)频率，程序规定serverCron每秒运行10次】" },
                { "lru_clock", "【自增的时钟，用于LRU管理,该时钟100ms(hz=10,因此每1000ms/10=100ms执行一次定时任务)更新一次】" },
                { "multiplexing_api", "【Redis所使用的事件处理机制】" },
                { "os", "【服务器的宿主操作系统】" },
                { "process_id", "【服务器进程的PID】" },
                { "redis_build_id", "【Git dirty flag】" },
                { "redis_git_dirty", "【Git dirty flag】" },
                { "redis_git_sha1", "【Git SHA1】" },
                { "redis_mode", "【运行模式，单机或者集群】" },
                { "redis_version", "【Redis服务器版本】" },
                { "run_id", "【Redis服务器的随机标识符(用于Sentinel和集群)】" },
                { "tcp_port", "【TCP/IP监听端口】" },
                { "uptime_in_days", "【自Redis服务器启动以来，经过的天数】" },
                { "uptime_in_seconds", "【自Redis服务器启动以来，经过的秒数】" },
            //------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            { "Persistence", "【RDB和AOF信息】" },
                { "aof_current_rewrite_time_sec", "【如果rewrite操作正在进行，则记录所使用的时间，单位秒】" },
                { "aof_enabled", "【是否开启了AOF】" },
                { "aof_last_bgrewrite_status", "【上次bgrewriteaof操作的状态】" },
                { "aof_last_cow_size", "【AOF过程中父进程与子进程相比执行了多少修改(包括读缓冲区，写缓冲区，数据修改等)】" },
                { "aof_last_rewrite_time_sec", "【最近一次AOF rewrite耗费的时长】" },
                { "aof_last_write_status", "【上次AOF写入状态】" },
                { "aof_rewrite_in_progress", "【标识AOF的rewrite操作是否在进行中】" },
                { "aof_rewrite_scheduled", "【rewrite任务计划，当客户端发送bgrewriteaof指令，如果当前rewrite子进程正在执行，那么将客户端请求的bgrewriteaof变为计划任务，待AOF子进程结束后执行rewrite】" },
                { "loading", "【服务器是否正在载入持久化文件】" },
                { "rdb_bgsave_in_progress", "【服务器是否正在创建RDB文件】" },
                { "rdb_changes_since_last_save", "【离最近一次成功生成RDB文件，写入命令的个数，即有多少个写入命令没有持久化】" },
                { "rdb_current_bgsave_time_sec", "【如果服务器正在创建RDB文件，那么这个域记录的就是当前的创建操作已经耗费的秒数】" },
                { "rdb_last_bgsave_status", "【最近一次RDB持久化是否成功】" },
                { "rdb_last_bgsave_time_sec", "【最近一次成功生成RDB文件耗时秒数】" },
                { "rdb_last_cow_size", "【RDB过程中父进程与子进程相比执行了多少修改(包括读缓冲区，写缓冲区，数据修改等)】" },
                { "rdb_last_save_time", "【离最近一次成功创建RDB文件的时间戳。当前时间戳减rdb_last_save_time等于多少秒未成功生成RDB文件】" },
            //------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            { "Cluster", "【Redis集群信息】" },
                { "cluster_enabled", "【实例是否启用集群模式】" },
            //------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            { "Replication", "【主从复制信息】" },
                { "connected_slaves", "【连接的slave实例个数】" },
                { "master_repl_offset", "【主从同步偏移量,此值如果和上面的offset相同说明主从一致没延迟，与master_replid可被用来标识主实例复制流中的位置】" },
                { "master_replid", "【主实例启动随机字符串】" },
                { "master_replid2", "【主实例启动随机字符串2】" },
                { "repl_backlog_active", "【复制积压缓冲区是否开启】" },
                { "repl_backlog_first_byte_offset", "【复制缓冲区里偏移量的大小】" },
                { "repl_backlog_histlen", "【此值等于master_repl_offset减repl_backlog_first_byte_offset，该值不会超过repl_backlog_size的大小】" },
                { "repl_backlog_size", "【复制积压缓冲大小】" },
                { "role", "【实例的角色，是master或slave】" },
                { "second_repl_offset", "【主从同步偏移量2,此值如果和master_repl_offset相同说明主从一致没延迟】" },
            //------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        };

        private static string InfoFormatter(string text)
        {
            var description = hash_Key_Formatter[text].ValueOrEmpty();
            return "{0}{1}".ToFormat(text, description);
        }

        #endregion Info

        #region ClientList

        public static dynamic ClientList()
        {
            var result = default(object);

            using (BeginCacheLock(LockResourcePrefix, "SYSTEM"))
            {
                try
                {
                    var _list_client = GetServer()
                        .ClientList()
                        ?.ToList();

                    var g_Server = _list_client
                        .GroupBy(item => item.Name);

                    var list_server_client = g_Server.Select(server => new {
                        ServerName = server.Key,
                        Total_Sub = _list_client.LongCount(item => item.Name.Equals(server.Key, StringComparison.Ordinal)),
                        ClientList = _list_client
                            .Where(item => item.Name.Equals(server.Key, StringComparison.Ordinal))
                            .OrderByDescending(item => item.AgeSeconds)
                            .ThenBy(item => item.Id)
                            .ThenBy(item => item.Database)
                            .Select(item => new {
                                item.Id,
                                item.AgeSeconds,
                                item.Database,
                                Address = item.Address.ToString(),
                                ClientFlags = item.Flags.ToString(),
                                ClientType = item.ClientType.ToString(),
                                item.Raw
                            })
                    }).ToList<dynamic>();

                    result = new {
                        Total_Server = g_Server.LongCount(),
                        Total_Client = _list_client.LongCount(),
                        Server_Client = list_server_client
                    };
                }
                catch (Exception ex)
                {
                    LogHelper.RuntimeLog(new {
                        ReqId = string.Empty,
                        flag_type = 4,
                        target = string.Format("/{0}/{1}", System.Reflection.MethodBase.GetCurrentMethod().ReflectedType.FullName, new System.Diagnostics.StackTrace().GetFrame(0).GetMethod().Name),
                        LogContent = $"【异常】：【{JSON.Serialize(ex)}】"
                    });

                    result = ex;
                }
            }

            return result;
        }

        #endregion ClientList




        #region Keys

        public static string[] Keys(string key = "*", bool isPattern = false, bool isFullKey = false, int db_num = -1)
        {
            if (key.IsNullOrWhiteSpace())
                throw new ArgumentException("key不能为空");

            var cacheKey = GetFinalKey(key, isFullKey);

            var list_key = new List<string>();
            using (BeginCacheLock(LockResourcePrefix, cacheKey))
            {
                try
                {
                    var em = GetServer()
                        .Keys(database: db_num, pattern: cacheKey)
                        .GetEnumerator();

                    while (em.MoveNext())
                        list_key.Add(em.Current.ValueOrEmpty());
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

            return list_key.ToArray();
        }

        #endregion Keys

        #region KeyExists

        /// <summary>
        /// 检查是否存在Key及对象
        /// </summary>
        /// <param name="key">缓存键</param>
        /// <param name="isFullKey">是否已经是完整的Key，如果不是，则会调用一次GetFinalKey()方法</param>
        /// <param name="db_num">db_num为-1时，使用UnifiedPayKey.Redis_DB_Num</param>
        /// <returns></returns>
        public static bool KeyExists(string key, bool isFullKey = false, int db_num = -1)
        {
            if (key.IsNullOrWhiteSpace())
                throw new ArgumentException("key不能为空");

            bool exists = false;
            var cacheKey = GetFinalKey(key, isFullKey);

            using (BeginCacheLock(LockResourcePrefix, cacheKey))
            {
                try
                {
                    exists = GetDatabase(db_num)
                        .KeyExists(cacheKey);
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

        #endregion KeyExists

        #region KeyType

        public static string KeyType(string key, bool isFullKey = false, int db_num = -1)
        {
            if (key.IsNullOrWhiteSpace())
                throw new ArgumentException("key不能为空");

            var _RedisType = RedisType.None;
            var cacheKey = GetFinalKey(key, isFullKey);

            using (BeginCacheLock(LockResourcePrefix, cacheKey))
            {
                try
                {
                    _RedisType = GetDatabase(db_num)
                        .KeyType(cacheKey);
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

            return _RedisType.ToString();
        }

        #endregion KeyType

        #region TTL

        public static int TTL(string key, bool isFullKey = false, int db_num = -1)
        {
            if (key.IsNullOrWhiteSpace())
                throw new ArgumentException("key不能为空");

            var expire = -1;
            var cacheKey = GetFinalKey(key, isFullKey);

            using (BeginCacheLock(LockResourcePrefix, cacheKey))
            {
                try
                {
                    var ts = GetDatabase(db_num)
                         .KeyTimeToLive(cacheKey);

                    if (ts is TimeSpan)
                        expire = Convert.ToInt32(ts.Value.TotalSeconds);
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

            return expire;
        }

        #endregion TTL

        #region Expire

        public static void Expire(string key, int expire = -1, bool isFullKey = false, int db_num = -1)
        {
            if (key.IsNullOrWhiteSpace())
                throw new ArgumentException("key不能为空");

            var cacheKey = GetFinalKey(key, isFullKey);
            using (BeginCacheLock(LockResourcePrefix, cacheKey))
            {
                try
                {
                    var db = GetDatabase(db_num);

                    db.KeyPersist(cacheKey, flags: CommandFlags.FireAndForget);
                    if (expire > 0)
                        db.KeyExpire(cacheKey, DateTime.Now.AddSeconds(expire), flags: CommandFlags.FireAndForget);
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

        #endregion Expire

        #region Remove

        /// <summary>
        /// 移除对象
        /// </summary>
        /// <param name="key">缓存键（星号*为匹配符）</param>
        /// <param name="isPattern">是否模糊删除 true：模糊</param>
        /// <param name="isFullKey">是否已经是完整的Key，如果不是，则会调用一次GetFinalKey()方法</param>
        /// <param name="db_num">db_num为-1时，使用UnifiedPayKey.Redis_DB_Num</param>
        public static void Remove(string key, bool isPattern = false, bool isFullKey = false, int db_num = -1)
        {
            if (key.IsNullOrWhiteSpace())
                throw new ArgumentException("key不能为空");

            var cacheKey = GetFinalKey(key, isFullKey);
            using (BeginCacheLock(LockResourcePrefix, cacheKey))
            {
                var db = GetDatabase(db_num);
                if (isPattern)
                {
                    try
                    {
                        #region 方法1 Keys回，KeyDelete去

                        //var arr_key = GetServer().Keys(database: UnifiedPayKey.Redis_DB_Num, pattern: cacheKey, pageSize: int.MaxValue);
                        //db.KeyDelete(arr_key?.ToArray());

                        #endregion 方法1 Keys回，KeyDelete去

                        #region 方法2 Lua KeyPattern去

                        var script = new StringBuilder();
                        script.Append(" local ks = redis.call('KEYS', @keypattern) ");                  // local ks为定义一个局部变量，其中用于存储获取到的keys
                        script.Append(" for i=1,#ks,5000 do ");                                         // #ks为ks集合的个数, 语句的意思： for(int i = 1; i <= ks.Count; i+=5000)
                        script.Append(" redis.call('del', unpack(ks, i, math.min(i+4999, #ks))) ");     // Lua集合索引值从1为起始，unpack为解包，获取ks集合中的数据，每次5000，然后执行删除
                        script.Append(" end ");
                        script.Append(" return true ");

                        //Redis的keys模糊查询
                        db.ScriptEvaluateAsync(
                            script: LuaScript.Prepare(script.ToString())
                            , parameters: new { keypattern = cacheKey }
                            , flags: CommandFlags.FireAndForget);

                        #endregion 方法2 Lua KeyPattern去
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
                else
                {
                    try
                    {
                        db.KeyDelete(cacheKey, flags: CommandFlags.FireAndForget);//删除键
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
        }

        /// <summary>
        /// 移除对象
        /// </summary>
        /// <param name="arr_key">缓存键数组（星号*为匹配符）</param>
        /// <param name="isPattern">是否模糊删除 true：模糊</param>
        /// <param name="isFullKey">是否已经是完整的Key，如果不是，则会调用一次GetFinalKey()方法</param>
        /// <param name="db_num">db_num为-1时，使用UnifiedPayKey.Redis_DB_Num</param>
        public static void Remove(string[] arr_key, bool isPattern = false, bool isFullKey = false, int db_num = -1)
        {
            if (arr_key == null || arr_key.Length == 0)
                throw new ArgumentException("key不能为空");

            var arr_cacheKey = GetFinalKey(arr_key, isFullKey);
            var CheckSum_Keys = MD5.Sign(arr_cacheKey.Join(","), string.Empty);

            using (BeginCacheLock(LockResourcePrefix, CheckSum_Keys))
            {
                var db = GetDatabase(db_num);
                if (isPattern)
                {
                    foreach (var cacheKey in arr_cacheKey)
                    {
                        try
                        {
                            #region 方法1 Keys回，KeyDelete去

                            //var arr_key = GetServer().Keys(database: UnifiedPayKey.Redis_DB_Num, pattern: cacheKey, pageSize: int.MaxValue);
                            //db.KeyDelete(arr_key?.ToArray());

                            #endregion 方法1 Keys回，KeyDelete去

                            #region 方法2 Lua KeyPattern去

                            var script = new StringBuilder();
                            script.Append(" local ks = redis.call('KEYS', @keypattern) ");                  // local ks为定义一个局部变量，其中用于存储获取到的keys
                            script.Append(" for i=1,#ks,5000 do ");                                         // #ks为ks集合的个数, 语句的意思： for(int i = 1; i <= ks.Count; i+=5000)
                            script.Append(" redis.call('del', unpack(ks, i, math.min(i+4999, #ks))) ");     // Lua集合索引值从1为起始，unpack为解包，获取ks集合中的数据，每次5000，然后执行删除
                            script.Append(" end ");
                            script.Append(" return true ");

                            //Redis的keys模糊查询
                            db.ScriptEvaluateAsync(
                                script: LuaScript.Prepare(script.ToString())
                                , parameters: new { keypattern = cacheKey }
                                , flags: CommandFlags.FireAndForget);

                            #endregion 方法2 Lua KeyPattern去
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
                else
                {
                    try
                    {
                        var list_keyRedis = new List<RedisKey>();
                        foreach (var cacheKey in arr_cacheKey)
                            list_keyRedis.Add(cacheKey);

                        db.KeyDelete(list_keyRedis.ToArray(), flags: CommandFlags.FireAndForget);//删除键
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
        }

        #endregion Remove

        #region GetCount

        /// <summary>
        /// 获取缓存集合总数
        /// </summary>
        /// <param name="key">缓存键 默认：*</param>
        /// <param name="isFullKey">是否已经是完整的Key，如果不是，则会调用一次GetFinalKey()方法</param>
        /// <param name="db_num">db_num为-1时，使用UnifiedPayKey.Redis_DB_Num</param>
        /// <returns></returns>
        public static long GetCount(string key = "*", bool isFullKey = false, int db_num = -1)
        {
            var cacheKey = GetFinalKey(key, isFullKey);
            var count = 0L;

            if (db_num < 0)
                db_num = SystemKey.Redis_DB_Num;

            using (BeginCacheLock(LockResourcePrefix, cacheKey))
            {
                try
                {
                    count = GetServer().Keys(database: db_num, pattern: cacheKey).LongCount();
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

            return count;
        }

        #endregion GetCount

        #region FlushDatabase

        /// <summary>
        /// 清空当前DataBase【谨慎操作】
        /// <param name="db_num">db_num为-1时，使用UnifiedPayKey.Redis_DB_Num</param>
        /// </summary>
        public static void FlushDatabase(int db_num = -1)
        {
            if (db_num < 0)
                db_num = SystemKey.Redis_DB_Num;

            using (BeginCacheLock(LockResourcePrefix, "FlushDatabase"))
            {
                try
                {
                    GetServer().FlushDatabase(database: db_num, flags: CommandFlags.FireAndForget);
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

        #endregion FlushDatabase

        #region ScriptEvaluate

        /// <summary>
        /// 执行Lua脚本
        /// </summary>
        /// <param name="script">Lua脚本</param>
        /// <param name="param">参数对象</param>
        /// <param name="db_num">db_num为-1时，使用UnifiedPayKey.Redis_DB_Num</param>
        /// <returns></returns>
        public static dynamic ScriptEvaluate(string script, dynamic parameters, int db_num = -1)
        {
            if (script.IsNullOrWhiteSpace())
                return null;

            RedisResult redisResult = null;

            using (BeginCacheLock(LockResourcePrefix, "ScriptEvaluate"))
            {
                try
                {
                    redisResult = GetDatabase(db_num)
                        .ScriptEvaluate(script: LuaScript.Prepare(script), parameters: parameters);

                    if (redisResult.IsNull)
                        redisResult = null;
                }
                catch (Exception ex)
                {
                    LogHelper.RuntimeLog(new {
                        ReqId = string.Empty,
                        flag_type = 4,
                        target = string.Format("/{0}/{1}", System.Reflection.MethodBase.GetCurrentMethod().ReflectedType.FullName, new System.Diagnostics.StackTrace().GetFrame(0).GetMethod().Name),
                        LogContent = $"【异常】：【{JSON.Serialize(ex)}】"
                    });

                    redisResult = null;
                }
            }

            return redisResult;
        }

        #endregion ScriptEvaluate


        public static Hashtable Convert_RedisHashContainer(dynamic obj)
        {
            if (obj == null)
                throw new ArgumentException("参数不能为null", "obj");

            Hashtable hash = new Hashtable();
            hash.Add("_data", JSON.Serialize(obj));
            hash.Add("_type", obj.GetType().FullName);
            hash.Add("_ts", string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now));

            return hash;
        }

        public static T Convert_RedisHashContainer<T>(Hashtable hash)
        {
            if (!(hash is Hashtable)
                || hash["_data"].IsNullOrWhiteSpace()
                || hash["_type"].IsNullOrWhiteSpace()
                || hash["_ts"].IsNullOrWhiteSpace())
                return default(T);

            T obj = JSON.Deserialize<T>(hash["data"].ToString());

            return obj;
        }
    }
}
