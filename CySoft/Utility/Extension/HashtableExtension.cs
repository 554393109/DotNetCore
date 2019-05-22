using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CySoft.Utility.Extension
{
    public static class HashtableExtension
    {
        /// <summary>
        /// 对Hashtable里的value进行Uri.EscapeDataString，
        /// 返回一个新的Hashtable。
        /// </summary>
        /// <param name="hash"></param>
        /// <returns></returns>
        public static Hashtable UrlEscape(this Hashtable hash)
        {
            if (hash == null)
                return null;

            Hashtable param_new = new Hashtable();
            foreach (string key in hash.Keys)
            {
                if (string.IsNullOrWhiteSpace(key))
                    continue;

                if (null == hash[key] || string.IsNullOrWhiteSpace(hash[key].ToString()))
                    param_new[key] = hash[key];
                else
                    param_new[key] = Uri.EscapeDataString(hash[key].ToString());
            }

            return param_new;
        }

        /// <summary>
        /// 对Hashtable里的value进行Uri.UnescapeDataString，
        /// 返回一个新的Hashtable。
        /// </summary>
        /// <param name="hash"></param>
        /// <returns></returns>
        public static Hashtable UrlUnescape(this Hashtable hash)
        {
            if (hash == null)
                return null;

            Hashtable param_new = new Hashtable();
            foreach (string key in hash.Keys)
            {
                if (string.IsNullOrWhiteSpace(key))
                    continue;

                if (null == hash[key] || string.IsNullOrWhiteSpace(hash[key].ToString()))
                    param_new[key] = hash[key];
                else
                    param_new[key] = Uri.UnescapeDataString(hash[key].ToString());
            }

            return param_new;
        }

        public static Dictionary<string, string> ToDictionary(this Hashtable hash)
        {
            var dic = new Dictionary<string, string>();

            foreach (string key in hash.Keys)
            {
                if (hash[key] != null)
                    dic.Add(key, hash[key].ToString());
            }
            return dic;
        }

        public static SortedDictionary<string, string> ToSortedDictionary(this Hashtable hash)
        {
            SortedDictionary<string, string> sTable = new SortedDictionary<string, string>();

            foreach (string iterm in hash.Keys)
            {
                sTable[iterm] = hash[iterm] != null ? hash[iterm].ToString() : string.Empty;
            }
            return sTable;
        }

        public static Dictionary<K, V> ToDictionary<K, V>(this Hashtable hash)
        {
            return hash
              .Cast<DictionaryEntry>()
              .ToDictionary(kvp => (K)kvp.Key, kvp => (V)kvp.Value);
        }


        /// <summary>
        /// 遍历并返回URL参数格式,a=1&b=2
        /// 忽略value为空的项
        /// </summary>
        /// <param name="hash"></param>
        /// <returns></returns>
        public static string ToUrlParams(this Hashtable hash, bool needEncode = true)
        {
            if (hash == null || hash.Count == 0)
                return string.Empty;

            var dic = hash.ToDictionary<string, object>();
            var enumerator = new SortedDictionary<string, object>(dic).GetEnumerator();
            StringBuilder builder = new StringBuilder();
            while (enumerator.MoveNext())
            {
                string key = enumerator.Current.Key;
                string val = enumerator.Current.Value?.ToString();
                if (key.IsNullOrWhiteSpace() || val.IsNullOrWhiteSpace())
                    continue;

                if (needEncode)
                    builder.Append("&").Append(key).Append("=").Append(Uri.EscapeDataString(val));
                else
                    builder.Append("&").Append(key).Append("=").Append(val);
            }

            return builder.ToString().TrimStart('&');
        }

        /// <summary>
        /// 移除空项
        /// </summary>
        /// <param name="hash"></param>
        /// <returns></returns>
        public static Hashtable RemoveEmpty(this Hashtable hash)
        {
            if (hash == null)
                return null;

            var param_new = new Hashtable();
            foreach (string key in hash.Keys)
            {
                if (key.IsNullOrWhiteSpace() || hash[key].IsNullOrWhiteSpace())
                    continue;

                param_new[key] = hash[key].ToString();
            }

            return param_new;
        }

        /// <summary>
        /// 移除非空项前后空格
        /// </summary>
        /// <param name="hash"></param>
        /// <returns></returns>
        public static Hashtable RemoveSpace(this Hashtable hash)
        {
            if (hash == null)
                return null;

            var param_new = new Hashtable();
            foreach (string key in hash.Keys)
            {
                if (key.IsNullOrWhiteSpace() || hash[key].IsNullOrWhiteSpace())
                    continue;

                param_new[key] = hash[key].ToString().Trim();
            }

            return param_new;
        }

        /// <summary>
        /// 判断该键值是否存在或为空或为NULL
        /// </summary>
        /// <param name="hash"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        public static bool IsNullOrWhiteSpace(this Hashtable hash, string name)
        {
            try
            {
                if (hash == null)
                    return true;
                if (hash.ContainsKey(name))
                {
                    var str = (hash[name] ?? string.Empty).ToString();
                    return string.IsNullOrWhiteSpace(str);
                }
                return true;
            }
            catch { }
            return true;
        }

        /// <summary>
        /// 取出该键值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="hash"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        public static T GetValue<T>(this Hashtable hash, string name)
        {
            if (hash == null || string.IsNullOrWhiteSpace(name))
                return default(T);
            try
            {
                if (hash.ContainsKey(name))
                    return (T)hash[name];
            }
            catch
            {
            }
            return default(T);
        }

        /// <summary>
        /// 合并多个Hashtable
        /// </summary>
        /// <param name="hash"></param>
        /// <param name="arr_hash"></param>
        /// <returns>返回新实例</returns>
        public static Hashtable Merge(this Hashtable hash, params Hashtable[] arr_hash)
        {
            var hash_new = new Hashtable();

            if (hash is Hashtable)
            {
                foreach (var key in hash.Keys)
                    hash_new[key] = hash[key];
            }

            if (arr_hash != null && arr_hash.Length > 0)
            {
                var hash_em = arr_hash.GetEnumerator();
                while (hash_em.MoveNext())
                {
                    if (hash_em.Current is Hashtable)
                    {
                        var hash_curr = hash_em.Current as Hashtable;
                        foreach (var key in hash_curr.Keys)
                            hash_new[key] = hash_curr[key];
                    }
                }
            }

            return hash_new;
        }
    }
}
