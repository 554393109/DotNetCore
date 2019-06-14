using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace CySoft.Utility.Extension
{
    public static class ObjectExtensions
    {
        #region 类型判断

        /// <summary>
        /// 是否为ip
        /// </summary>
        /// <param name="ip"></param>
        /// <returns></returns>
        public static bool IsIP(this object obj)
        {
            bool isCorrect = false;
            string ip = string.Empty;

            if (obj != null)
                ip = obj.ToString();

            isCorrect = Regex.IsMatch(ip, @"^((2[0-4]\d|25[0-5]|[01]?\d\d?)\.){3}(2[0-4]\d|25[0-5]|[01]?\d\d?)$");
            return isCorrect;
        }

        /// <summary>
        /// 是否为Url
        /// </summary>
        /// <param name="ip"></param>
        /// <returns></returns>
        public static bool IsURL(this object obj)
        {
            bool isCorrect = false;
            string url = string.Empty;

            if (obj != null)
                url = obj.ToString();

            isCorrect = Regex.IsMatch(url, @"^((http|https)://)(([a-zA-Z0-9\._-]+\.[a-zA-Z]{2,6})|([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}))(:[0-9]{1,4})*(/[a-zA-Z0-9\&%_\./-~-]*)?");
            return isCorrect;
        }

        /// <summary>
        /// 判断是否Int
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static bool IsInt(this object obj)
        {
            bool isCorrect = false;
            int val = 0;

            if (obj != null)
                isCorrect = int.TryParse(obj.ToString(), out val);

            return isCorrect;
        }

        /// <summary>
        /// 判断是否Long
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static bool IsLong(this object obj)
        {
            bool isCorrect = false;
            long val = 0;

            if (obj != null)
                isCorrect = long.TryParse(obj.ToString(), out val);

            return isCorrect;
        }

        /// <summary>
        /// 判断是否Decimal
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static bool IsDecimal(this object obj)
        {
            bool isCorrect = false;
            decimal val = 0;

            if (obj != null)
                isCorrect = decimal.TryParse(obj.ToString(), out val);

            return isCorrect;
        }

        /// <summary>
        /// 判断是否DateTime
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static bool IsDateTime(this object obj)
        {
            var isCorrect = false;
            var dt = default(DateTime);

            if (obj != null)
                isCorrect = DateTime.TryParse(obj.ToString(), out dt);

            return isCorrect;
        }

        /// <summary>
        /// 判断是否DateTime
        /// 【含年月日时分秒使用】
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static bool IsDateTime2(this object obj)
        {
            var isCorrect = false;
            var dt = default(DateTime);

            var val = obj.ValueOrEmpty();
            if (!val.IsNullOrWhiteSpace())
            {
                if (val.IndexOf("年", StringComparison.OrdinalIgnoreCase) > -1)
                    // 2018年01月02日 12点34分56秒
                    isCorrect
                        = val.IndexOf("年", StringComparison.OrdinalIgnoreCase) == -1
                        ? DateTime.TryParse(val.Replace("  ", " "), out dt)
                        : DateTime.TryParse(val.Replace("  ", " ").Replace("年", "-").Replace("月", "-").Replace("日", string.Empty).Replace("时", ":").Replace("分", ":").Replace("秒", string.Empty), out dt);
            }

            return isCorrect;
        }

        /// <summary>
        /// 判断是否DateTime且是默认时间【1900-01-01 00:00:00】
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static bool IsDateTimeDefault(this object obj)
        {
            var isCorrect = false;
            var dt = default(DateTime);

            if (obj != null
                && DateTime.TryParse(obj.ToString(), out dt))
            {
                isCorrect = (new DateTime(1900, 1, 1)) == dt;
            }

            return isCorrect;
        }

        /// <summary>
        /// 判断是否Null或string.Empty
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static bool IsNullOrWhiteSpace(this object obj)
        {
            bool isCorrect = false;

            if (obj == null || string.IsNullOrWhiteSpace(obj.ToString()))
                isCorrect = true;

            return isCorrect;
        }

        #endregion 类型判断

        #region 类型转换

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="defaultvalue">默认值</param>
        /// <returns></returns>
        public static bool ToBool(this object value, bool defaultvalue = false)
        {
            if (value.IsNullOrWhiteSpace())
                return defaultvalue;

            bool result;
            if (bool.TryParse(value.ToString(), out result))
                return result;

            return defaultvalue;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="defaultvalue">默认值</param>
        /// <returns></returns>
        public static int ToInt32(this object value, int defaultvalue = 0)
        {
            if (value.IsNullOrWhiteSpace())
                return defaultvalue;

            decimal result;
            if (decimal.TryParse(value.ToString(), out result))
                return (int)result;

            return defaultvalue;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="defaultvalue">默认值</param>
        /// <returns></returns>
        public static long ToInt64(this object value, long defaultvalue = 0L)
        {
            if (value.IsNullOrWhiteSpace())
                return defaultvalue;

            decimal result;
            if (decimal.TryParse(value.ToString(), out result))
                return (long)result;

            return defaultvalue;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="defaultvalue">默认值</param>
        /// <returns></returns>
        public static double ToDouble(this object value, double defaultvalue = 0.00)
        {
            if (value.IsNullOrWhiteSpace())
                return defaultvalue;

            double result;
            if (double.TryParse(value.ToString(), out result))
                return result;

            return defaultvalue;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="defaultvalue">默认值</param>
        /// <returns></returns>
        public static decimal ToDecimal(this object value, decimal defaultvalue = 0.00M)
        {
            if (value.IsNullOrWhiteSpace())
                return defaultvalue;

            decimal result;
            if (decimal.TryParse(value.ToString(), out result))
                return result;

            return defaultvalue;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="format">格式化，如：yyyyMMddHHmmss</param>
        /// <param name="defaultvalue">默认值</param>
        /// <returns></returns>
        public static DateTime ToDateTime(this object value, string format = "", string defaultvalue = "1900-01-01 00:00:00.000")
        {
            if (!defaultvalue.IsDateTime())
                defaultvalue = "1900-01-01 00:00:00.000";

            if (value.IsNullOrWhiteSpace())
                return DateTime.Parse(defaultvalue);

            DateTime result;
            if (format.IsNullOrWhiteSpace())
            {
                if (DateTime.TryParse(value.ToString(), out result))
                    return result;
            }
            else
            {
                try
                {
                    result = DateTime.ParseExact(value.ToString(), format, null);
                    return result;
                }
                catch { }
            }

            return DateTime.Parse(defaultvalue);
        }

        public static T ToEnum<T>(this object value)
        {
            var result = (T)Enum.Parse(typeof(T), value.ValueOrEmpty(), true);
            return result;
        }

        public static bool ToTryEnum<T>(this object value, out T result)
            where T : struct
        {
            var success = Enum.TryParse<T>(value.ValueOrEmpty(), true, out result);
            return success;
        }

        #endregion 类型转换

        #region 值处理

        /// <summary>
        /// 半角转全角(SBC case)
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string ToSBC(this object value)
        {
            if (value.IsNullOrWhiteSpace())
                return string.Empty;

            //半角转全角：
            var c = value.ToString().ToCharArray();
            for (int i = 0; i < c.Length; i++)
            {
                if (c[i] == 32)
                {
                    c[i] = (char)12288;
                    continue;
                }

                if (c[i] < 127)
                    c[i] = (char)(c[i] + 65248);
            }
            return new string(c);
        }

        /// <summary>
        ///  全角转半角
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string ToDBC(this object value)
        {
            if (value.IsNullOrWhiteSpace())
                return string.Empty;

            var c = value.ToString().ToCharArray();
            for (int i = 0; i < c.Length; i++)
            {
                if (c[i] == 12288)
                {
                    c[i] = (char)32;
                    continue;
                }

                if (c[i] > 65280 && c[i] < 65375)
                    c[i] = (char)(c[i] - 65248);
            }
            return new string(c);
        }

        #endregion 值处理

        /// <summary>
        /// 为null时使用string.Empty
        /// obj?.ToString() ?? string.Empty;
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static string ValueOrEmpty(this object obj)
        {
            //return (obj == null || string.IsNullOrWhiteSpace(obj.ToString()))
            //    ? string.Empty
            //    : obj.ToString();

            return obj?.ToString() ?? string.Empty;
        }

        /// <summary>
        /// 为null或空时使用val
        /// (obj == null || string.IsNullOrWhiteSpace(obj.ToString())) ? val.ValueOrEmpty() : obj.ToString();
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="val"></param>
        /// <returns></returns>
        public static string ValueOrEmpty(this object obj, string val)
        {
            return (obj == null || string.IsNullOrWhiteSpace(obj.ToString()))
                ? val.ValueOrEmpty()
                : obj.ToString();
        }

        /// <summary>
        /// 为null或空时使用取值数组中首个非空值；若取值数组不存在非空值，则返回string.Empty
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="arr_val">取值数组</param>
        /// <returns></returns>
        public static string ValueOrFirstNotEmpty(this object obj, params string[] arr_val)
        {
            if (obj == null || string.IsNullOrWhiteSpace(obj.ToString()))
            {
                if (arr_val == null || arr_val.Length == 0)
                    return string.Empty;

                for (int i = 0; i < arr_val.Length; i++)
                {
                    if (!string.IsNullOrWhiteSpace(arr_val[i]))
                        return arr_val[i].ToString();
                }
            }

            return obj.ToString();
        }
    }
}
