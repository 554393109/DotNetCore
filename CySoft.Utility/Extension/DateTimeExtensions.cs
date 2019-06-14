using System;

namespace CySoft.Utility.Extension
{
    public static class DateTimeExtensions
    {
        /// <summary>
        /// 本地时间 -> 时间戳
        /// </summary>
        /// <param name="dateTime"></param>
        /// <returns></returns>
        public static long ToUnixTimestamp(this DateTime dateTime, TimestampAccuracy accuracy = TimestampAccuracy.Seconds)
        {
            return DateTimeHelper.ToUnixTimestamp(dateTime, accuracy);
        }

        /// <summary>
        /// 本地时间.ToUniversalTime() -> 时间戳
        /// </summary>
        /// <param name="dateTime"></param>
        /// <returns></returns>
        public static long ToUnixUTCTimestamp(this DateTime dateTime, TimestampAccuracy accuracy = TimestampAccuracy.Seconds)
        {
            return DateTimeHelper.ToUnixUTCTimestamp(dateTime, accuracy);
        }
    }
}
