using System;

using Newtonsoft.Json;

namespace CySoft.Utility
{
    public static class JSON
    {
        private static readonly JsonSerializerSettings defaultSettings = new JsonSerializerSettings
        {
            DateFormatString = "yyyy-MM-dd HH:mm:ss.fff",           // 格式化DateTime
            FloatFormatHandling = FloatFormatHandling.String,       // 格式化浮点型【防止溢出和转换为科学计数表达】，不能使用FloatFormatHandling.Symbol
            FloatParseHandling = FloatParseHandling.Decimal,        // 格式化浮点型【防止溢出和转换为科学计数表达】
            NullValueHandling = NullValueHandling.Include,          // null值处理
        };

        public static T Deserialize<T>(string input, string dateFormatString = "yyyy-MM-dd HH:mm:ss.fff")
        {
            var settings = new JsonSerializerSettings
            {
                DateFormatString = dateFormatString,                    // 格式化DateTime
                FloatFormatHandling = FloatFormatHandling.String,       // 格式化浮点型【防止溢出和转换为科学计数表达】
                FloatParseHandling = FloatParseHandling.Decimal,        // 格式化浮点型【防止溢出和转换为科学计数表达】
            };

            try
            {
                return JsonConvert.DeserializeObject<T>(input, settings: settings);
            }
            catch (ArgumentNullException ex)
            {
                //TextLogHelper.WriterExceptionLog(ex);
                // LogHelper.Error(ex);
                throw ex;
            }
            catch (ArgumentException ex)
            {
                //TextLogHelper.WriterExceptionLog(ex);
                // LogHelper.Error(ex);
                throw ex;
            }
            catch (InvalidOperationException ex)
            {
                //TextLogHelper.WriterExceptionLog(ex);
                // LogHelper.Error(ex);
                throw ex;
            }
            catch (Exception ex)
            {
                //TextLogHelper.WriterExceptionLog(ex);
                // LogHelper.Error(ex);
                throw ex;
            }
        }

        public static string Serialize(object obj, int formatting = 0, bool ignoreNull = false, string dateFormatString = "yyyy-MM-dd HH:mm:ss.fff")
        {
            var settings = new JsonSerializerSettings
            {
                DateFormatString = dateFormatString,                    // 格式化DateTime
                FloatFormatHandling = FloatFormatHandling.String,       // 格式化浮点型【防止溢出和转换为科学计数表达】，不能使用FloatFormatHandling.Symbol
                FloatParseHandling = FloatParseHandling.Decimal,        // 格式化浮点型【防止溢出和转换为科学计数表达】

                // null值处理
                NullValueHandling = ignoreNull ? NullValueHandling.Ignore : NullValueHandling.Include
            };

            return JSON.Serialize(obj, formatting: formatting, settings: settings);
        }

        public static string Serialize(object obj, int formatting, JsonSerializerSettings settings = null)
        {
            try
            {
                settings = settings ?? defaultSettings;

                return JsonConvert.SerializeObject(
                    value: obj
                    , formatting: (Newtonsoft.Json.Formatting)formatting
                    , settings: settings);
            }
            catch (InvalidOperationException ex)
            {
                // LogHelper.Error(ex);
                throw ex;
            }
            catch (ArgumentException ex)
            {
                // LogHelper.Error(ex);
                throw ex;
            }
            catch (Exception ex)
            {
                // LogHelper.Error(ex);
                throw ex;
            }
        }

        /// <summary>
        /// 将给定对象转换为指定类型。
        /// </summary>
        /// <typeparam name="T">obj 将转换成的类型。</typeparam>
        /// <param name="obj">序列化的 JSON 字符串。</param>
        /// <returns>已转换为目标类型的对象。</returns>
        public static T ConvertToType<T>(object obj)
        {
            try
            {
                return JSON.Deserialize<T>(JSON.Serialize(obj));
            }
            catch (InvalidOperationException ex)
            {
                //TextLogHelper.WriterExceptionLog(ex);
                // LogHelper.Error(ex);
                throw ex;
            }
            catch (ArgumentException ex)
            {
                //TextLogHelper.WriterExceptionLog(ex);
                // LogHelper.Error(ex);
                throw ex;
            }
            catch (Exception ex)
            {
                //TextLogHelper.WriterExceptionLog(ex);
                // LogHelper.Error(ex);
                throw ex;
            }
        }
    }
}

