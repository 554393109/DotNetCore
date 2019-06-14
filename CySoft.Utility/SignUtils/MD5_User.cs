using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace CySoft.Utility.SignUtils
{
    public class MD5_User
    {
        /// <summary>
        /// 使用UTF-8进行MD5签名
        /// 忽略参数中存的sign
        /// 最后{key}
        /// </summary>
        /// <param name="parameters"></param>
        /// <param name="key">签名密钥</param>
        /// <returns></returns>
        public static string Sign(IDictionary parameters, string key, FromatSign format = FromatSign.UPPER)
        {
            string str_sign = null;
            if (parameters != null && parameters.Count > 0)
            {
                Dictionary<string, string> parameters_temp = new Dictionary<string, string>();
                foreach (DictionaryEntry item in parameters)
                {
                    string val = (item.Value == null)
                        ? string.Empty
                        : item.Value.ToString();

                    parameters_temp.Add((string)item.Key, val);
                }

                str_sign = _Sign(parameters_temp, key, format);
            }
            return str_sign;
        }

        /// <summary>
        /// 给请求参数签名
        /// </summary>
        /// <param name="parameters">所有字符型的请求参数，忽略参数中存的sign</param>
        /// <param name="key">签名密钥</param>
        /// <param name="qhs">是否前后都加密钥进行签名</param>
        /// <returns>签名</returns>
        private static string _Sign(IDictionary<string, string> parameters, string key, FromatSign format)
        {
            // 第一步：把字典按Key的字母顺序排序
            IDictionary<string, string> sortedParams = new SortedDictionary<string, string>(parameters);
            IEnumerator<KeyValuePair<string, string>> dem = sortedParams.GetEnumerator();

            // 第二步：把所有参数名和参数值串在一起
            StringBuilder query = new StringBuilder();
            while (dem.MoveNext())
            {
                string _key = dem.Current.Key;
                string value = dem.Current.Value;
                if (!string.IsNullOrWhiteSpace(_key) && !string.IsNullOrWhiteSpace(value))
                {
                    //忽略sign参数
                    if (_key.Equals("sign", StringComparison.OrdinalIgnoreCase))
                        continue;

                    query.Append("&").Append(_key).Append("=").Append(value);
                }
            }

            query.Append(key ?? string.Empty);

            StringBuilder result = new StringBuilder();
            // 第三步：使用MD5加密
            using (System.Security.Cryptography.MD5CryptoServiceProvider md5 = new System.Security.Cryptography.MD5CryptoServiceProvider())
            //using (System.Security.Cryptography.MD5 md5 = System.Security.Cryptography.MD5.Create())
            {
                byte[] bytes = md5.ComputeHash(Encoding.UTF8.GetBytes(query.ToString().Trim('&')));

                // 第四步：把二进制转化为大写的十六进制
                for (int i = 0; i < bytes.Length; i++)
                {
                    string hex = string.Empty;

                    if (format == FromatSign.UPPER)
                        hex = bytes[i].ToString("X2");                   //大写
                    else
                        hex = bytes[i].ToString("x2");                   //小写

                    result.Append(hex);
                }
            }



            return result.ToString();
        }

        /// <summary>
        /// 验证签名
        /// </summary>
        /// <param name="parameters"></param>
        /// <param name="key">签名密钥</param>
        /// <returns></returns>
        public static bool CheckSign(IDictionary parameters, string key)
        {
            bool flag = false;

            try
            {
                //获取接收到的签名
                string return_sign = (string)parameters["sign"];
                if (!string.IsNullOrWhiteSpace(return_sign))
                {
                    //在本地计算新的签名
                    string cal_sign = Sign(parameters, key);

                    if (cal_sign.Equals(return_sign, StringComparison.Ordinal))
                        flag = true;

                    //若sign == InternalSignKey，返回true
                    if (return_sign.Equals(SystemKey.InternalSignKey, StringComparison.OrdinalIgnoreCase))
                        flag = true;

                    //Debug模式不验签，返回true
                    if (SystemKey.IsDebugMode)
                        flag = true;
                }
            }
            catch (Exception ex)
            {
                LogHelper.Error(ex);
            }

            return flag;
        }

        /// <summary>
        /// 将url参数转换成map 
        /// @param param aa=11&bb=22&cc=33 
        /// @return 。
        /// </summary>  
        public static Hashtable GetUrlParams(string param)
        {
            Hashtable ht = new Hashtable();
            if (string.IsNullOrWhiteSpace(param))
                return ht;

            var paramArr = param.Split('&');
            foreach (var item in paramArr)
            {
                var itemArr = item.Split('=');
                if (itemArr.Length == 2)
                    ht.Add(itemArr[0], itemArr[1]);
                else if (itemArr.Length > 2)
                    ht.Add(itemArr[0], item.Substring(itemArr[0].Length, item.Length - itemArr[0].Length));
            }
            return ht;
        }
    }
}
