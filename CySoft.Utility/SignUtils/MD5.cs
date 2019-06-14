//888888888888888888888888888888888888888888888888888888888888888888888888888888888888888888888888
//888888888888888888888888888888888888!ooooo*88888888888888888888888888888888888888888888888888888
//8888888888888888888888888888888888o!!!;;;!!o!888888888888888888888888888888888888888888888888888
//888888888888888888888888888888888!!!;;;;;;;!!!$8888888888888888888888888888888888888888888888888
//888888888888888888888888888888888!!;;;;;;;;;;!!88888888888888888888888888$!!!$888888888888888888
//888888888888888888888888888888888o!!;;;;;;;;;!oo888888888$!!oo!!!oo!$&!o!o!!!oo!!!88888888888888
//888888888888888888888o!!!!$888888&!o;;;;;;;;;;o!&o!!oo!!!o!!!!!!;;!!o!o;;;;;;;;;;!o!888888888888
//888888888888888888&!!!!;!!!o!o8888o!!;;;;!;!!!ooo!!;!;;;;;;;;;;;;;;;!!;;;;;;;;;;;;;!!!8888888888
//888888888888888888oo;;;;;;;;;o!o888o!o;!oo!o!!;;;;;;;;;;;;;;;;;;;;;;!!;;;;;;;;;!!o!;!oo888888888
//88888888888888888$!!;;;;;;;;;;;!o!88!o!!!;;;;;;;;;;;;ooo!!!oo;;;;;;;oo!;;;;!;;!;oo;;;!o!88888888
//888888888888888888o!;;;;;;;;;;;;!!ooo;;;;;;;;;;;;;;;!o;.....o!!;;;;;;o!;;;o!!!;;;;;;;;oo&8888888
//888888888888888888&oo;;;;;;;;;!o!!;;;;;;;;;!ooo!;;;!o!8888..;!o;;;;;;;o!!;;;!;;;;;;;;;!!o8888888
//88888888888888888888o!!;;;;;!oo!;;;;;;;;;o!o;;!o!o;!o!!88o..!o!;;;;;;;;!!!;;;;;;;;;;;;!!*8888888
//888888888888888888888$!!o;o!!!;;;;;;;;;!!o;.;...;oo;;!oooo!o!;;;;;;;;;;;!!o!;;;;;;;;;!!o88888888
//888888888888888888888888o!!!;;;;;;;;;;;!!!8888...!!!;;;;;;!;;;;;;;;;;;;;;;!ooo!;;;;;!oo888888888
//88888888888888888888888o!!;;;;;;;;;;;;;;!o&88...;oo;;;;;;;;;;;;;;;;;;;;;;;;;;!ooooooo&8888888888
//888888888888888888888*!!;;;;;;;;;;;;;;;;;!o!ooo!o!;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;ooo&888888888888
//88888888888888888888!oo;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;!ooo888888888888888
//8888888888888888888!!!;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;oo!*88888888888888888
//888888888888888888!!!;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;!o!o!&8888888888888888888
//88888888888888888$oo;;;;;;;;;;;o!o!!!;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;oo!o88888888888888888888888
//88888888888888888!!!;;;;;;;;o!o!ooooooo;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;!!!88888888888888888888888
//8888888888888888ooo;;;;;;;!!!!!ooooooooo!;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;!!o8888888888888888888888
//8888888888888888o!!;;;;;;;!!!!ooooooooooo;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;!oo888888888888888888888
//8888888888888888!o;;;;;;;;!!ooooooooooooo;;;;;;;;;;;;;;;;;;;;;;;o$$;;;;;;!o888888888888888888888
//8888888888888888!!;;;;;;;;;!oooooooooooo!;;;;;;;;;;;;;;;;;;;;;;;o$$;;;;;;!!888888888888888888888
//8888888888888888o!;;;;;;;;;;!o!oooooooo;;;;;;;;;;;;;;;;;;;;;;;;!$$*;;;;;;!!888888888888888888888
//8888888888888888!o!;;;;;;;;;;;;!!oo!;;;;;;;o$o!;;;;;;;;;;;;;;;;$$$;;;;;;;!!888888888888888888888
//8888888888888888&!o;;;;;;;;;;;;;;;;;;;;;;;;;$$$$!;;;;;;;;;!;;$$$o;;;;;;;;!!888888888888888888888
//88888888888888888!!!;;;;;;;;;;;;;;;;;;;;;;;;;;!$$$$$$$$$$$$$$$!;;;;;;;;;!!$888888888888888888888
//888888888888888888o!!;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;!!oo!!!;;;;;;;;;;;;;!!!8888888888888888888888
//8888888888888888888!!!;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;!oo88888888888888888888888
//88888888888888888888!!o;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;!!o888888888888888888888888
//888888888888888888888&o!!;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;!!o88888888888888888888888888
//88888888888888888888888&!!!!;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;!!o!8888888888888888888888888888
//8888888888888888888888888;!ooo!;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;ooooooo!888888888888888888888888888
//888888888888888888888888ooo**o!oo!o!;;;;;;;;;;;;;;;;;;;!!!!o!o******!!!8888888888888888888888888
//88888888888888888888888!o*********o!!oooo!oo!!!!!!ooo!!oo************o!!888888888888888888888888
//8888888888888888888888!!**********************oo***********************!!88888888888888888888888
//88888888888888888888o!!*************************************************o!8888888888888888888888
//888888888888888888!!oo***************************************************!o!88888888888888888888

/************************************************************************
 * 文件标识：  e4818b0e-d391-425c-9cc0-1ea4dc658fa3
 * 项目名称：  CySoft.Utility.SignUtils  
 * 项目描述：  
 * 类 名 称：  MD5
 * 版 本 号：  v1.0.0.0 
 * 说    明：  MD5签名
 * 作    者：  尹自强
 * 创建时间：  2018/8/21 11:37:46
 * 更新时间：  2018/8/21 11:37:46
************************************************************************
 * Copyright @ 尹自强 2018. All rights reserved.
************************************************************************/

using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using CySoft.Utility.Extension;

namespace CySoft.Utility.SignUtils
{
    public class MD5
    {
        /// <summary>
        /// 使用UTF-8进行MD5签名
        /// 忽略参数中存的sign和value为空的键值对
        /// 最后&key={key}
        /// </summary>
        /// <param name="parameters"></param>
        /// <param name="sign_key">签名密钥</param>
        /// <param name="format">大小写格式化 默认：FromatSign.UPPER</param>
        /// <returns></returns>
        public static string Sign(IDictionary parameters, string sign_key, FromatSign format = FromatSign.UPPER)
        {
            if (parameters == null || parameters.Count == 0)
                return string.Empty;

            var parameters_temp = new Dictionary<string, string>();
            foreach (DictionaryEntry item in parameters)
            {
                if (item.Key.IsNullOrWhiteSpace()
                    || item.Value.IsNullOrWhiteSpace())
                    continue;

                parameters_temp.Add(item.Key.ToString(), item.Value.ToString());
            }

            var str_sign = BuildSortString(parameters_temp, sign_key, format);
            return str_sign;
        }

        /// <summary>
        /// key排序+拼接
        /// </summary>
        /// <param name="parameters"></param>
        /// <param name="sign_key">签名密钥</param>
        /// <param name="format">大小写格式化 默认：FromatSign.UPPER</param>
        /// <returns></returns>
        private static string BuildSortString(IDictionary<string, string> parameters, string sign_key, FromatSign format)
        {
            // 把字典按Key的字母顺序排序
            var sortedParams = new SortedDictionary<string, string>(parameters);
            var query_string = new StringBuilder();

            var dem = sortedParams.GetEnumerator();
            while (dem.MoveNext())
            {
                var key = dem.Current.Key;
                var value = dem.Current.Value;
                if ("SIGN".Equals(key, StringComparison.OrdinalIgnoreCase))
                    continue;

                query_string.Append("&").Append(key).Append("=").Append(value);
            }

            // {query_string}&key={sign_key}
            query_string.Append("&key=").Append(sign_key.ValueOrEmpty());

            return Sign(query_string.ToString().TrimStart('&'), null, format);
        }

        /// <summary>
        /// 使用UTF-8进行MD5签名
        /// 加盐&key={sign_key}，若sign_key.IsNullOrWhiteSpace()则不加盐
        /// </summary>
        /// <param name="content">签名原文</param>
        /// <param name="sign_key">签名密钥 为空则不加盐</param>
        /// <param name="format">大小写格式化 默认：FromatSign.UPPER</param>
        /// <returns></returns>
        public static string Sign(string content, string sign_key, FromatSign format = FromatSign.UPPER)
        {
            if (content.IsNullOrWhiteSpace())
                return string.Empty;

            if (!sign_key.IsNullOrWhiteSpace())
                content = $"{content}&key={sign_key}";

            var _format = format == FromatSign.UPPER
                        ? "X2"              /*大写*/
                        : "x2";             /*小写*/

            var result = new StringBuilder();
            using (var provider = new System.Security.Cryptography.MD5CryptoServiceProvider())
            {
                var byte_sign = provider.ComputeHash(Encoding.UTF8.GetBytes(content));

                // 把二进制转化为十六进制
                for (int i = 0; i < byte_sign.Length; i++)
                    result.Append(byte_sign[i].ToString(format: _format));
            }

            return result.ToString();
        }

        /// <summary>
        /// 验证签名
        /// </summary>
        /// <param name="parameters">需验签内容</param>
        /// <param name="sign_key">签名密钥</param>
        /// <returns></returns>
        public static bool CheckSign(IDictionary parameters, string sign_key)
        {
            if (parameters == null
                || parameters.Count == 0
                || parameters["sign"].IsNullOrWhiteSpace())
                return false;

            var param_sign = parameters["sign"].ToString();        //获取接收到的签名
            var calc_sign = string.Empty;

            try
            {
                //在本地计算新的签名
                calc_sign = Sign(parameters, sign_key);
            }
            catch (Exception ex)
            {
                LogHelper.Error(ex);
            }

            bool flag = false;

            if (calc_sign.Equals(param_sign, StringComparison.OrdinalIgnoreCase))
                flag = true;

            //若sign == InternalSignKey，返回true
            if (param_sign.Equals(SystemKey.InternalSignKey, StringComparison.OrdinalIgnoreCase))
                flag = true;

            // IsDebugMode不验签，返回true
            if (SystemKey.IsDebugMode)
                flag = true;

            return flag;
        }

        /// <summary>
        /// 验证签名
        /// </summary>
        /// <param name="content">需验签内容</param>
        /// <param name="sign">签名</param>
        /// <param name="sign_key">签名密钥</param>
        /// <returns></returns>
        public static bool CheckSign(string content, string sign, string sign_key)
        {
            if (content.IsNullOrWhiteSpace()
                || sign.IsNullOrWhiteSpace()
                /*|| sign_key.IsNullOrWhiteSpace()*/)
                return false;

            var calc_sign = string.Empty;

            try
            {
                calc_sign = Sign(content, sign_key);
            }
            catch (Exception ex)
            {
                LogHelper.Error(ex);
            }

            bool flag = false;
            if (calc_sign.Equals(sign, StringComparison.OrdinalIgnoreCase))
                flag = true;

            // 若sign == InternalSignKey，返回true
            if (sign.Equals(SystemKey.InternalSignKey, StringComparison.OrdinalIgnoreCase))
                flag = true;

            // IsDebugMode不验签，返回true
            if (SystemKey.IsDebugMode)
                flag = true;

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
