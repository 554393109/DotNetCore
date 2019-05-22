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
 * 文件标识：  c7d636c0-2bc7-4973-a1f9-790218e386ff
 * 项目名称：  CySoft.Utility.DEncrypt.DES  
 * 项目描述：  C#（CBC/PKCS7）<===> JAVA（CBC/PKCS5），C#（ECB/PKCS7）<===> JAVA（ECB/PKCS5）
 * 
 * 类 名 称：  Des3Encrypt
 * 版 本 号：  v1.0.0.0 
 * 说    明：  
 * 作    者：  尹自强
 * 创建时间：  2018/9/20 17:22:39
 * 更新时间：  2018/9/20 17:22:39
************************************************************************
 * Copyright @ 尹自强 2018. All rights reserved.
************************************************************************/

using System;
using System.Security.Cryptography;
using System.Text;
using CySoft.Utility.Extension;

namespace CySoft.Utility.DEncrypt
{
    public class Des3Encrypt
    {
        private static readonly Encoding encoding = Encoding.UTF8;
        private static readonly byte[] IV = InitIV(16);

        public static string Encrypt(string content, string key, string mode = "ECB", string padding = "PKCS7")
        {
            var result = string.Empty;
            if (string.IsNullOrWhiteSpace(content)
                || string.IsNullOrWhiteSpace(key))
                return result;

            var cipherMode = mode.ToEnum<CipherMode>();
            var paddingMode = padding.ToEnum<PaddingMode>();

            try
            {
                var byte_content = encoding.GetBytes(content);
                var byte_key = encoding.GetBytes(key);

                using (var provider = new TripleDESCryptoServiceProvider() { Key = byte_key, Mode = cipherMode, Padding = paddingMode })
                {
                    if (CipherMode.CBC == cipherMode)
                        provider.IV = IV;

                    #region 方法1

                    using (var encryptor = provider.CreateEncryptor())
                    {
                        var byte_result = encryptor.TransformFinalBlock(byte_content, 0, byte_content.Length);
                        result = Convert.ToBase64String(byte_result);
                    }

                    #endregion 方法1

                    #region 方法2

                    //using (var stream = new MemoryStream())
                    //{
                    //    var cStream = new CryptoStream(stream, provider.CreateEncryptor(), CryptoStreamMode.Write);
                    //    cStream.Write(byte_content, 0, byte_content.Length);
                    //    cStream.FlushFinalBlock();
                    //    cStream.Close();

                    //    var byte_result = stream.ToArray();
                    //    stream.Close();

                    //    result = Convert.ToBase64String(byte_result);
                    //}

                    #endregion 方法2
                }
            }
            catch (Exception ex)
            {
                // LogHelper.Error(ex);
            }

            return result;
        }

        public static string Dencrypt(string content, string key, string mode = "ECB", string padding = "PKCS7")
        {
            var result = string.Empty;
            if (string.IsNullOrWhiteSpace(content)
                || string.IsNullOrWhiteSpace(key))
                return result;

            try
            {
                var byte_content = Convert.FromBase64String(content);
                var byte_key = encoding.GetBytes(key);

                var cipherMode = mode.ToEnum<CipherMode>();
                var paddingMode = padding.ToEnum<PaddingMode>();

                using (var provider = new TripleDESCryptoServiceProvider() { Key = byte_key, Mode = cipherMode, Padding = paddingMode })
                {
                    if (CipherMode.CBC == cipherMode)
                        provider.IV = IV;

                    #region 方法1

                    using (var decryptor = provider.CreateDecryptor())
                    {
                        var byte_result = decryptor.TransformFinalBlock(byte_content, 0, byte_content.Length);
                        result = encoding.GetString(byte_result);
                    }

                    #endregion 方法1

                    #region 方法2

                    //using (var stream = new MemoryStream())
                    //{
                    //    var cStream = new CryptoStream(stream, provider.CreateDecryptor(), CryptoStreamMode.Write);
                    //    cStream.Write(byte_content, 0, byte_content.Length);
                    //    cStream.FlushFinalBlock();
                    //    cStream.Close();

                    //    var byte_result = stream.ToArray();
                    //    stream.Close();

                    //    result = encoding.GetString(byte_result);
                    //}

                    #endregion 方法2
                }
            }
            catch (Exception ex)
            {
                // LogHelper.Error(ex);
            }

            return result;
        }

        /// <summary>
        /// 初始化向量
        /// </summary>
        /// <param name="blockSize"></param>
        /// <returns></returns>
        private static byte[] InitIV(int blockSize)
        {
            var iv = new byte[blockSize];
            for (int i = 0; i < blockSize; i++)
                iv[i] = (byte)0x0;

            return iv;
        }
    }
}
