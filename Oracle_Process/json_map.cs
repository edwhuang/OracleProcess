using System;

using System.Collections.Generic;
using Oracle.ManagedDataAccess.Client;
using System.Net;
using System.Text;
using System.IO;

namespace Oracle_Process
{
    public class json_mapper
    {
        public json_mapper()
        {
        }
        public string exec_map(OracleDataReader _rd, Dictionary<string, string> map)
        {


            string _content = "";
            string _row_sp = "";
            long rowcnt = 0;
            while (_rd.Read()) {

                string row_content = "";
                string _sp = "";
                foreach (var key in map.Keys) {
                    if (key.ToString().Substring(0, 1) == "_") {
                        row_content = row_content + _sp + "\"" + map[key].ToString() + "\"" +
                           ":" + key.ToString() ;
                    }
                    else
                    {

                        row_content = $"{row_content}{_sp}\"{map[key].ToString()}\":\"{_rd[key.ToUpper()].ToString()}\"";
                    }
                    _sp = ",";
                }
                row_content = "{" + row_content + "}";
                _content = _content + _row_sp + row_content;
                _row_sp = ",";
                rowcnt++;
            }
            if (rowcnt > 1) { _content = "[" + _content + "]"; };
            return _content;
        }

        public string HttpRequest(string url, string postData)
        {
            string _result;
            try
            {
                ServicePointManager.ServerCertificateValidationCallback = delegate { return true; };
                string _post_data = postData;
                //  _result = WebRequest(url, _post_data);

                var jsonBytes = Encoding.ASCII.GetBytes(_post_data);
                ServicePointManager.ServerCertificateValidationCallback = delegate { return true; };
                var httpWebRequest = (HttpWebRequest)System.Net.WebRequest.Create(url);
                httpWebRequest.ProtocolVersion = HttpVersion.Version10;//http1.0
                                                                       //httpWebRequest.Connection = "Close";
                httpWebRequest.ContentType = "application/json; charset=utf-8";
                httpWebRequest.Method = "POST";
                httpWebRequest.ContentLength = jsonBytes.Length;
                var streamWriter = httpWebRequest.GetRequestStream();
                //   StreamWriter streamWriter = new StreamWriter(httpWebRequest.GetRequestStream());
                string json = _post_data;

                streamWriter.Write(jsonBytes, 0, jsonBytes.Length);
                streamWriter.Flush();
                //   streamWriter.Close();
                try
                {
                    HttpWebResponse httpResponse = (HttpWebResponse)httpWebRequest.GetResponse();
                    StreamReader streamReader = new StreamReader(httpResponse.GetResponseStream());
                    String result = streamReader.ReadToEnd();
                    _result = result;
                }
                catch (WebException ex)
                {
                    using (var stream = ex.Response.GetResponseStream())
                    using (var reader = new StreamReader(stream))
                    {
                        String result = reader.ReadToEnd();
                        return result;
                    }
                }


                // logger.Info(_result);
                return _result;
            }
            catch (Exception e)
            {
                return e.Message;
            }

        }

    }

    public class account_center_mapper
    {
        string json_call;
        string _sql_mas;
        string _sql_dtl;
        string _url;
        Dictionary<string, string> _map_c;
        Dictionary<string, string> _map_sub;
        json_mapper jp;
       
        public account_center_mapper(string account_center_url)
        {

            _url = account_center_url;
            jp = new json_mapper();
            json_call = @"{
                ""id"": ""TEST"",
  ""jsonrpc"": ""2.0"",
  ""method"": ""setAccountInfo"",
  ""params"": _PARAMS_
    }";
    _sql_mas = "SELECT * FROM BSM_CLIENT_MAS WHERE SERIAL_ID=:SERIAL_ID";
            _sql_dtl = @"select  to_char(nvl(""transaction_id"",b.pk_no)) transaction_id,
             nvl(""package_id"", b.package_id) package_id ,
             to_char(""created"" - (8 / 24), 'YYYY-MM-DD HH24:MI:SS') created,
             to_char(""last_modified"" - (8 / 24), 'YYYY-MM-DD HH24:MI:SS') last_modified,
             decode(""device_id"", null, 'null',  ""device_id"") device_id,
             decode(Pay_type, null, 'null', '贈送', 'gift', '兌換券', 'coupon', '信用卡', 'credit', Pay_type) payment_method,
           to_char(nvl(nvl(""service_start_time"", b.start_date),
                         to_date('2000/01/01',
                                 'YYYY/MM/DD')) - (8 / 24),
                     'YYYY-MM-DD HH24:MI:SS') start_date,
             to_char(nvl(nvl(""service_end_time"", b.end_date),
                         to_date('2999/12/31 23:59:59',
                                 'YYYY/MM/DD HH24:MI:SS')) - (8 / 24),
                     'YYYY-MM-DD HH24:MI:SS') end_date,
            decode(""free_start_time"", null, 'null', to_char(""free_start_time"" - (8 / 24), 'YYYY-MM-DD HH24:MI:SS')) free_start_date,
            decode(""free_end_time"", null, 'null', to_char(""free_end_time"" - (8 / 24), 'YYYY-MM-DD HH24:MI:SS')) free_end_date
           from acl.subscription a, bsm_client_details b,bsm_purchase_mas c
       where b.pk_no = a.""transaction_id""(+)
       and c.mas_no(+) = b.src_no
        and b.status_flg = 'P' and b.SERIAL_ID=:SERIAL_ID";
            _map_c = new Dictionary<string, string>();
            _map_sub = new Dictionary<string, string>();

            _map_c.Add("SERIAL_ID", "client_id");
            _map_c.Add("OWNER_PHONE", "phone");
            _map_c.Add("_SUBSCRIPTIONS_", "subscriptions");

            _map_sub.Add("transaction_id", "transaction_id");
            _map_sub.Add("package_id", "package_id");
            _map_sub.Add("created", "created");
            _map_sub.Add("last_modified", "last_modified");
            _map_sub.Add("device_id", "device_id");
            _map_sub.Add("payment_method", "payment_method");
            _map_sub.Add("start_date", "start_date");
            _map_sub.Add("end_date", "end_date");
            _map_sub.Add("free_start_date", "free_start_date");
            _map_sub.Add("free_end_date", "free_end_date");

        }

        public string set_account_center(string p_account_id,OracleConnection _conn)
        {
            string content;
            string content_sub;
            OracleCommand _cmd = new OracleCommand(_sql_mas, _conn);
            try
            {
                _cmd.Parameters.Add("SERIAL_ID", p_account_id);
                OracleDataReader _rd = _cmd.ExecuteReader();
                content = jp.exec_map(_rd, _map_c);
            }
            finally
            {
                _cmd.Dispose();
            }
            OracleCommand _cmd_d = new OracleCommand(_sql_dtl, _conn);
            try
            {
                _cmd_d.Parameters.Add("SERIAL_ID", p_account_id);
                OracleDataReader _rd_d = _cmd_d.ExecuteReader();
                content_sub = jp.exec_map(_rd_d, _map_sub);
            }
            finally
            {
                _cmd.Dispose();
            }

            content = content.Replace("_SUBSCRIPTIONS_", content_sub);
            json_call = json_call.Replace("_PARAMS_", content);
            return jp.HttpRequest(_url, json_call);
        }



    }
       
        



   
}
