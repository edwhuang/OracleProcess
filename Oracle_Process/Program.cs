using System;
using System.Data;
using System.Threading;
using Oracle.ManagedDataAccess.Client;
namespace Oracle_Process
{
    class Program
    {
        static void Main(string[] args)
        {
            OracleConnection _conn = new OracleConnection();
            string process_type = "";
            string client_id = "";
            string purchase_no = "";
            decimal? purchase_pk_no = 0;
            Boolean retry_q = true;
            Boolean retry_p = true;
            _conn.ConnectionString = @"Data Source=172.23.200.71:1521/Orclstg;Persist Security Info=True;User ID=IPTV;Password=IPTV";

            while(true){
                retry_q = true;
                while(retry_q){
                    try
                    {
                        retry_q = false;
                        _conn.Open();
                        Console.WriteLine("Connect to oracle database ....");

                        string _sql_q = @"declare
  v_dequeue_options    dbms_aq.dequeue_options_t;
  v_message_properties dbms_aq.message_properties_t;
  v_message_handle     raw(16);
  purchase_msg         purchase_msg_type;
  v_retry_cnt          number(16);
begin
  dbms_aq.dequeue(queue_name         => 'purchase_msg_queue',
                  dequeue_options    => v_dequeue_options,
                  message_properties => v_message_properties,
                  payload            => purchase_msg,
                  msgid              => v_message_handle);
  :CLIENT_ID :=purchase_msg.client_id;
  :PURCHASE_PK_NO := nvl(purchase_msg.pk_no,0);
  :PURCHASE_NO := purchase_msg.purchase_no;
  :PROCESS_TYPE:= purchase_msg.process_type;
end;";
                        OracleCommand _comm_q = new OracleCommand(_sql_q, _conn);
                        _comm_q.BindByName = true;

                        client_id = "";
                        purchase_pk_no = 0;
                        purchase_no = "";
                        process_type = "";

                        _comm_q.Parameters.Add("CLIENT_ID", OracleDbType.Char, 64, client_id, ParameterDirection.Output);
                        _comm_q.Parameters.Add("PURCHASE_PK_NO", OracleDbType.Decimal, 16, purchase_pk_no, ParameterDirection.Output);
                        _comm_q.Parameters.Add("PURCHASE_NO", OracleDbType.Char, 64, purchase_no, ParameterDirection.Output);
                        _comm_q.Parameters.Add("PROCESS_TYPE", OracleDbType.Char, 32, process_type, ParameterDirection.Output);

                        _comm_q.ExecuteNonQuery();
                        client_id = Convert.ToString(_comm_q.Parameters["CLIENT_ID"].Value).Trim();
                        purchase_no = Convert.ToString(_comm_q.Parameters["PURCHASE_NO"].Value).Trim();
                        process_type = Convert.ToString(_comm_q.Parameters["PROCESS_TYPE"].Value).Trim();
                        purchase_pk_no = Convert.ToDecimal(_comm_q.Parameters["PURCHASE_PK_NO"].Value.ToString());
                        Console.WriteLine("Recevre " + process_type);
                        Console.WriteLine("Client ID " + client_id);
                        Console.WriteLine("Purchase NO " + purchase_no);
                        retry_q = false;
                        _comm_q.Dispose();

                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        if (ex.Message.IndexOf("ORA-0406") >= 0)
                        {

                            Console.WriteLine("Wait 1 min Retry again ...");
                            retry_q = true;
                        }
                        Thread.Sleep(60000);

                    }
                    finally
                    {
                        _conn.Close();
                    }
                }

                retry_p = true;
                while(retry_p){
                    retry_p = false;
                    if (process_type != "")
                    {
                        try
                        {

                            _conn.Open();
                            string _sql_p = @"declare
  -- Non-scalar parameters require additional processing 
  purchase_msg purchase_msg_type;
begin
  purchase_msg := new purchase_msg_type(:CLIENT_ID,:PURCHASE_PK_NO,:PURCHASE_NO,:PROCESS_TYPE);
  purchase_callback_procedure_bk(purchase_msg => purchase_msg);
end;";
                            OracleCommand _comm_p = new OracleCommand(_sql_p, _conn);
                            if (purchase_pk_no == null) { purchase_pk_no = 0; }
                            _comm_p.BindByName = true;

                            _comm_p.Parameters.Add("CLIENT_ID",  client_id);
                            _comm_p.Parameters.Add("PURCHASE_PK_NO", OracleDbType.Decimal, 16, purchase_pk_no, ParameterDirection.Input);
                            _comm_p.Parameters.Add("PURCHASE_NO", purchase_no);
                            _comm_p.Parameters.Add("PROCESS_TYPE",  process_type);

                            _comm_p.ExecuteNonQuery();
                            Console.WriteLine("Sucess");
                            retry_p = false;
                            _comm_p.Dispose();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                            if (ex.Message.IndexOf("ORA-0406") >= 0
                            || ex.Message.IndexOf("ORA-06") >= 0)
                            {
                                Console.WriteLine("Wait 1 min Retry again ...");
                                Thread.Sleep(60000);
                                retry_p = true;
                            }
                        }
                        finally
                        {
                            _conn.Close();
                        }

                    }
                }
            }
          
        }
    }
}

    

