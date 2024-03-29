﻿using System;
using System.Data;
using System.Threading;
using Oracle.ManagedDataAccess.Client;
using NDesk.Options;
namespace Oracle_Process
{
    class Program
    {
        static void Main(string[] args)
        {
            OracleConnection _conn = new OracleConnection();
            string datasource = "da-oracledb.svc.litv.tv:1521/orcl";
            string userid = "iptv";
            string password = "iptv";
            string process_type = "";
            string client_id = "";
            string purchase_no = "";
            string queue_name = "purchase_msg_queue";
            decimal? purchase_pk_no = 0;
            Boolean retry_q = true;
            Boolean retry_p = true;
            string account_center_url = "https://dev-setaccinfo.svc.litv.tv/lambda/SaveAccountInfo";
            account_center_mapper ac = new account_center_mapper(account_center_url);
            OptionSet _p = new OptionSet()
            {
                {"db|DataSource=","the DataSource is {DATASOURCE}",
       db => datasource = db  },
       {"u|Userid=","the UserId is {USERID}",
       u => userid = u  },
       {"p|Password=","the DataSource is {Password}",
       p => password = p  },
       {"q|queue_name=","the Queue is {Queue_Name}",
       q => queue_name = q  },
       {"acc_url|account_url=","the Account center url is {account_center_url}",
       acc_url => account_center_url = acc_url  }
            };
            _p.Parse(args);

            

            // _conn.ConnectionString = @"Data Source=172.23.200.71:1521/orclstg " + ";Persist Security Info=True;User ID=iptv;Password=iptv";

            while (true){
                retry_q = true;
                while(retry_q){
                    try
                    {
                        retry_q = false;
                        _conn = new OracleConnection();
                        _conn.ConnectionString = @"Data Source=" + datasource + ";Persist Security Info=True;User ID=" + userid + ";Password=" + password;
                        _conn.Open();
                        Console.WriteLine(DateTime.Now.ToString() + " " + "Connect to oracle database ....");

                        string _sql_q = @"declare
  v_dequeue_options    dbms_aq.dequeue_options_t;
  v_message_properties dbms_aq.message_properties_t;
  v_message_handle     raw(16);
  purchase_msg         purchase_msg_type;
  v_retry_cnt          number(16);
begin
v_dequeue_options.wait :=20;
v_dequeue_options.navigation := DBMS_AQ.FIRST_MESSAGE;
v_dequeue_options.dequeue_mode:=dbms_aq.remove;
  dbms_aq.dequeue(queue_name         => '" + queue_name+@"',
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
                        _comm_q.CommandTimeout = 30;
                        _comm_q.BindByName = true;

                        client_id = "";
                        purchase_pk_no = 0;
                        purchase_no = "";
                        process_type = "";

                        _comm_q.Parameters.Add("CLIENT_ID", OracleDbType.Char, 64, client_id, ParameterDirection.Output);
                        _comm_q.Parameters.Add("PURCHASE_PK_NO", OracleDbType.Decimal, 16, purchase_pk_no, ParameterDirection.Output);
                        _comm_q.Parameters.Add("PURCHASE_NO", OracleDbType.Char, 64, purchase_no, ParameterDirection.Output);
                        _comm_q.Parameters.Add("PROCESS_TYPE", OracleDbType.Char, 32, process_type, ParameterDirection.Output);

                        try
                        {
                            _comm_q.ExecuteNonQuery();
                            client_id = Convert.ToString(_comm_q.Parameters["CLIENT_ID"].Value).Trim();
                            purchase_no = Convert.ToString(_comm_q.Parameters["PURCHASE_NO"].Value).Trim();
                            process_type = Convert.ToString(_comm_q.Parameters["PROCESS_TYPE"].Value).Trim();
                            purchase_pk_no = Convert.ToDecimal(_comm_q.Parameters["PURCHASE_PK_NO"].Value.ToString());
                            Console.WriteLine(DateTime.Now.ToString() + " " + "Recevre " + process_type);
                            Console.WriteLine(DateTime.Now.ToString() + " " + "Client ID " + client_id);
                            Console.WriteLine(DateTime.Now.ToString() + " " + "Purchase NO " + purchase_no);
                            retry_q = false;
                        }
                        catch (Exception ex)
                        {
                            if (ex.Message.IndexOf("ORA-025") < 0)
                            {
                                Console.WriteLine(DateTime.Now.ToString() + " " + ex.Message);
                            }
                           
                            _conn.Close();
                            _conn.Dispose();
                            if (ex.Message.IndexOf("ORA-0406") >= 0)
                            {
                                Console.WriteLine(DateTime.Now.ToString() + " " + "Wait30 sec Retry again ...");
                                retry_q = true;
                                Thread.Sleep(30000);
                            }
                        }
                        finally
                        {
                            _comm_q.Dispose();
                        }

                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(DateTime.Now.ToString()+" "+ex.Message);
                      
                        _conn.Close();
                        if (ex.Message.IndexOf("ORA-0406") >= 0)
                        {
                            Console.WriteLine(DateTime.Now.ToString() + " "+"Wait30 sec Retry again ...");
                            retry_q = true;
                            Thread.Sleep(30000);
                        }
                    }
                    finally
                    {
                        _conn.Close();
                        _conn.Dispose();
                    }
                }

                retry_p = true;
                while(retry_p){
                    retry_p = false;
                    if (process_type != "")
                    {
                        try
                        {
                            _conn = new OracleConnection();
                            _conn.ConnectionString = @"Data Source=" + datasource + ";Persist Security Info=True;User ID=" + userid + ";Password=" + password;

                            _conn.Open();
                            string _sql_p = @"declare
  -- Non-scalar parameters require additional processing 
  purchase_msg purchase_msg_type;
begin
  purchase_msg := new purchase_msg_type(:CLIENT_ID,:PURCHASE_PK_NO,:PURCHASE_NO,:PROCESS_TYPE);
  purchase_callback_procedure(purchase_msg => purchase_msg);
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
                                Console.WriteLine("Wait 30 sec Retry again ...");
                                Thread.Sleep(30000);
                                retry_p = true;
                            }
                        }
                        finally
                        {
                            Console.WriteLine(ac.set_account_center(client_id, _conn));

                            _conn.Close();
                            _conn.Dispose();
                        }

                    }
                }
            }
          
        }
    }
}

    

