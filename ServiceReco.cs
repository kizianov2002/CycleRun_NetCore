using System;
using System.IO;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using CycleRun_NetCore.SqlConn;
using CarModelSdk_NetCore;
using System.Collections.Generic;
using System.Linq;

namespace CycleRun_NetCore
{
    class ServiceReco
    {
        public static MyParams myParams;

        static SqlConnection conn;

        public static void Log(string text, int level=2)
        {
            Int32.TryParse(myParams.Value("LogToDisk"), out int logToDisk);
            Int32.TryParse(myParams.Value("LogLevel"), out int logLevel);

            if (level <= logLevel)
            {
                string str = " - " + (text.Length > 1 ? (DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " - " + text) : "");
                if (logToDisk > 0)
                {
                    TextWriter writer;

                    if (File.Exists("LOG.txt"))
                        writer = File.AppendText("LOG.txt");
                    else
                        writer = File.CreateText("LOG.txt");

                    writer.WriteLine(str);
                    writer.Close();
                }
                Console.WriteLine(str);
            }
        }

        public static void Pause(int msec)
        {

            {
                Int32.TryParse(myParams.Value("UsePauses"), out int usePauses);

                if (usePauses > 0)
                {
                    Log("PAUSE  " + msec + "  мс. ", 1);
                    System.Threading.Thread.Sleep(msec);
                }
            }
        }

        public static DateTime StringToDate000(string str)
        {
            int yer = Int32.Parse(str.Substring(0, 4));
            int mon = Int32.Parse(str.Substring(5, 2));
            int day = Int32.Parse(str.Substring(8, 2));

            return new DateTime(yer, mon, day, 0, 0, 0);
        }

        public static DateTime StringToDateTime(string str)
        {
            int yer = Int32.Parse(str.Substring(0, 4));
            int mon = Int32.Parse(str.Substring(5, 2));
            int day = Int32.Parse(str.Substring(8, 2));

            int hor = Int32.Parse(str.Substring(11, 2));
            int min = Int32.Parse(str.Substring(14, 2));
            int sec = Int32.Parse(str.Substring(17, 2));

            return new DateTime(yer, mon, day, hor, min, sec);
        }

        public static string DateTimeToString(DateTime dt)
        {
            return dt.ToString("yyyy-MM-dd HH:mm:ss");
        }


        public ServiceReco(MyParams mpr)
        {
            myParams = mpr;

            Log(" ", 1);
            Log("START PROGRAM ", 0);
            Log(" ", 0);

            RestoreConnection();
        }

        public static void  RestoreConnection()
        {
            Int32.TryParse(myParams.Value("MarkExceptions"), out int markExceptions);

            conn = DBUtils.GetDBConnection();
            try
            {
                conn.Open();

                if (conn.State == System.Data.ConnectionState.Open)
                {
                    Log(new string("Connection OK"), 0);
                }
                else
                    Log(new string("Connection is not established!"), 0);
            }
            catch (Exception e)
            {
                if (markExceptions > 0)  Console.WriteLine(new string("Connection Error: " + e));
                if (markExceptions > 1)  Console.WriteLine(" -> -> " + e.StackTrace);
            }
            finally
            {
                //Console.WriteLine(" ");
            }
        }

        public static string readParamFromDB(string valName, string valDefault)
        {
            Log("readParamFromDB: " + valName + " (default " + valDefault + ")", 1);

            Int32.TryParse(myParams.Value("MarkExceptions"), out int markExceptions);

            if (conn == null)
                RestoreConnection();

            string valValue = valDefault;   // "01.01.2020 00:00:01";

            try
            {
                if (conn == null)
                    RestoreConnection();

                string sql = myParams.Value("PARAMS_SqlRead");
                sql = sql
                    .Replace("##Name##", valName);

                //Console.WriteLine(sql);
                //Console.ReadLine();

                // Создать объект Command
                SqlCommand cmd = new SqlCommand
                {
                    Connection = conn,
                    CommandText = sql,
                    CommandTimeout = 1000
                };

                using (DbDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.HasRows)
                    {
                        if (reader.Read())
                        {
                            valValue = reader.GetString(0);
                        }
                        break;
                    }
                }
            }
            catch (Exception e)
            {
                if (markExceptions > 0)  Log(new string("Reading '" + valName + "' from [dbo].[TSInfo_RecoStatus] error: " + e));
            }
            finally
            {
                //Console.WriteLine("finally");
                // Закрыть соединение.
            }

            Log("Read param from DB: " + valName + " >> '" + valValue + "', from table [dbo].[TSInfo_RecoStatus]" + " - OK", 2);
            return valValue;
        }

        public static int saveParamToDB(string valName, string valValue)
        {
            Log("saveParamToDB: " + valName + " = " + valValue, 1);

            Int32.TryParse(myParams.Value("MarkExceptions"), out int markExceptions);

            if (conn == null)
                RestoreConnection();

            string sql = myParams.Value("PARAMS_SqlSave");
            sql = sql
                .Replace("##Name##", valName)
                .Replace("##Value##", valValue);

            //Console.WriteLine(sql);
            //Console.ReadLine();

            int resIns = -1;

            Log("sql: " + sql, 2);

            try
            {
                if (conn == null)
                    RestoreConnection();

                SqlCommand command = new SqlCommand(null, conn);
                command.CommandText = sql;

                command.Prepare();
                resIns = command.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                if (markExceptions > 0)  Console.WriteLine(new string("Saving '" + valName + "' = '" + valValue + "' to [dbo].[TSInfo_RecoStatus] error: " + e));
                if (markExceptions > 1)  Console.WriteLine(" -> -> " + e.StackTrace);
            }
            finally
            {
                //Console.WriteLine(" -> -> finally "); 
            }

            Log("Save param to DB: " + valName + " << '" + valValue + "', in table [dbo].[TSInfo_RecoStatus]" + " - OK", 1);

            return resIns;
        }

        private int updateParamInDB(string valName, string valValue)
        {
            Log("updateParamInDB: " + valName + " = " + valValue, 1);

            Int32.TryParse(myParams.Value("MarkExceptions"), out int markExceptions);

            if (conn == null)
                RestoreConnection();

            string sql = myParams.Value("PARAMS_SqlUpdate");
            sql = sql
                .Replace("##Name##", valName)
                .Replace("##Value##", valValue);

            int resIns = -1;

            try
            {
                if (conn == null)
                    RestoreConnection();

                SqlCommand command = new SqlCommand(null, conn);
                command.CommandText = sql;

                command.Prepare();
                resIns = command.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                if (markExceptions > 0)  Log(new string("Saving '" + valName + "' = '" + valValue + "' to [dbo].[TSInfo_RecoStatus] error: " + e));
                if (markExceptions > 1)  Log(" -> -> " + e.StackTrace);
            }
            finally
            {
                //Console.WriteLine(" -> -> finally "); 
            }

            Log("Save param to DB: " + valName + " << '" + valValue + "'", 2);

            return resIns;
        }

        public static int renameParamInDB(string oldName, string newName)
        {
            Log("renameParamInDB: " + oldName + " -> " + newName, 1);

            Int32.TryParse(myParams.Value("MarkExceptions"), out int markExceptions);

            if (conn == null)
                RestoreConnection();

            string sql = myParams.Value("PARAMS_SqlRename");
            sql = sql
                .Replace("##OldName##", oldName)
                .Replace("##NewName##", newName);

            //Console.WriteLine(sql);
            //Console.ReadLine();

            int resIns = -1;

            Log("sql: " + sql, 1);
            //Console.ReadLine();

            try
            {
                if (conn == null)
                    RestoreConnection();

                SqlCommand command = new SqlCommand(null, conn);
                command.CommandText = sql;

                command.Prepare();
                resIns = command.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                if (markExceptions > 0)  Console.WriteLine(new string("Renaming param '" + oldName + "' to '" + newName + "' in table [dbo].[TSInfo_RecoStatus] error: " + e));
                if (markExceptions > 1)  Console.WriteLine(" -> -> " + e.StackTrace);
            }
            finally
            {
                //Console.WriteLine(" -> -> finally "); 
            }

            Log("Rename param '" + oldName + "' to '" + newName + "' in table [dbo].[TSInfo_RecoStatus]" + " - OK", 1);

            return resIns;
        }

        public static int cleanTempParamNamesInDB()
        {
            Log("cleanTempParamNamesInDB ", 1);

            Int32.TryParse(myParams.Value("MarkExceptions"), out int markExceptions);

            if (conn == null)
                RestoreConnection();

            string sql = myParams.Value("PARAMS_SqlCleanTemp");

            //Console.WriteLine(sql);
            //Console.ReadLine();

            int resIns = -1;

            Log("sql: " + sql, 1);
            //Console.ReadLine();

            try
            {
                if (conn == null)
                    RestoreConnection();

                SqlCommand command = new SqlCommand(null, conn);
                command.CommandText = sql;

                command.Prepare();
                resIns = command.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                if (markExceptions > 0)  Console.WriteLine(new string("Cleaning temporary names in table [dbo].[TSInfo_RecoStatus] error: " + e));
                if (markExceptions > 1)  Console.WriteLine(" -> -> " + e.StackTrace);
            }
            finally
            {
                //Console.WriteLine(" -> -> finally "); 
            }

            Log("Cleaning temporary names in table [dbo].[TSInfo_RecoStatus]" + " - OK", 1);

            return resIns;
        }

        private int deleteParamFromDB(string valName)
        {
            Log("deleteParamFromDB: " + valName, 1);

            Int32.TryParse(myParams.Value("MarkExceptions"), out int markExceptions);

            if (conn == null)
                RestoreConnection();

            string sql = myParams.Value("PARAMS_SqlDelete");
            sql = sql
                .Replace("##Name##", valName);

            //Console.WriteLine(sql);
            //Console.ReadLine();

            int resIns = -1;

            Log("sql: " + sql, 1);
            //Console.ReadLine()/*;*/

            try
            {
                if (conn == null)
                    RestoreConnection();

                SqlCommand command = new SqlCommand(null, conn);
                command.CommandText = sql;

                command.Prepare();
                resIns = command.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                if (markExceptions > 0)  Console.WriteLine(new string("Deleting param '" + valName + "' from [dbo].[TSInfo_RecoStatus] error: " + e));
                if (markExceptions > 1)  Console.WriteLine(" -> -> " + e.StackTrace);
            }
            finally
            {
                //Console.WriteLine(" -> -> finally "); 
            }

            //Log("Delete param from DB: " + valName + " - OK", 1);
            //Console.ReadLine();

            return resIns;
        }


        private int updateLastDate(string newLastDate)
        {
            string curLastDate = readParamFromDB("LastDate", "");

            if (newLastDate != curLastDate)
                saveParamToDB("LastDate " + DateTime.Now.ToString("yyyy-MM-dd HH:mm"), curLastDate);

            return saveParamToDB("LastDate", newLastDate);
        }


        public static long TestPhotosCount()
        {
            Int32.TryParse(myParams.Value("MarkExceptions"), out int markExceptions);
            long lastId = Int64.Parse(readParamFromDB("LastID", "0"));
            string dateTimeFrom = readParamFromDB("DateTimeFrom", "2020-01-01 00:00:00");
            string dateTimeTo = readParamFromDB("DateTimeTo", "2020-01-01 23:59:59");

            Log("TestPhotosCount ( " + lastId + ", " + dateTimeFrom + ", " + dateTimeTo + ",  )", 2);

            long cnt = 0;

            if (conn == null)
                RestoreConnection();

            try
            {
                string sqlTestIDs = myParams.Value("SqlTestIDs");

                sqlTestIDs = sqlTestIDs
                    .Replace("##dateTimeFrom##", dateTimeFrom)
                    .Replace("##dateTimeTo##", dateTimeTo)
                    .Replace("##lastId##", lastId.ToString());

                Log(sqlTestIDs, 2);

                // Создать объект Command
                SqlCommand cmd = new SqlCommand
                {
                    Connection = conn,
                    CommandText = sqlTestIDs,
                    CommandTimeout = 10000
                };

                using (DbDataReader reader = cmd.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        if (reader.Read())
                        {
                            cnt = reader.GetInt32(0);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                if (markExceptions > 0)  Console.WriteLine(new string("Reading count unreaded images from DB Error: " + e));
                if (markExceptions > 1)  Console.WriteLine(" -> -> " + e.StackTrace);
                cnt = 0;
            }
            finally
            {
                // Закрыть соединение.
                //conn.Close();
            }

            return cnt;
        }


        private DateTime getNextDateTime(string curDateString, out DateTime strtDT, out DateTime stopDT, out bool isToday)
        {
            //Console.WriteLine("getNextDateTime (" + curDateString + ")");

            Int32.TryParse(myParams.Value("UseDarkFilter"), out int useDarkFilter);
            Int32.TryParse(myParams.Value("Latitude_deg"), out int latitude_deg);
            Int32.TryParse(myParams.Value("OffsetDayTime_min"), out int offsetDayTime_min);
            Int32.TryParse(myParams.Value("OffsetTimeZone_min"), out int offsetTimeZone_min);
            Int32.TryParse(myParams.Value("TodayTimeLag_ms"), out int todayTimeLag_ms);

            Int32.TryParse(myParams.Value("ForseToday"), out int forseToday);

            var todayDT = DateTime.Today;
            strtDT = StringToDate000(curDateString);
            isToday = strtDT >= todayDT;

            // если уже начался новый световой день - форсим переход на текущую дату
            if ((strtDT > todayDT)
                || (forseToday > 0 && strtDT < todayDT))
            {
                // получим время рассвета для текущей даты
                int today_yer = todayDT.Year;
                int today_mon = todayDT.Month;
                int today_day = todayDT.Day;

                int todaySunrize_mins = CarModelSdk.CalcSunrize_mins_full(todayDT.Month, todayDT.Day, latitude_deg, offsetDayTime_min, offsetTimeZone_min);

                int todaySunrize_hr = todaySunrize_mins / 60;
                int todaySunrize_mn = todaySunrize_mins % 60;
                int todaySunrize_sc = 0;
                var todaySunrizeDT = new DateTime(today_yer,
                                                  today_mon,
                                                  today_day,
                                                  todaySunrize_hr,
                                                  todaySunrize_mn,
                                                  todaySunrize_sc);

                // форсим переход на текущую дату
                if (DateTime.Now >= todaySunrizeDT)
                {
                    strtDT = todayDT;

                    int res = updateLastDate(todayDT.ToString("yyyy-MM-dd"));
                    //int res = updateParamInDB("LastDate", todayDT.ToString("yyyy-MM-dd"));
                    int res1 = updateParamInDB("LastID", "0");
                    Log("FORSE TODAY:  todayDT = " + todayDT.ToString("yyyy-MM-dd HH:mm:ss"), 1);
                }
            }

            int yer = strtDT.Year;
            int mon = strtDT.Month;
            int day = strtDT.Day;

            stopDT = strtDT.AddDays(1);

            // если активен фильтр по тёмному времени суток - поправим границы диапазона
            if (useDarkFilter > 0)
            {
                // получим время рассвета
                int sunrize_mins = CarModelSdk.CalcSunrize_mins_full(mon, day, latitude_deg, offsetDayTime_min, offsetTimeZone_min);
                int sunrize_hr = sunrize_mins / 60;
                int sunrize_mn = sunrize_mins % 60;
                int sunrize_sc = 0;

                strtDT = new DateTime(strtDT.Year, strtDT.Month, strtDT.Day, sunrize_hr, sunrize_mn, sunrize_sc);

                // получим время заката
                int sunset_mins = CarModelSdk.CalcSunset_mins_full(mon, day, latitude_deg, offsetDayTime_min, offsetTimeZone_min);
                if (sunset_mins < 1440)
                {
                    int sunset_hr = sunset_mins / 60;
                    int sunset_mn = sunset_mins % 60;
                    int sunset_sc = 0;
                    stopDT = new DateTime(strtDT.Year, strtDT.Month, strtDT.Day, sunset_hr, sunset_mn, sunset_sc);
                }
                else
                {
                    stopDT = new DateTime(stopDT.Year, stopDT.Month, stopDT.Day, 0, 0, 0);
                }
                //Console.WriteLine("stopDT = " + stopDT.ToString("yyyy-MM-dd HH:mm:ss"));
            }

            int resFrom = updateParamInDB("DateTimeFrom", strtDT.ToString("yyyy-MM-dd HH:mm:ss"));
            int resTo = updateParamInDB("DateTimeTo", stopDT.ToString("yyyy-MM-dd HH:mm:ss"));

            return stopDT;
        }


        public bool OpenNewIDsSpans(int procCount, ref List<string> procNames)
        {
            // Console.WriteLine("OpenNewIDsSpans()");

            Int32.TryParse(myParams.Value("MarkIDsSpans"), out int markIDsSpans);
            Int32.TryParse(myParams.Value("MarkExceptions"), out int markExceptions);

            string procHubName = myParams.Value("ProcHubName");
            Int32.TryParse(myParams.Value("UseMultyComp"), out int useMultyComp);
            Int32.TryParse(myParams.Value("IDsProcSpanLim"), out int idsProcSpanLim);
            Int32.TryParse(myParams.Value("IDsBathSize"), out int idsBathSize);
            Int32.TryParse(myParams.Value("IDsProcSpanLim_today"), out int idsProcSpanLim_today);
            Int32.TryParse(myParams.Value("IDsBathSize_today"), out int idsBathSize_today);
            Int32.TryParse(myParams.Value("TodayTimeLag_ms"), out int todayTimeLag_ms);
            DateTime lastDT = StringToDate000(readParamFromDB("LastDate", "2020-01-01"));

            procNames.Clear();
            List<SpanRec> oldList = new List<SpanRec>();

            try
            {
                if (conn == null)
                    RestoreConnection();

                DateTime dtLimit = DateTime.Now.AddDays(-1);

                string sql = myParams.Value("PARAMS_SqlIDsSpans");
                sql = sql.TrimStart(' ', '(');
                sql = sql.TrimEnd(' ', ')');
                sql = sql
                    .Replace("##AddWhere##", (useMultyComp > 0 ? " and [Value]<='" + dtLimit.ToString("yyyy-MM-dd HH:mm:ss") + "'" : ""));

                // Создать объект Command
                SqlCommand cmd = new SqlCommand
                {
                    Connection = conn,
                    CommandText = sql,
                    CommandTimeout = 1000
                };

                oldList.Clear();

                using (DbDataReader reader = cmd.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            SpanRec rec = new SpanRec()
                            {
                                Id = reader.GetInt32(0),
                                Code = reader.GetString(1),
                                ID_From = reader.GetString(2),
                                ID_To = reader.GetString(3),
                                DT_From = reader.GetString(4),
                                DT_To = reader.GetString(5),
                                OpenDT = DateTime.Parse(reader.GetString(6))
                            };
                            oldList.Add(rec);
                        }
                    }
                    reader.Close();
                }
            }
            catch (Exception e)
            {
                if (markExceptions > 0)  Console.WriteLine(new string ("Reading IDs from DB Error: " + e));
            }
            finally
            {
                // Закрыть соединение.
                //conn.Close();
            }


            // разметим на процессы те диапазоны, что уже есть
            if (markIDsSpans>0)  Console.WriteLine("Present spans using:");
            for (int i=0; i<oldList.Count && i<procCount; i++)
            {
                procNames.Add(oldList[i].Code);
                saveParamToDB(oldList[i].Code + "_Span", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));

                if (markIDsSpans > 0) Console.WriteLine(" - " + i + ") " + oldList[i].Code);
            }

            bool noData = false;

            // сформируем недостающие диапазоны
            DateTime dt = DateTime.Now;
            if (markIDsSpans > 0)  Console.WriteLine("New spans creating:");
            for (int i = procNames.Count; i < procCount; i++)
            {
                // проверка числа необработанных записей
                long lastId = Int64.Parse(readParamFromDB("LastID", "0"));
                lastDT = StringToDate000(readParamFromDB("LastDate", "2020-01-01"));
                string dateTimeFrom = readParamFromDB("DateTimeFrom", "2020-01-01 00:00:00");
                string dateTimeTo = readParamFromDB("DateTimeTo", "2020-01-01 23:59:59");

                long cntRecs = 0;
                bool isToday = lastDT == DateTime.Today;

                while (cntRecs < 1)
                {
                    cntRecs = TestPhotosCount();

                    if (isToday 
                        && cntRecs < idsBathSize)
                    {
                        noData = true;
                        break;
                    }

                    if (cntRecs < 1)
                    {
                        // переход на новый день
                        lastDT = lastDT.AddDays(1);
                        isToday = lastDT == DateTime.Today;

                        updateLastDate(lastDT.ToString("yyyy-MM-dd"));
                        //updateParamInDB("LastDate", lastDT.ToString("yyyy-MM-dd"));
                        updateParamInDB("DateTimeFrom", "2020-01-01 00:00:00");
                        updateParamInDB("DateTimeTo", "2020-01-01 23:59:59");
                        updateParamInDB("LastID", "0");
                    }
                }

                if (noData)
                    break;

                // перечитаем из базы последнее значение LastDate
                lastDT = StringToDate000(readParamFromDB("LastDate", "2020-01-01"));

                // создадим новый диапазон IDs
                int n = (dt.Minute + dt.Millisecond + i) % 1000;
                var Code = procHubName + n.ToString();
                procNames.Add(Code);

                if (markIDsSpans > 0)  Console.WriteLine(" - " + i + ") " + Code);

                DateTime nextDT = getNextDateTime(lastDT.ToString("yyyy-MM-dd"), out DateTime strtDT, out DateTime stopDT, out isToday);
                lastId = Int64.Parse(readParamFromDB("LastID", "0"));

                string dateTimeFrom_res = DateTimeToString(strtDT);
                string dateTimeTo_res = DateTimeToString(stopDT);

                // особая обработка текущего дня
                if (isToday)
                {
                    if (stopDT > DateTime.Now.AddSeconds(-todayTimeLag_ms / 1000))
                    {
                        stopDT = DateTime.Now.AddSeconds(-todayTimeLag_ms / 1000);
                        Log("isToday = " + isToday + ", stopDateTime = " + DateTimeToString(stopDT), 1);
                    }
                }

                if (conn == null)
                    RestoreConnection();

                long idFrom = -1;
                long idTo = -1;

                try
                {
                    Log("[[ PREPARE ... ]]   time interval   " + DateTimeToString(stopDT) + " - " + DateTimeToString(stopDT), 2);

                    string sqlReadIDs = myParams.Value("SqlReadIDs");

                    string sqlReadIDs_res = sqlReadIDs
                        .Replace("##idsBathSize##", isToday ? idsProcSpanLim_today.ToString() : idsProcSpanLim.ToString())
                        .Replace("##lastId##", lastId.ToString())
                        .Replace("##dateTimeFrom##", DateTimeToString(strtDT))
                        .Replace("##dateTimeTo##", DateTimeToString(stopDT));

                    Log(sqlReadIDs_res, 2);

                    // Создать объект Command
                    SqlCommand cmd = new SqlCommand
                    {
                        Connection = conn,
                        CommandText = sqlReadIDs_res,
                        CommandTimeout = 10000
                    };

                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                idTo = reader.GetInt64("TargetInfoID");

                                if (idFrom < 0) idFrom = idTo;
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    if (markExceptions > 0)  Console.WriteLine(new string("Reading IDs from DB Error: " + e));
                    return false;
                }
                finally
                {
                    // Закрыть соединение.
                    //conn.Close();
                }

                //сохраним диапазон IDs в базу
                // Console.WriteLine(Code + "_Span = " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
                // Console.WriteLine(Code + "_ID_From = " + idFrom.ToString());
                // Console.WriteLine(Code + "_ID_To = " + idTo.ToString());
                // Console.WriteLine(Code + "_DateTime_From = " + dateTimeFrom_res);
                // Console.WriteLine(Code + "_DateTime_To = " + dateTimeTo_res);

                // Console.WriteLine("LastDate = " + strtDT.ToString("yyyy-MM-dd"));
                // Console.WriteLine("LastID = " + idTo.ToString());
                // Console.WriteLine("DateTimeFrom = " + dateTimeFrom_res);
                // Console.WriteLine("DateTimeTo = " + dateTimeTo_res);
                // Console.ReadLine();

                if (idTo < 1)
                    break;

                int resSpan = saveParamToDB(Code + "_Span", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
                int resIdFr = saveParamToDB(Code + "_ID_From", idFrom.ToString());
                int resIdTo = saveParamToDB(Code + "_ID_To", idTo.ToString());
                int resDtFr = saveParamToDB(Code + "_DateTime_From", dateTimeFrom_res);
                int resDtTo = saveParamToDB(Code + "_DateTime_To", dateTimeTo_res);

                int resLsDT = updateLastDate(strtDT.ToString("yyyy-MM-dd"));
                //int resLsDT = updateParamInDB("LastDate", strtDT.ToString("yyyy-MM-dd"));
                int resLsID = updateParamInDB("LastID", idTo.ToString());
                int resLsFr = updateParamInDB("DateTimeFrom", dateTimeFrom_res);
                int resLsTo = updateParamInDB("DateTimeTo", dateTimeTo_res);
            }
           // Console.ReadLine();

            return true;
        }


        private class SpanRec
        {
            public long Id;
            public string Code;
            public string ID_From;
            public string ID_To;
            public string DT_From;
            public string DT_To;
            public DateTime OpenDT;

            public override string ToString()
            {
                string str =
                    "Id = " + Id + "\n" +
                    "Code = " + Code + "\n" +
                    "ID_From = " + ID_From + "\n" +
                    "ID_To = " + ID_To + "\n" +
                    "DT_From = " + DT_From + "\n" +
                    "DT_To = " + DT_To + "\n";
                return str;
            }
        }

        public bool CloseOldIDsSpan(string procName)
        {
            //Console.WriteLine("CloseOldIDsSpan (" + procName + ")");

            deleteParamFromDB(procName + "_Span");
            deleteParamFromDB(procName + "_DateTime_From");
            deleteParamFromDB(procName + "_DateTime_To");
            deleteParamFromDB(procName + "_ID_From");
            deleteParamFromDB(procName + "_ID_To");
            return true;
        }

    }
}
