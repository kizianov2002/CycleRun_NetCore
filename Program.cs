using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace CycleRun_NetCore
{
    class Program
    {
        static void Main(string[] args)
        {
            MyParams myParams = new MyParams();
            myParams.Print();

            ServiceReco service = new ServiceReco(myParams);

            Int32.TryParse(myParams.Value("UseMultyProc"), out int useMultyProc);
            Int32.TryParse(myParams.Value("ProcCount"), out int procCount);
            string procHubName = myParams.Value("ProcHubName");
            if (procCount < 1) procCount = 1;
            if (procCount > 20) procCount = 20;

            Int32.TryParse(myParams.Value("TodayDoIntime"), out int todayDoIntime);
            Int32.TryParse(myParams.Value("TodayDoInNight"), out int todayDoInNight);

            Int32.TryParse(myParams.Value("MarkStatistic"), out int markStatistic);

            Int32.TryParse(myParams.Value("IDsProcSpanLim"), out int idsProcSpanLim);
            Int32.TryParse(myParams.Value("IDsBathSize"), out int idsBathSize);
            Int32.TryParse(myParams.Value("IDsProcSpanLim_today"), out int idsProcSpanLim_today);
            Int32.TryParse(myParams.Value("IDsBathSize_today"), out int idsBathSize_today);
            Int32.TryParse(myParams.Value("IDsSaveStep"), out int idsSaveStep);

            Int32.TryParse(myParams.Value("UsePauses"), out int usePauses);
            Int32.TryParse(myParams.Value("OneFilePause_ms"), out int oneFilePause_ms);
            Int32.TryParse(myParams.Value("IDsBathPause_ms"), out int idsBathPause_ms);
            Int32.TryParse(myParams.Value("IDsSpanPause_ms"), out int idsSpanPause_ms);
            Int32.TryParse(myParams.Value("NoFilesPause_ms"), out int noFilesPause_ms);

            Int32.TryParse(myParams.Value("UseCycle"), out int useCycle);

            {
                ServiceReco.Log(" ", 0);
                string curDir = Environment.CurrentDirectory;
                Int32.TryParse(myParams.Value("LogLevel"), out int logLevel);
                Int32.TryParse(myParams.Value("LogToDisk"), out int logToDisk);
                ServiceReco.Log("CURRENT DIRECTORY = " + curDir, 0);
                ServiceReco.Log("LogLevel = " + logLevel + " (0-1-2)", 0);
                ServiceReco.Log("LogToDisk = " + logToDisk, 0);

                ServiceReco.Log(" ", 0);
                ServiceReco.Log("UseMultyProc = " + useMultyProc, 0);
                ServiceReco.Log("ProcCount  = " + procCount, 1);
                ServiceReco.Log("ProcHubName  = '" + procHubName + "'", 0);

                ServiceReco.Log(" ", 0);
                Int32.TryParse(myParams.Value("ForseToday"), out int forseToday);
                Int32.TryParse(myParams.Value("RecognizeMode"), out int mode);
                Int32.TryParse(myParams.Value("RecognizeBorder"), out int recognizeBorder);
                ServiceReco.Log("UseCycle = " + useCycle, 0);
                ServiceReco.Log("ForseToday  = " + forseToday, 0);
                ServiceReco.Log("Mode = " + mode.ToString() + " (" + ((mode & 1) > 0 ? "Mark" : "-") + ", " + ((mode & 2) > 0 ? "Type" : "-") + ")", 0);
                ServiceReco.Log("RecognizeBorder = " + recognizeBorder, 0);

                ServiceReco.Log(" ", 0);
                ServiceReco.Log("IDsProcSpanLim = " + idsProcSpanLim, 0);
                ServiceReco.Log("IDsBathSize = " + idsBathSize, 0);
                ServiceReco.Log("IDsProcSpanLim_today = " + idsProcSpanLim_today, 0);
                ServiceReco.Log("IDsBathSize_today = " + idsBathSize_today, 0);
                ServiceReco.Log("IDsSaveStep = " + idsSaveStep, 0);

                ServiceReco.Log(" ", 0);
                ServiceReco.Log("WAITING OPTIONS:", 0);
                ServiceReco.Log("UsePauses = " + usePauses, 0);
                if (usePauses > 0)
                {
                    ServiceReco.Log("OneFilePause_ms = " + oneFilePause_ms, 0);
                    ServiceReco.Log("IDsBathPause_ms = " + idsBathPause_ms, 0);
                    ServiceReco.Log("IDsSpanPause_ms = " + idsSpanPause_ms, 0);
                    ServiceReco.Log("NoFilesPause_ms = " + noFilesPause_ms, 0);
                }
                Int32.TryParse(myParams.Value("TodayTimeLag_ms"), out int todayTimeLag_ms);
                ServiceReco.Log("TodayTimeLag_ms = " + todayTimeLag_ms, 0);

                ServiceReco.Log(" ", 0);
                Int32.TryParse(myParams.Value("UseDarkFilter"), out int useDarkFilter);
                ServiceReco.Log("FILTER OPTIONS:", 0);
                ServiceReco.Log("UseDarkFilter = " + useDarkFilter, 0);
                if (useDarkFilter > 0)
                {
                    Int32.TryParse(myParams.Value("Latitude_deg"), out int latitude_deg);
                    Int32.TryParse(myParams.Value("OffsetDayTime_min"), out int offsetDayTime_min);
                    Int32.TryParse(myParams.Value("OffsetTimeZone_min"), out int offsetTimeZone_min);
                    ServiceReco.Log("Latitude_deg = " + latitude_deg, 0);
                    ServiceReco.Log("OffsetDayTime_min = " + offsetDayTime_min, 0);
                    ServiceReco.Log("OffsetTimeZone_min = " + offsetTimeZone_min, 0);
                }

                if (/*useThreads==0*/true)
                {
                    ServiceReco.Log(" ", 0);
                    Int32.TryParse(myParams.Value("FilesToDisk"), out int filesToDisk);
                    ServiceReco.Log("DISK USE OPTIONS:", 0);
                    ServiceReco.Log("FilesToDisk = " + filesToDisk, 0);
                    if (filesToDisk > 0)
                    {
                        string dirIn = myParams.Value("DirIn");
                        string dirOut = myParams.Value("DirOut");
                        Int32.TryParse(myParams.Value("SaveOps_SaveRecs"), out int saveOps_SaveRecs);
                        Int32.TryParse(myParams.Value("SaveOps_SaveUnrecs"), out int saveOps_SaveUnrecs);
                        ServiceReco.Log("DirIn = " + dirIn, 0);
                        ServiceReco.Log("DirOut = " + dirOut, 0);
                        ServiceReco.Log("SaveOps_SaveRecs = " + saveOps_SaveRecs, 0);
                        ServiceReco.Log("SaveOps_SaveUnrecs = " + saveOps_SaveUnrecs, 0);
                    }
                }
                ServiceReco.Log(" ", 0);
            }

            if (useMultyProc < 1)
                procCount = 1;

            do
            {
                var threads = new Thread[procCount];
                Int32.TryParse(myParams.Value("UseCycle"), out useCycle);

                List<string> names = new List<string>();
                service.OpenNewIDsSpans(procCount, ref names);

                if (markStatistic>0)
                    ServiceReco.Log("выбрано"
                      + " - распознано"
                      + " - время (сек)"
                      + " - время (мин)"
                      + " - чтение БД"
                      + " - распознание"
                      + " - простой"
                      , 0);

                for (int i = 0; i < names.Count; i++)
                {
                    threads[i] = new Thread(new ParameterizedThreadStart(Go));
                    threads[i].Start(names[i]);
                    System.Threading.Thread.Sleep(1000);
                }

                for (int i = 0; i < names.Count; i++)
                {
                    threads[i].Join();
                }

                for (int i = 0; i < names.Count; i++)
                {
                    // удаляем отработанную пачку
                    service.CloseOldIDsSpan(names[i]);
                }

                if (usePauses > 0 
                     && idsSpanPause_ms > 0)
                    ServiceReco.Pause(idsSpanPause_ms);

                // проверка числа необработанных записей
                string lastDateTime = ServiceReco.readParamFromDB("LastDate", "2020-01-01");
                long lastId = long.Parse(ServiceReco.readParamFromDB("LastID", "0"));
                string dateTimeFrom = ServiceReco.readParamFromDB("DateTimeFrom", "2020-01-01 00:00:00");
                string dateTimeTo = ServiceReco.readParamFromDB("DateTimeTo", "2020-01-01 23:59:59");
                DateTime lastDT = ServiceReco.StringToDate000(lastDateTime);

                // если текущий день и записей нет - пауза
                bool isToday = (lastDT >= DateTime.Today);
                if (isToday && names.Count <= 1)
                {
                    if (markStatistic > 0)
                        ServiceReco.Log("Date: " + lastDateTime + ",  interval:  " + dateTimeFrom.Substring(11, 8) + "-" + dateTimeTo.Substring(11, 8) + ",  unprocessed photos: -NO DATA-,  LastID: " + lastId, 0);

                    // это текущий день - подождём новых фооток
                    if (usePauses > 0
                         && noFilesPause_ms > 0)
                        ServiceReco.Pause(noFilesPause_ms);

                    // в 20:00 вечерний перезапуск распознания
                    if (todayDoInNight > 0)
                    {
                        DateTime now = DateTime.Now;
                        if (now.Hour == 20)
                            ServiceReco.saveParamToDB("LastID", "0");
                    }
                }
                else
                {
                    if (markStatistic > 0)
                    {   // запрос к БД
                        long cntRecs = ServiceReco.TestPhotosCount();
                        ServiceReco.Log("Date: " + lastDateTime + ",  interval:  " + dateTimeFrom.Substring(11, 8) + "-" + dateTimeTo.Substring(11, 8) + ",  unprocessed photos: " + cntRecs + ",  LastID: " + lastId, 0);
                    }
                }


                // очистка мусора
                GC.Collect();
                GC.WaitForPendingFinalizers();
                myParams.ReadAll();
            }
            while (useCycle > 0);

            //return;
        }


        public static void Go(object x)
        {
            string name = (string)x;

            // запускаем внешний обработчик
            Process myProcess = new Process();

            myProcess.StartInfo.UseShellExecute = false;
            myProcess.StartInfo.FileName = "AngelVision.TypeModelReco.exe";
            myProcess.StartInfo.Arguments = "-name " + name;
            myProcess.Start();

            myProcess.WaitForExit();
        }

    }
}
