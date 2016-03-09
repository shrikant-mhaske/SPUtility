using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Data;
using MySql.Data.MySqlClient;
using System.Data.SqlClient;

namespace RawData
{
    public class RawDataBusinessLogic
    {

        #region GetCustomer Dbstring & Speciality
        //Get customer list
        public DataTable getCusomerlist()
        {
            DataTable custList = new DataTable();
            try
            {
                FileLogger.AppendLog("Access", "RawDataBusinessLogic=>getCusomerlist", "Start getting Customer list");
               /*  string query = @"select  distinct MasterCustomerID,dbstring,Specility as 'Domain'  from [License Server].[dbo].[licensemaster] 
                             where user_type='Super Admin'  and  isNull(MasterCustomerID,'')<>'' and 
                             firstname not like '%qa%' and firstname not like '%test%' and firstname not like '%demo%' and
                             lastname not like '%qa%' and lastname not like '%test%' and lastname not like '%demo%'"; */


                /* string query = @"select  distinct Top 100 MasterCustomerID,dbstring,Specility as 'Domain'  from [License Server].[dbo].[licensemaster] 
                              where user_type='Super Admin'  and  Specility='Dental' and dbstring like '%Host=10.67.206.111;%' "; */

                  string query = @"select   MasterCustomerID,dbstring,Specility as 'Domain'  from [License Server].[dbo].[licensemaster] 
                         where user_type='Super Admin'  and  Specility='Dental' and MasterCustomerID in ('d10648')  "; 
                custList = DBmain.getDataFromSQLServer(query);
                FileLogger.AppendLog("Access", "RawDataBusinessLogic=>getCusomerlist", "Finish getting Customer list");

            }
            catch (Exception Ex)
            {
                FileLogger.AppendLog("Error", "getCusomerlist()", Ex.Message);
            }
            return custList;

        }
        #endregion

        #region Get field & query to execute on mysql
        //Get query list
        public DataTable getQuerylist()
        {
          
            DataTable custList = new DataTable();
            try
            {
                FileLogger.AppendLog("Access", "RawDataBusinessLogic=>getQuerylist", "Start getting Query list");
                string query = @"select FieldName,FieldQuery from  [SCD].[DBO].[AllCustomerRawDataQuery];";
                custList = DBmain.getDataFromSQLServer(query);
                FileLogger.AppendLog("Access", "RawDataBusinessLogic=>getQuerylist", "Finish getting Query list");

            }
            catch (Exception Ex)
            {
                FileLogger.AppendLog("Error", "getQuerylist()", Ex.Message);
            }
            return custList;

        }
        #endregion 

        #region Start getting data one by one customer
        public bool getDataOneByOne(DataTable custlist, DataTable querylist)
        {
            bool ret = false;
            
            
            try
            {
                if (custlist != null && custlist.Rows.Count > 0)
                {
                    if (querylist != null && querylist.Rows.Count > 0)
                    {
                        // Define the cancellation token.
                        CancellationTokenSource source = new CancellationTokenSource();
                        CancellationToken token = source.Token;

                        //Define limit to thread
                        LimitedConcurrencyLevelTaskScheduler lcts = new LimitedConcurrencyLevelTaskScheduler(16);
                        TaskFactory factory = new TaskFactory(lcts);

                        Task<int>[] tasks = new Task<int>[custlist.Rows.Count];
                        int i = 0;
                        foreach (DataRow temp in custlist.Rows)

                        {

                            //   tasks[i] = Task<int>.Factory.StartNew(() => getSingleAccountDetail(Convert.ToString(temp["MasterCustomerID"]), Convert.ToString(temp["Domain"]), Convert.ToString(temp["dbstring"]), querylist), token);
                            tasks[i] = factory.StartNew(() => getSingleAccountDetail(Convert.ToString(temp["MasterCustomerID"]), Convert.ToString(temp["Domain"]), Convert.ToString(temp["dbstring"]), querylist), token);
                            i++;                           
                            // getSingleAccountDetail(Convert.ToString(temp["MasterCustomerID"]), Convert.ToString(temp["Domain"]), Convert.ToString(temp["dbstring"]), querylist);                           
                        }

                        Task.WaitAll(tasks);
                        FileLogger.AppendLog("Access", "", "Complete All DB raw data");



                    }
                    else
                    {
                        FileLogger.AppendLog("Access", "", "Querylist is empty");
                    }
                }
                else
                {
                    FileLogger.AppendLog("Access", "", "Customerlist is empty");
                }

            }
            catch (Exception Ex)
            {
                FileLogger.AppendLog("Error", "getDataOneByOne", Ex.Message);
            }
           
            return ret;
        }
        #endregion

        #region Get single account detail
        //Get data of single account
        public static int getSingleAccountDetail(string MID, string D, string DBString, DataTable querydt)
        {
            try
            {
                FileLogger.AppendLog(MID + "-Access", "AllCustomerRowData=>>getSingleAccountDetail", "====Start getting data of " + MID + "====");
               
                List<Detail> lstDetail = new List<Detail>();
                
                bool ret = false;
                try
                {

                   
                    //get All practicename data
                    string query = "select distinct PracticeID,CustID,Practicename,city,state,zip from allpracticenames where practiceid is not null";        
                    DataTable allPracticeNameData = DBmain.getDataFromMySQL(DBString, query);
                    FileLogger.AppendLog(MID + "-Access", "AllCustomerRowData=>>getSingleAccountDetail", "====Start getting data of " + MID + "====");
                    if (allPracticeNameData != null && allPracticeNameData.Rows.Count > 0)
                    {
                        //Practicename & Custid loop
                        foreach (DataRow drAPN in allPracticeNameData.Rows)
                        {
                            //Empty list
                            lstDetail = new List<Detail>();

                            // lopping query 1 by 1 
                            foreach (DataRow drqr in querydt.Rows)
                            {
                                //Replacing custid & practiceid in query
                                string query1 = Convert.ToString(drqr["FieldQuery"]).Trim().Replace("rawdatacustid", Convert.ToString(drAPN["CustID"]).Trim()).Replace("rawdatapracticeid", Convert.ToString("'" + drAPN["PracticeID"] + "'").Trim());
                                DataTable queryResult = DBmain.getDataFromMySQL(DBString, query1);
                                if (queryResult != null && queryResult.Rows.Count > 0)
                                {
                                    //add query data into list
                                    Detail detaiTemp = new Detail();
                                    detaiTemp.MastercustomerID = MID;
                                    detaiTemp.FieldName = Convert.ToString(drqr["FieldName"]).Trim();
                                    detaiTemp.value = Convert.ToString(queryResult.Rows[0][0]).Trim();
                                    lstDetail.Add(detaiTemp);
                                }
                                else
                                {
                                    //ad if ther is no data for query
                                    Detail detaiTemp = new Detail();
                                    detaiTemp.MastercustomerID = MID;
                                    detaiTemp.FieldName = Convert.ToString(drqr["FieldName"]).Trim();
                                    detaiTemp.value = "No Data";
                                    lstDetail.Add(detaiTemp);
                                }


                            }



                            //Practicename
                            Detail detailPracticename = new Detail();
                            detailPracticename.MastercustomerID = MID;
                            detailPracticename.FieldName = "Practicename";
                            detailPracticename.value = Convert.ToString(drAPN["Practicename"]).Trim();
                            lstDetail.Add(detailPracticename);

                            //city
                            Detail detailcity = new Detail();
                            detailcity.MastercustomerID = MID;
                            detailcity.FieldName = "City";
                            detailcity.value = Convert.ToString(drAPN["city"]).Trim();
                            lstDetail.Add(detailcity);

                            //state
                            Detail detailstate = new Detail();
                            detailstate.MastercustomerID = MID;
                            detailstate.FieldName = "State";
                            detailstate.value = Convert.ToString(drAPN["state"]).Trim();
                            lstDetail.Add(detailstate);

                            //zip
                            Detail detailzip = new Detail();
                            detailzip.MastercustomerID = MID;
                            detailzip.FieldName = "Zip";
                            detailzip.value = Convert.ToString(drAPN["zip"]).Trim();
                            lstDetail.Add(detailzip);

                            //Domain                       
                            Detail detailDomain = new Detail();
                            detailDomain.MastercustomerID = MID;
                            detailDomain.FieldName = "Domain";
                            detailDomain.value = D;
                            lstDetail.Add(detailDomain);


                            //Get last refresh detail & SPU status 
                            DataTable refData = getRefreshDetailFromSCD(MID, drAPN["CustID"].ToString().Trim());
                            if (refData != null && refData.Rows.Count > 0)
                            {
                                //SPUID                                                                
                                Detail detailSPUID = new Detail();
                                detailSPUID.MastercustomerID = MID;
                                detailSPUID.FieldName = "SPUID";
                                detailSPUID.value = Convert.ToString( refData.Rows[0]["SPUID"]);
                                lstDetail.Add(detailSPUID);

                                //LastRefreshDate
                                Detail detailLastRefreshDate = new Detail();
                                detailLastRefreshDate.MastercustomerID = MID;
                                detailLastRefreshDate.FieldName = "LastRefreshDate";
                                detailLastRefreshDate.value = Convert.ToString(refData.Rows[0]["LastRefreshDate"]);
                                lstDetail.Add(detailLastRefreshDate);

                                //SPUStatus
                                Detail detailSPUStatus = new Detail();
                                detailSPUStatus.MastercustomerID = MID;
                                detailSPUStatus.FieldName = "SPUStatus";
                                detailSPUStatus.value = Convert.ToString(refData.Rows[0]["SPUStatus"]);
                                lstDetail.Add(detailSPUStatus);
                            }

                            else
                            {
                                //SPUID                                                                
                                Detail detailSPUID = new Detail();
                                detailSPUID.MastercustomerID = MID;
                                detailSPUID.FieldName = "SPUID";
                                detailSPUID.value = "Not Uploaded in SCD Log";
                                lstDetail.Add(detailSPUID);

                                //LastRefreshDate
                                Detail detailLastRefreshDate = new Detail();
                                detailLastRefreshDate.MastercustomerID = MID;
                                detailLastRefreshDate.FieldName = "LastRefreshDate";
                                detailLastRefreshDate.value = "Not Uploaded in SCD Log";
                                lstDetail.Add(detailLastRefreshDate);

                                //SPUStatus
                                Detail detailSPUStatus = new Detail();
                                detailSPUStatus.MastercustomerID = MID;
                                detailSPUStatus.FieldName = "SPUStatus";
                                detailSPUStatus.value = "Not Uploaded in SCD Log";
                                lstDetail.Add(detailSPUStatus);
                            }



                            //create list of query
                            List<string> queryList = new List<string>();


                            string PracticeID = Convert.ToString(drAPN["PracticeID"]).Trim();
                            int CustID = Convert.ToInt32(drAPN["CustID"].ToString().Trim());


                            //Add current raw data of current masterid into  AllCustomerRawDataHistory table
                            queryList.Add("insert into  [SCD].dbo.[AllCustomerRawDataHistory] select * from [SCD].[dbo].[AllCustomerRawData] where [AllCustomerRawData].mastercustomerid='" + MID + "' and  [AllCustomerRawData].[PracticeID]='" + PracticeID + "' and [AllCustomerRawData].[CustID]=" + CustID);

                            //delete existing data current masterid  from [AllCustomerRawData] to fill new data
                            queryList.Add("delete from [SCD].[dbo].[AllCustomerRawData] where [AllCustomerRawData].mastercustomerid='" + MID + "' and  [AllCustomerRawData].[PracticeID]='" + PracticeID + "' and [AllCustomerRawData].[CustID]=" + CustID);

                            //fill new data one by on 
                            foreach (Detail dt in lstDetail)
                            {
                                string tempQuery = "insert into [SCD].dbo.[AllCustomerRawData](MasterCustomerID,FieldName,[Value],[InnsertedTime],[PracticeID],[CustID]) values('" + dt.MastercustomerID + "','" + dt.FieldName + "','" + dt.value + "',GetDate(),'" + PracticeID + "'," + CustID + ")";
                                queryList.Add(tempQuery);
                            }

                            ret = DBmain.setSQLServerQueryListInDb(queryList, MID, PracticeID, CustID);


                        }

                    }
                    else
                    {
                        Detail detaiTemp = new Detail();
                        detaiTemp.MastercustomerID = MID;
                        detaiTemp.FieldName = "EmptyAllPracticeNames";
                        detaiTemp.value = "allpracticenames is empty";
                        lstDetail.Add(detaiTemp);



                        List<string> queryList = new List<string>();

                        //Add current raw data of current masterid into  AllCustomerRawDataHistory table
                        queryList.Add("insert into  [SCD].dbo.[AllCustomerRawDataHistory] select * from [SCD].[dbo].[AllCustomerRawData] where [AllCustomerRawData].mastercustomerid='" + MID + "'");

                        //delete existing data current masterid  from [AllCustomerRawData] to fill new data
                        queryList.Add("delete from [SCD].[dbo].[AllCustomerRawData] where [AllCustomerRawData].mastercustomerid='" + MID + "'");

                        //insert data
                        string tempQuery = "insert into [SCD].dbo.[AllCustomerRawData](MasterCustomerID,FieldName,Value,InnsertedTime) values('" + detaiTemp.MastercustomerID + "','" + detaiTemp.FieldName + "','" + detaiTemp.value + "',GetDate())";
                        queryList.Add(tempQuery);


                        ret = DBmain.setSQLServerQueryListInDb(queryList, MID, "NoPracticeID", 0);

                    }

                }
                catch (Exception Ex)
                {
                    FileLogger.AppendLog(MID + "-Error", "getSingleAccountDetail=>" + MID, Ex.Message);
                }
                if (ret)
                    FileLogger.AppendLog(MID + "-Access", "AllCustomerRowData=>>getSingleAccountDetail", "    Successfully getting data of " + MID);
                else
                    FileLogger.AppendLog(MID + "-Access", "AllCustomerRowData=>>getSingleAccountDetail", "    Some error in getting data of " + MID);
                FileLogger.AppendLog(MID + "-Access", "AllCustomerRowData=>>getSingleAccountDetail", "====Finish getting data of " + MID + "====");
                // return ret;
            }
            catch (Exception Ex)
            {

            }

            return 1;

        }



        #endregion

        #region Get Refresh Detail & SPUStatus from MasterID & CustID
        public static DataTable getRefreshDetailFromSCD(string MID, string CID)
        {
            // CustID in AllPracticename is PracticeID in UploadedSettings 
            DataTable RefData = new DataTable();
            try
            {
                string query = string.Format(@"select US.SPUID,US.LastRefreshDate,case when  (custmaster.isInactive=1) then 'InActive' else 'Active' end as 'SPUStatus'  from 
                                            (
                                            select techid as 'SPUID',
                                            convert(varchar(11), isNull([WebSync], ''), 121) as 'LastRefreshDate',
                                            convert(varchar(11), isNull([SynchronizationID], ''), 121) as 'MasterCustomerID',
                                            convert(varchar(11), isNull([PracticeID], ''),121 )as 'PracticeID'
                                            from
                                            (select techid,[sub],[value] from[SCD].DBO.UploadedSettings where   filename = 'SSCSettings.ini' and  isNull(value, '') <> '') as result
                                            Pivot
                                            (
                                            Max([value])
                                            for sub in 
                                            (
                                            [SynchronizationID],
                                            [WebSync],
                                            [PracticeID]
                                             )
                                            ) as p
                                            ) as US
                                            left join[SCD].DBO.custmaster on US.SPUID = custmaster.techid
                                            where US.MasterCustomerID = '{0}' and US.PracticeID = '{1}'", MID, CID);
                RefData = DBmain.getDataFromSQLServer(query);
            }
            catch (Exception Ex)
            {
                FileLogger.AppendLog(MID + "-Error", "getRefreshDetailFromSCD=>" + MID, Ex.Message);
            }
            return RefData;
        }
        #endregion

        #region  LimitedConcurrencyLevelTaskScheduler

        /// <summary>
        /// Provides a task scheduler that ensures a maximum concurrency level while
        /// running on top of the ThreadPool.
        /// </summary>
        public class LimitedConcurrencyLevelTaskScheduler : TaskScheduler
        {
            /// <summary>Whether the current thread is processing work items.</summary>
            [ThreadStatic]
            private static bool _currentThreadIsProcessingItems;
            /// <summary>The list of tasks to be executed.</summary>
            private readonly LinkedList<Task> _tasks = new LinkedList<Task>(); // protected by lock(_tasks)
                                                                               /// <summary>The maximum concurrency level allowed by this scheduler.</summary>
            private readonly int _maxDegreeOfParallelism;
            /// <summary>Whether the scheduler is currently processing work items.</summary>
            private int _delegatesQueuedOrRunning = 0; // protected by lock(_tasks)

            /// <summary>
            /// Initializes an instance of the LimitedConcurrencyLevelTaskScheduler class with the
            /// specified degree of parallelism.
            /// </summary>
            /// <param name="maxDegreeOfParallelism">The maximum degree of parallelism provided by this scheduler.</param>
            public LimitedConcurrencyLevelTaskScheduler(int maxDegreeOfParallelism)
            {
                if (maxDegreeOfParallelism < 1) throw new ArgumentOutOfRangeException("maxDegreeOfParallelism");
                _maxDegreeOfParallelism = maxDegreeOfParallelism;
            }

            /// <summary>Queues a task to the scheduler.</summary>
            /// <param name="task">The task to be queued.</param>
            protected sealed override void QueueTask(Task task)
            {
                // Add the task to the list of tasks to be processed.  If there aren't enough
                // delegates currently queued or running to process tasks, schedule another.
                lock (_tasks)
                {
                    _tasks.AddLast(task);
                    if (_delegatesQueuedOrRunning < _maxDegreeOfParallelism)
                    {
                        ++_delegatesQueuedOrRunning;
                        NotifyThreadPoolOfPendingWork();
                    }
                }
            }

            /// <summary>
            /// Informs the ThreadPool that there's work to be executed for this scheduler.
            /// </summary>
            private void NotifyThreadPoolOfPendingWork()
            {
                ThreadPool.UnsafeQueueUserWorkItem(_ =>
                {
                    // Note that the current thread is now processing work items.
                    // This is necessary to enable inlining of tasks into this thread.
                    _currentThreadIsProcessingItems = true;
                    try
                    {
                        // Process all available items in the queue.
                        while (true)
                        {
                            Task item;
                            lock (_tasks)
                            {
                                // When there are no more items to be processed,
                                // note that we're done processing, and get out.
                                if (_tasks.Count == 0)
                                {
                                    --_delegatesQueuedOrRunning;
                                    break;
                                }

                                // Get the next item from the queue
                                item = _tasks.First.Value;
                                _tasks.RemoveFirst();
                            }

                            // Execute the task we pulled out of the queue
                            base.TryExecuteTask(item);
                        }
                    }
                    // We're done processing items on the current thread
                    finally { _currentThreadIsProcessingItems = false; }
                }, null);
            }

            /// <summary>Attempts to execute the specified task on the current thread.</summary>
            /// <param name="task">The task to be executed.</param>
            /// <param name="taskWasPreviouslyQueued"></param>
            /// <returns>Whether the task could be executed on the current thread.</returns>
            protected sealed override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
            {
                // If this thread isn't already processing a task, we don't support inlining
                if (!_currentThreadIsProcessingItems) return false;

                // If the task was previously queued, remove it from the queue
                if (taskWasPreviouslyQueued) TryDequeue(task);

                // Try to run the task.
                return base.TryExecuteTask(task);
            }

            /// <summary>Attempts to remove a previously scheduled task from the scheduler.</summary>
            /// <param name="task">The task to be removed.</param>
            /// <returns>Whether the task could be found and removed.</returns>
            protected sealed override bool TryDequeue(Task task)
            {
                lock (_tasks) return _tasks.Remove(task);
            }

            /// <summary>Gets the maximum concurrency level supported by this scheduler.</summary>
            public sealed override int MaximumConcurrencyLevel { get { return _maxDegreeOfParallelism; } }

            /// <summary>Gets an enumerable of the tasks currently scheduled on this scheduler.</summary>
            /// <returns>An enumerable of the tasks currently scheduled.</returns>
            protected sealed override IEnumerable<Task> GetScheduledTasks()
            {
                bool lockTaken = false;
                try
                {
                    Monitor.TryEnter(_tasks, ref lockTaken);
                    if (lockTaken) return _tasks.ToArray();
                    else throw new NotSupportedException();
                }
                finally
                {
                    if (lockTaken) Monitor.Exit(_tasks);
                }
            }
        }

        #endregion  LimitedConcurrencyLevelTaskScheduler

    }


    public class Detail
    {
        public string MastercustomerID { get; set; }
        public string FieldName { get; set; }
        public string value { get; set; }
    }

}
