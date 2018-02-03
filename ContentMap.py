from pyspark import SparkConf,SparkContext
import time
import sys
import re
from pyspark.sql.functions import *
from pyspark.sql.functions import broadcast
from pyspark.sql import *
from pyspark.sql.types import *
import datetime
from pyspark.sql import functions as F
import pandas as pd
from time import gmtime, strftime
import logging
import argparse
from pytz import timezone
import pytz
from datetime import datetime
from pyspark.sql.types import MapType
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
import smtplib
from email import encoders
import random

### creating spark context
conf = SparkConf()
conf.setAppName('alpha-code')
sc = SparkContext(conf=conf)

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


def get_ist_date(utc_dt):
    import pytz
    from datetime import datetime
    local_tz = pytz.timezone('Asia/Kolkata')
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
    return local_tz.normalize(local_dt)

def get_pst_date():
    from datetime import datetime
    dt = datetime.utcnow()
    return dt

def lower_locale(locale):
    return locale.lower()

lower_locale_udf = udf(lower_locale,StringType())

### Function for importing SQL table
def importSQLTable(dbname, tablename):
    temp = (sqlContext.read.format("jdbc")
    .option("url", severName.value)
    .option("driver",severProperties.value["driver"])
    .option("dbtable", dbname+".dbo."+tablename)
    .option("user", severProperties.value["user"])
    .option("password", severProperties.value["password"]).load())
    return temp

def importSQLTableForBusinessBasedSuppression(query):
    temp = (sqlContext.read.format("jdbc").
        option("url", "jdbc:sqlserver://10.23.19.63").
        option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver").
        option("dbtable", query).
        option("user", "occuser").
        option("password", "Exp3dia22").load())
    return temp

def code_completion_email(body,subject,pos,locale_name):
    fromaddr = "expedia_notifications@affineanalytics.com"
    toaddr = ["alphaalerts@expedia.com"]
    toaddress = ", ".join(toaddr)
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddress
    msg['Subject'] = subject
    bodysub="\n [This is an automated mail triggered from one of your running application] \n \n Alpha process for " +str(pos)+" , "+str(locale_name)+ " (pos,locale)." + " has {}. \n \n Please do not reply directly to this e-mail. If you have any questions or comments regarding this email, please contact us at AlphaTechTeam@expedia.com."
    actual_message=bodysub.format(body) 
    msg.attach(MIMEText(actual_message, 'plain'))       
    server = smtplib.SMTP('smtp.office365.com', 587)
    server.starttls()
    server.login(fromaddr, "Affine@123")
    text = msg.as_string()
    server.sendmail(fromaddr, toaddr, text)
    server.quit()
    print ("Code Completion mail triggered successfully")
    return "1"



AlphaStartDate = get_pst_date()
print("AlphaStartDate = ",AlphaStartDate)

### defining log id initiation
rep = 1440

### function to append logs to central log table

def log_df_update(spark,IsComplete,Status,EndDate,ErrorMessage,RowCounts,StartDate,FilePath,tablename):
    import pandas as pd
    l = [(process_name_log.value,process_id_log.value,IsComplete,Status,StartDate,EndDate,ErrorMessage,int(RowCounts),FilePath)]
    schema = (StructType([StructField("SourceName", StringType(), True),StructField("SourceID", IntegerType(), True),StructField("IsComplete", IntegerType(), True),StructField("Status", StringType(), True),StructField("StartDate", TimestampType(), True),StructField("EndDate", TimestampType(), True),StructField("ErrorMessage", StringType(), True),StructField("RowCounts", IntegerType(), True),StructField("FilePath", StringType(), True)]))
    rdd_l = sc.parallelize(l)
    log_df = spark.createDataFrame(rdd_l,schema)
    #  "Orchestration.dbo.AlphaProcessDetailsLog_LTS"
    log_df.withColumn("StartDate",from_utc_timestamp(log_df.StartDate,"PST")).withColumn("EndDate",from_utc_timestamp(log_df.EndDate,"PST")).write.jdbc(url=url, table=tablename,mode="append", properties=properties)

#PST date for comparing with IST
def get_pst_date_final(utc_dt):
    import pytz
    from datetime import datetime
    local_tz = pytz.timezone('US/Pacific')
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
    return local_tz.normalize(local_dt)
    
from datetime import datetime as dt

AlphaLaunchDate = get_pst_date()
ISTdate= get_ist_date(dt.utcnow())
PSTdate = get_pst_date_final(dt.utcnow())
ISTLauchDate=ISTdate.strftime('%Y-%m-%d')
PSTLauchDate=PSTdate.strftime('%Y-%m-%d')

#print("AlphaLaunchDate: ",AlphaLaunchDate)
#print("ISTdate: ",ISTdate)
#print("PSTdate: ", PSTdate)
#print("ISTLauchDate: ",ISTLauchDate)
#print("PSTLauchDate: ",PSTLauchDate)

if((ISTdate > PSTdate) & (dt.now(pytz.timezone('US/Pacific')).hour > 20)):
    launch_dt = ISTLauchDate
else:
    launch_dt = PSTLauchDate

### defining parameters for a campaign 
StartDate = get_pst_date()

try :

    parser = argparse.ArgumentParser()
    parser.add_argument("--locale_name", help="Write locale_name like en_nz. This is manadatory.")
    parser.add_argument("--job_type", help="Write prod or test. This is manadatory. Used to determine type of job")
    parser.add_argument("--env_type", help="Write prod or test. This is manadatory. Used to determine data source")
    parser.add_argument("--cpgn_type", help="Write loyalty or marketing")
    
    parser.add_argument("--run_date", help="Write launch_date like Y-M-D")
    parser.add_argument("--test_type", help="AUTOTEST or MANUAL or BACKUP OR XYZ")
    parser.add_argument("--campaign_id", help="Campaign ID or Null")
    parser.add_argument("--email_address", help="Enter email address")
#    parser.add_argument("--GITVER", help="Enter git version to use")

    args = parser.parse_args()
    locale_name = args.locale_name
    job_type = args.job_type
    env_type = args.env_type
    cpgn_type = args.cpgn_type
    run_date = args.run_date
    test_type = args.test_type
#    GITVER = args.GITVER
#   locale_name = 'en_nz'
#   job_type = 'test'
#   env_type = 'prod'
    
#   if job_type == 'test':
#       run_date = '2017-12-06'
#       test_type = 'BACKUP'
        
    email_address = ''
    campaign_id = ''    

    if job_type == 'test':
        if test_type == 'MANUAL':
            email_address = args.email_address
            campaign_id = args.campaign_id
        elif test_type == 'AUTOTEST':
            email_address = args.email_address
    
    #locale_name = 'en_nz' #this will have to be commented out during the migration to jenkins
    #job_type = 'test' #this variable will always be prod for Jenkin jobs
    #env_type = 'test'
    
    #run_date = '2017-10-29'
    #test_type = 'BACKUP'
    
    #campaign_id = 1
    #email_address = 'xxx@expedia.com'
    
    ### sql server info for writing log table
    properties = {"user" : "occuser" , "password":"Exp3dia22" , "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver"}
    if env_type == 'prod':
        url = "jdbc:sqlserver://10.23.18.135"
        ocelotDb = "AlphaProd"
    else:
        url = "jdbc:sqlserver://10.23.16.35"
        ocelotDb = "AlphaTest"
    
    severName = sc.broadcast(url)
    severProperties = sc.broadcast(properties)
    
    #hard-coded variables
    data_environ = env_type ##for production it should be always 'prod'
    pos = locale_name.split('_')[1].upper()
    current_date =  time.strftime("%Y/%m/%d")
    
    status_table = importSQLTable("Orchestration","AlphaConfig")
    pos1 = locale_name.split('_')[1].upper() #Converting the country to uppercase
    pos0 = locale_name.split('_')[0]  #Obtaining the language code
    
    global search_string
    search_string = pos0+"_"+pos1 #storing locale in 'en_US' format
    required_row = status_table.filter(status_table.Locale == search_string).filter("brand like 'Brand Expedia'").collect() #finding out the rows that contain the brand Expedia only
  
    global process_id
    global process_name 
    process_id = required_row[0]['id']  #ID of the particular row
    process_name = required_row[0]['ProcessName'] #Process name for the filtered row

    process_id_log = sc.broadcast(process_id)
    process_name_log = sc.broadcast(process_name)
    
    if job_type == 'prod':
        CentralLog_str='Orchestration.dbo.CentralLog'
        AlphaProcessDetailsLog_str='Orchestration.dbo.AlphaProcessDetailsLog'
        success_str='Campaign Suppression process completed'
        failure_str='Campaign Suppression process failed'
    elif job_type == 'test':
        if test_type != 'BACKUP':
            CentralLog_str='Orchestration.dbo.CentralLog_LTS'
            AlphaProcessDetailsLog_str='Orchestration.dbo.AlphaProcessDetailsLog_LTS'
            success_str='Live Test Sends completed'
            failure_str='Live Test Sends failed'
        else:
            CentralLog_str='Orchestration.dbo.CentralLog_BACKUP'
            AlphaProcessDetailsLog_str='Orchestration.dbo.AlphaProcessDetailsLog_BACKUP'
            success_str='Campaign Suppression process completed'
            failure_str='Campaign Suppression process failed'
            
    log_df_update(sqlContext,1,"Content Map module has started for {} ".format(locale_name),get_pst_date(),' ','0',StartDate,' ',AlphaProcessDetailsLog_str)
    log_df_update(sqlContext,1,'Parameters are correct',get_pst_date(),' ','0',StartDate,' ',AlphaProcessDetailsLog_str)
    
except:
    log_df_update(sqlContext,0,'Failed',get_pst_date(),'Parameters are improper','0',StartDate,' ',AlphaProcessDetailsLog_str)
    log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
    log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
    code_completion_email("failed due to parameters not being present","Alpha process update for "+locale_name,pos,locale_name)

    raise Exception("Parameters not present!!!")
    
if  job_type == 'prod':
    LaunchDate = launch_dt
else:
    LaunchDate = run_date

    
file_dt = (LaunchDate.split("-")[0]) + (LaunchDate.split("-")[1]) + (LaunchDate.split("-")[2])
    
#LaunchDate = ISTLauchDate
print("LaunchDate = " + str(LaunchDate))

#print("Central log: ", CentralLog_str)
#print("Alpha details log: ", AlphaProcessDetailsLog_str)

def BuildPath(env_type,job_type,test_type,LaunchDate,locale_name,cpgn_type):
    path_dict = {}
    for path in ['OcelotDataProcessing_Output','Mis_Output','TP_Output','User_Token_Output','RecipientID_Output',]:
        if  job_type != 'prod':
            if test_type in ('MANUAL','BACKUP','AUTOTEST'):
                path_dict[path] = "s3://occ-decisionengine/AlphaModularization/Environment_{}/Job_{}/Test_{}/{}/{}/{}/{}/".format(env_type,job_type,test_type,LaunchDate,locale_name,cpgn_type,path)
        else:
            path_dict[path] = "s3://occ-decisionengine/AlphaModularization/Environment_{}/Job_{}/{}/{}/{}/{}/".format(env_type,job_type,LaunchDate,locale_name,cpgn_type,path)
    return path_dict

path_dict = BuildPath(env_type,job_type,test_type,LaunchDate,locale_name,cpgn_type)
print("created dictionary of paths")


def importData():
    import ast
    
        #Read data from Ocelot module
    metaCampaign_backup_path = path_dict['OcelotDataProcessing_Output']+"dfMetaCampaignData_VarDef_backup"
    dfMetaCampaignData_VarDef_backup = sqlContext.read.parquet(metaCampaign_backup_path)
    metaCampaign_path = path_dict['OcelotDataProcessing_Output']+"dfMetaCampaignData_VarDef"
    dfMetaCampaignData_VarDef = sqlContext.read.parquet(metaCampaign_path)
        #Read data from MIS module
    mis_outputPath = path_dict['Mis_Output']+"misDictContent/mis_content_dict.txt"
    mis_content_rdd = sc.textFile(mis_outputPath)
    mis_content_dict = ast.literal_eval(mis_content_rdd.collect()[0])
    #mis_df = sqlContext.read.parquet("s3://occ-decisionengine/MetaDataCreation_Output/misDictContent")
    #mis_content_dict=mis_df.select("Dictionary").collect()[0]["Dictionary"]

        #Read data from Traveler modul
    tp_output_path = path_dict['TP_Output']+"final_df_for_alpha"
    final_df_for_alpha = sqlContext.read.parquet(tp_output_path)
    test_flag = sqlContext.read.parquet(path_dict['OcelotDataProcessing_Output']+"TestFlag")
    module_version_test_flag = test_flag.select("flag").collect()[0][0]
    final_dh_path = path_dict['OcelotDataProcessing_Output']+"final_df"
    final_df = sqlContext.read.parquet(final_dh_path)
    final_dh_path_module_type = path_dict['OcelotDataProcessing_Output']+"finaldfModuleType"
    finaldfModuleType = sqlContext.read.parquet(final_dh_path_module_type)
    
    dfMetaCampaignData1_path = path_dict['OcelotDataProcessing_Output']+"dfMetaCampaignData1"
    dfMetaCampaignData1 = sqlContext.read.parquet(dfMetaCampaignData1_path)

    dfMetaCampaignData_31_path = path_dict['OcelotDataProcessing_Output']+"dfMetaCampaignData_31"
    dfMetaCampaignData_31 = sqlContext.read.parquet(dfMetaCampaignData_31_path)


        #tp_cols_path = "s3://occ-decisionengine/OcelotDataProcessing_Output/{}/{}/traveler_data_req".format(LaunchDate,locale_name)
        #tp_cols=spark.read.parquet(tp_cols_path)
    dfLoyalty = final_df_for_alpha.select("paid","tpid","campaign_id","PM_OK_IND","LoyaltyMemberID","LRMStatus","LoyaltyAvailablePoints","LoyaltyMemberTierName","MonetaryValue","LoyaltyMemberStatus")
    
    return dfMetaCampaignData_VarDef_backup,dfMetaCampaignData_VarDef,mis_content_dict,module_version_test_flag,final_df,final_df_for_alpha,dfMetaCampaignData1,dfLoyalty,dfMetaCampaignData_31,finaldfModuleType

print("Importing data")    
(dfMetaCampaignData_VarDef_backup,dfMetaCampaignData_VarDef,mis_content_dict,module_version_test_flag,final_df,final_df_for_alpha,dfMetaCampaignData1,dfLoyalty,dfMetaCampaignData_31,finaldfModuleType)=importData()
print("Data imported successfully")



def createTestDict():
    lookup_df = final_df.filter("T_C_flag != 'control'").withColumn("Replacement_Identifier",concat(col("test_keys"),lit("#"),col("campaign_segment_type_id"),lit("#"),col("campaign_look_up_id") )).select("Replacement_Identifier","module_id").toPandas()
    #Creating a dictionary for control look up
    lookup_df.index = lookup_df.Replacement_Identifier
    lookup_dict = lookup_df.to_dict()['module_id']
    #print(lookup_dict)
    #creating a dictionary for module type  look up
    lookup_df_control = final_df.filter("T_C_flag != 'control'").withColumn("Replacement_Identifier_Type",concat(col("test_keys"),lit("#"),col("campaign_segment_type_id"),lit("#"),col("campaign_look_up_id"),lit("#"),col("module_type_id"),lit("#"),col("slot_position"),lit("#"),col("segment_type_id") )).select("Replacement_Identifier_Type","module_id").toPandas()
    lookup_df_control.index = lookup_df_control.Replacement_Identifier_Type
    lookup_dict_control = lookup_df_control.to_dict()['module_id']
    #print(lookup_dict_control)
    #Step 1: Check if there is a module version test scheduled for this campaign. If not, skip this cell.
    #step 2: Obtain a mapping between each module type and the corresponding placement type. Dict struct --> {module_id : placement_type}
    #Step 3: Filter meta campaign only for active published. This will rotate only active published versions amongst candidates

    #Creating a mapping. This includes mapping for test published data
    placement_modid_lookup_pandas = dfMetaCampaignData_31.select("module_id","placement_type").distinct().toPandas()
    placement_modid_lookup_pandas.index = placement_modid_lookup_pandas.module_id
    placement_modid_dict = placement_modid_lookup_pandas.to_dict()['placement_type']

    return lookup_dict,lookup_dict_control,placement_modid_dict

lookup_dict,lookup_dict_control,placement_modid_dict = createTestDict()
print("created dictionary for campaign test")


def obtainSamplingForModuleType():
    return(finaldfModuleType)


def moduleTypeTestCreateDict():
    test_lookup_final = obtainSamplingForModuleType()
    controlSegmentModuleMapIDs = test_lookup_final.filter("Control_Test_Flag = 0").select("segment_module_map_id").distinct().rdd.flatMap(lambda x:x).collect()
    segmentsUnderTest = test_lookup_final.select("segment_type_id").distinct().rdd.flatMap(lambda x:x).collect()
    campaignsForTest = dfMetaCampaignData_31.filter(dfMetaCampaignData_31.segment_module_map_id.isin(controlSegmentModuleMapIDs)).select("campaign_id").distinct().rdd.flatMap(lambda x:x).collect()
    treatmentModuleList = test_lookup_final.select("module_type_id").distinct().rdd.flatMap(lambda x:x).collect()
    segmentsUnderTest = [str(i) for i in segmentsUnderTest]
    if(len(campaignsForTest) == 0):
        moduleTypeTestFlag = 0
    else:
        moduleTypeTestFlag = 1
    print(campaignsForTest)
    #TestKeyModuleDict basically tells us the module being assigned to a given test key for a given segment. It has the formant test_keys#segmenttypeID : [moduleTypeID] . Note that the values are present in a list format. This is because there can be multiple tests running on the same segment
    #segmentTypeControlDict is a dictionary that tells us the control module type for each segment. This dictionary is now redundant. 
    testKeyPandas = test_lookup_final.select("test_keys","module_type_id","segment_type_id").withColumn("moduleTypeKey",concat(col("test_keys"),lit("#"),col("segment_type_id"))).groupby("moduleTypeKey").agg(F.collect_set("module_type_id").alias("module_type_id")).toPandas()
    testKeyPandas.index = testKeyPandas['moduleTypeKey']
    TestKeyModuleDict = testKeyPandas.to_dict()['module_type_id']
    segmentTypeControlPandas = test_lookup_final.filter("Control_Test_Flag = 0").select("module_type_id","segment_type_id").distinct().toPandas()
    segmentTypeControlPandas.index = segmentTypeControlPandas['segment_type_id']
    segmentTypeControlDict = segmentTypeControlPandas.to_dict()['module_type_id']
    return TestKeyModuleDict,segmentTypeControlDict,segmentsUnderTest,campaignsForTest,treatmentModuleList,moduleTypeTestFlag


TestKeyModuleDict,segmentTypeControlDict,segmentsUnderTest,campaignsForTest,treatmentModuleList,moduleTypeTestFlag = moduleTypeTestCreateDict()



def generateCampaignDict():

    col_list = ['campaign_id','slot_position','var_structure','var_source','var_position','module_id','module_type_id','module_priority_in_slot','segment_type_id','version','ttl_versions', 'source_connection']
    
    dict_cpgn = sc.broadcast(dfMetaCampaignData_VarDef[dfMetaCampaignData_VarDef.var_position.isNotNull()].withColumn('var_position',dfMetaCampaignData_VarDef.var_position.cast(StringType())).select(col_list).toPandas())
    
    if(module_version_test_flag == '1'):
        dict_cpgn_new = sc.broadcast(dfMetaCampaignData_VarDef_backup[dfMetaCampaignData_VarDef_backup.var_position.isNotNull()].withColumn('var_position',dfMetaCampaignData_VarDef_backup.var_position.cast(StringType())).select(col_list).toPandas())
    else:
        dict_cpgn_new = dict_cpgn

    return dict_cpgn_new,dict_cpgn
    
(dict_cpgn_new,dict_cpgn) = generateCampaignDict()

def posFilters():
    StartDate = get_pst_date()
    pos_info = (final_df_for_alpha.select("tpid","eapid","locale")
                    .filter("locale is not null and tpid is not null and eapid is not null").limit(1).rdd.first())

    pos_filter_cond = "locale = '" + pos_info["locale"] + "' and tpid = " + str(pos_info["tpid"]) + " and eapid = " + str(pos_info["eapid"])
    return pos_filter_cond
    
pos_filter_cond=posFilters()

global slots

def mapSlot():

    ### creating dictionary mapping for number of variable position in each slot
    slot_position_map = (dfMetaCampaignData_VarDef_backup
                                 .groupBy('slot_position')
                                 .agg({'var_position':'max'})
                                 .withColumnRenamed('max(var_position)','var_position')
                                 .rdd
                                 .collectAsMap())

    ### getting number of slots and list of slots
    slots = len(list(dict_cpgn.value.slot_position.unique()))
    slot_list = (list(dict_cpgn.value.slot_position.unique()))
    slot_list.sort()
    StartDate = get_pst_date()

    #Change 4: I now need information about campaign segment type id as well
    ### creating data frame with unique combination of campaign id, test keys and segment type 
    df_slot =(final_df_for_alpha
                            .select('campaign_id','test_keys','segment_type_id','campaign_segment_type_id')
                            .distinct()
                            .withColumn('number_slots',lit(slots))
                            .repartition(rep)
                            .cache())
                        
    if df_slot.count() <=0 :
        log_df_update(sqlContext,0,'df_slot creation',get_pst_date(),'check final_df_for_alpha ','0',StartDate,' ',AlphaProcessDetailsLog_str)
        log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
        log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
        code_completion_email("failed due to error in df_slot creation","Alpha process update for "+locale_name,pos,locale_name)
        raise Exception("Error in df_slot creation!!!")
    else :
        log_df_update(sqlContext,1,'df_slot creation',get_pst_date()," ",'0',StartDate,' ',AlphaProcessDetailsLog_str)

    return df_slot,slot_list,slots,slot_position_map
    
df_slot,slot_list,slots,slot_position_map = mapSlot()
print("created df slot successfully")


### function to assign module ids to each unique combination created above
def module_allocation(campaign_id,test_keys,segment_type_id,number_slots):

    import pandas as pd
    import datetime
    num_slots = number_slots
    test_keys =  int(test_keys)
    map_dict1 = {}
    segment_type_id_ls = [int(i) for i in segment_type_id.split("#")] #Listing out all the segment types

    for i in slot_list: 
             slot_position = i
             cpgn_sub = dict_cpgn.value[(dict_cpgn.value.campaign_id == str(campaign_id)) &
                                                    (dict_cpgn.value.slot_position == slot_position) ]

             cpgn_sub_final = cpgn_sub[(cpgn_sub['segment_type_id'].isin(segment_type_id_ls))]  #finding out whether segment type id is present in the list of segment types

             test = pd.DataFrame()

             module_types = list(cpgn_sub_final.module_type_id.unique()) #unique module types

             for module in module_types:

                    ttl_versions = int(cpgn_sub_final[(cpgn_sub_final.module_type_id == module)].iloc[0].ttl_versions) #one of the version
                    seed = datetime.date.today().isocalendar()[1] + datetime.datetime.today().weekday() #used to rotate these versions on a daily basis
                    version_num = int((int(test_keys)+ seed%(ttl_versions))%(ttl_versions)) + 1
                    test = test.append(cpgn_sub_final[(cpgn_sub_final.version==version_num) & (cpgn_sub_final.module_type_id == module)])  #appended to the test dataframe


             col_req = ['module_type_id','module_priority_in_slot']

             if len(test.index) == 0:
                    final_dict = {"dummy#dummy":"dummy"}   #create a dummy dictionary if the particular slot has nothing to be filled with

             else:  
                    test.index = (test[col_req]
                                         .astype(str)
                                         .apply(lambda x : '#'.join(x), axis=1))

                    test['module_id'] = test['module_id'].astype(str)

                    final_dict = test[['module_id']].to_dict()['module_id'] 
             map_dict1[str(i)] = final_dict
    
    return map_dict1  #map_dict1-{slot_position:{module_type_id##module_priority_in_slot:module_id}}

module_allocation_udf = udf(module_allocation,MapType(StringType(),MapType(StringType(),StringType())))


def moduleTypeModifier(campaign_id,test_keys,segment_type_id,campaign_segment_type_id,map_dict1):
    #Checking if a person is eligible for tests
    ## Through this function, beware of key not found errors. Especially for TestKeyModuleDict. I do not see why the key would be missing, but it is good to have a check just in case  
    removeAllTreatment = 0 # Un-used currently. Will be useful if a bug arises whose fix is to remove all treatment module types allcolated
    campaignNotEligible = 0 #Part of the safety net
    segmentNotEligible = 0 #Part of the safety net
    preserveModuleList = [] #Will be populated with the set of module types that should not be deleted for a traveler 
    removalList = set(treatmentModuleList) #Initially, we will assume that all modules introduced  as per the tests will be removed
    segmentIDList = segment_type_id.split("#") #List of all the segments a traveler would be eligible for
    activeSegmentsTraveler = set(segmentIDList).intersection(set(segmentsUnderTest)) #intersection of list of segments that the tests target and travelers eligible segments 
    if(campaign_id not in campaignsForTest):
        removeAllTreatment = 1 #Unused. Might be useful in case any bugs arise
        campaignNotEligible = 1 #Unused. Might be useful in case any bugs arise

    if(len(activeSegmentsTraveler) == 0):
        removeAllTreatment = 1 #Unused. Might be useful in case any bugs arise
        segmentNotEligible = 1 #Unused. Might be useful in case any bugs arise
        
    moduleTypeCounter = 0 #Unused
    for segmentTypeID in activeSegmentsTraveler:
        moduleKey = str(test_keys) + "#" +str(segmentTypeID) #Creating a key for each Traveler. This key will be used to track the module types allocated to him/her as per the tests
        for moduleTypeTemp in TestKeyModuleDict[moduleKey] :
            preserveModuleList.append(moduleTypeTemp) #If tests are designed on multiple segment type ID's, there will be multiple preserve modules
        removalList = (removalList - set(preserveModuleList)) #Has the list of modules that needs to be removed for the given traveler
        moduleTypeCounter = moduleTypeCounter + 1 #Unused
        #segmentTypeControlDict
        
    new_dict = {} #Will contain the new dictionary after removing the extra module types. This will be at a slot level
    removalList = [str(i) for i in removalList]
    for slot in map_dict1:
        moduleAllocationDict = map_dict1[slot] #Obtaining the original allocation
        moduleDict = {} #Will contain populated dictionaries for each slot. Hence, it is refreshed to 0 for each slot 
        for moduleTypePrio in moduleAllocationDict:
            moduleType = moduleTypePrio.split("#")[0]
            if moduleType not in removalList: #Will populate moduleDict only if the given module type is not in the blacklist for this traveler. Hence, removing extra module types 
                moduleDict[moduleTypePrio] = moduleAllocationDict[moduleTypePrio]
        new_dict[slot] = moduleDict #Updating the dictionary for the given slot
        
    #Fall back situation. This is when the new dictionary has zero entries. This happens if there are unequal placement types between control and treatment
    slotClearList = []
    for slot in new_dict:
        newmoduleAllocationDict = new_dict[slot]
        moduleDict = {}
        priorityList = []
        slotRefreshFlag = 0
        if len(newmoduleAllocationDict) == 0 : #In the event where a slot has zero module types, We will repopulate it with control 
            new_dict[slot] = map_dict1[slot]
    return(new_dict)


moduleTypeModifierUdf = udf(moduleTypeModifier,MapType(StringType(),MapType(StringType(),StringType())))


def module_allocation_modifier(campaign_id,test_keys,campaign_segment_type_id,map_dict1,segment_type_id,test_segments):
    key_version = test_keys+"#"+campaign_segment_type_id+"#"+campaign_id
    try:
        temp = str(lookup_dict[key_version])
    except:
        temp = "dont_touch"
    eligible_segments = segment_type_id.split("#")
    test_segments = test_segments.split("#")
    possible_segment_type_ids = set(eligible_segments).intersection(set(test_segments))
    if(len(possible_segment_type_ids) == 0):
        return(map_dict1)
    
    if (temp == "dont_touch"):
        return map_dict1
    else:
        counter = 0
        temp_slot_dict = {}
        for segment_type_id_temp in possible_segment_type_ids :
            if(counter == 0):
                for slot in map_dict1:
                    temp_map_dict = {}
                    for mod_id__priority in map_dict1[slot]:
                        mod_id = mod_id__priority.split("#")[0]
                        priority = mod_id__priority.split("#")[1]
                        version_replacement_id = test_keys+"#"+campaign_segment_type_id+"#"+campaign_id+"#"+mod_id+"#"+str(slot)+"#"+segment_type_id_temp
                        try:
                            replacement_version_id = str(lookup_dict_control[version_replacement_id]) #IF the key is not found, it goes to except
                            if(placement_modid_dict[int(replacement_version_id)] == placement_modid_dict[int(map_dict1[slot][mod_id__priority])]):
                                temp_map_dict[mod_id__priority] = replacement_version_id
                            else:
                                temp_map_dict[mod_id__priority] = map_dict1[slot][mod_id__priority]
                        except:
                            temp_map_dict[mod_id__priority] = map_dict1[slot][mod_id__priority]
                    temp_slot_dict[slot] = temp_map_dict
                    counter = counter + 1
            else:
                for slot in temp_slot_dict:
                    temp_map_dict = {}
                    for mod_id__priority in map_dict1[slot]:
                        mod_id = mod_id__priority.split("#")[0]
                        priority = mod_id__priority.split("#")[1]
                        version_replacement_id = test_keys+"#"+campaign_segment_type_id+"#"+campaign_id+"#"+mod_id+"#"+str(slot)+"#"+segment_type_id_temp
                        try:
                            replacement_version_id = str(lookup_dict_control[version_replacement_id]) #IF the key is not found, it goes to except
                            if(placement_modid_dict[int(replacement_version_id)] == placement_modid_dict[int(map_dict1[slot][mod_id__priority])]):
                                temp_map_dict[mod_id__priority] = replacement_version_id
                            else:
                                temp_map_dict[mod_id__priority] = temp_slot_dict[slot][mod_id__priority]
                        except:
                            temp_map_dict[mod_id__priority] = temp_slot_dict[slot][mod_id__priority]
                    temp_slot_dict[slot] = temp_map_dict
                    counter = counter + 1

    return(temp_slot_dict)
    
module_allocation_modifier_udf = udf(module_allocation_modifier,MapType(StringType(),MapType(StringType(),StringType())))


def module_info(map_dict1):
    map_dict = {}
    for slot in map_dict1:
             for key in map_dict1[slot]:
                    module_id = map_dict1[slot][key]
                    
                    if module_id == "dummy":
                         test_dict = {"dummy#dummy":"dummy"}
                    
                    else:
                         test = dict_cpgn_new.value[dict_cpgn_new.value.module_id == int(module_id)][['var_source',
                                                                                                      'var_position',
                                                                                                      'var_structure', 'source_connection']]
                         col_req = ['var_source','var_position', 'source_connection']
                         test.index = (test[col_req]
                                                .astype(str)
                                                .apply(lambda x : '#'.join(x), axis=1))
                         test_dict = test[['var_structure']].to_dict()['var_structure']
                    map_dict[str(module_id)] = test_dict #map_dict- {module_id:{var_source##var_position:var_structure}}

    return map_dict
    
module_info_udf = udf(module_info,MapType(StringType(),MapType(StringType(),StringType())))



def slotFun():

    df_slot_fun = (df_slot.withColumn('map_dict1',module_allocation_udf('campaign_id','test_keys','segment_type_id','number_slots')).cache()) #adds map_dict1
    print("------------Finished fun function")
    if df_slot_fun.count() <=0 :
        log_df_update(sqlContext,0,'Module allocation function completed',get_pst_date(),'check df_slot and dict_cpgn tables','0',StartDate,' ',AlphaProcessDetailsLog_str)
        log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
        log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
        code_completion_email("failed due to error in module allocation function","Alpha process update for "+locale_name,pos,locale_name)
        raise Exception("Error in module allocation function!!!")
    else :
        log_df_update(sqlContext,1,'Module allocation function completed',get_pst_date()," ",'0',StartDate,' ',AlphaProcessDetailsLog_str)

    if(moduleTypeTestFlag == 1):
        df_slot_fun_new = df_slot_fun.withColumn("new_dict",moduleTypeModifierUdf("campaign_id","test_keys","segment_type_id","campaign_segment_type_id","map_dict1"))
        print("------------Module type test detected. Now removing extra module types")
        df_slot_fun_new = df_slot_fun_new.drop("map_dict1")
        df_slot_fun_new = df_slot_fun_new.withColumnRenamed("new_dict","map_dict1")
        df_slot_fun = df_slot_fun_new
#       df_slot_fun_new.show(100,False)
        
    if(module_version_test_flag == '1'):
        print("------------Module version test detected. Now replacing modules wherever possible")
        test_segments = final_df.select("segment_type_id").distinct().rdd.flatMap(lambda x:x).collect()
        test_segments = [str(i) for i in test_segments]
        test_segments = ["#".join(test_segments)]
        df_slot_fun = df_slot_fun.withColumn("test_segments",lit(test_segments[0]))
        df_slot_fun_new = df_slot_fun.withColumn("new_dict",module_allocation_modifier_udf("campaign_id","test_keys","campaign_segment_type_id","map_dict1","segment_type_id","test_segments"))
        df_slot_fun_new = df_slot_fun_new.drop("map_dict1")
        df_slot_fun_new = df_slot_fun_new.withColumnRenamed("new_dict","map_dict1")
        df_slot_fun = df_slot_fun_new
        df_slot_fun.cache()

    return df_slot_fun
    
df_slot_fun = slotFun()


def slotFunMod(df_slot_fun):

    StartDate = get_pst_date()

    df_slot_fun_module = (df_slot_fun.withColumn('map_dict',module_info_udf('map_dict1')).repartition(rep).cache())
    #df_slot_fun_module adds a column map_dict
    if df_slot_fun_module.count() <=0 :
        log_df_update(sqlContext,0,'Module info function completed',get_pst_date(),'check df_slot and dict_cpgn tables','0',StartDate,' ',AlphaProcessDetailsLog_str)
        log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
        log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
        code_completion_email("failed due to error in module info function","Alpha process update for "+locale_name,pos,locale_name)
        raise Exception("Error in module info function!!!")
    else :
        log_df_update(sqlContext,1,'Module info function completed',get_pst_date()," ",'0',StartDate,' ',AlphaProcessDetailsLog_str)


    return df_slot_fun_module
     
df_slot_fun_module = slotFunMod(df_slot_fun)



global final_travel_cols

def campaignPriorityDict():

    ### subsetting columns required from traveler data and creating key by concatenating the required columns
    StartDate = get_pst_date()
    
    final_travel_cols_df=sqlContext.read.parquet(path_dict['OcelotDataProcessing_Output']+"final_travel_cols")

    final_travel_cols=final_travel_cols_df.collect()[0]['final_travel_cols']
    final_travel_cols = list(set(final_travel_cols+['campaign_id','test_keys','segment_type_id']))


    #final_travel_cols = list(set(joining_cols_final+content_cols_final+['campaign_id','test_keys','segment_type_id']))
    
    final_df_for_alpha_key = final_df_for_alpha.withColumn('key',concat_ws('_',*final_travel_cols))
    
    traveler_data_content = (final_df_for_alpha_key.select(final_travel_cols+['key']).distinct().repartition(rep).cache())
    
    ## meta data creation for campaign suppression, dictionary with default flag and campaign priority
    cols_suppression = ['module_id','campaign_id','priority','placement_type','module_type','default']
    
    #Change 3: We need information about test published modules again
    df_suppre = (dfMetaCampaignData_VarDef_backup.withColumn("default", dfMetaCampaignData_VarDef_backup["default"].cast(IntegerType()))
                    .select(cols_suppression).distinct().toPandas())  #Business understanding. Default column with binary values

    df_suppre.index = df_suppre["module_id"]
    
    suppress_dict = df_suppre.to_dict()
    
    cpgn_prio_pd = dfMetaCampaignData_VarDef_backup.select('campaign_id','priority').distinct().toPandas()
    cpgn_prio_pd.index = cpgn_prio_pd['campaign_id']
    
    cpgn_prio_dict = cpgn_prio_pd.to_dict()['priority']
    
    return cpgn_prio_dict,traveler_data_content,suppress_dict,final_df_for_alpha_key
    
(cpgn_prio_dict,traveler_data_content,suppress_dict,final_df_for_alpha_key) = campaignPriorityDict()

### populating the value in each slot position
##Populating None instead of default content
import json
def set_dynamic_content_value(dictionary, key, value):
    unacceptable_values = ['value_not_found', 'nan', 'none']
    comparison_value = value.lower()
    if(not (comparison_value in unacceptable_values)):
        dictionary[key] = value


def content_map(row):

    import datetime

    seed = datetime.date.today().isocalendar()[1] + datetime.datetime.today().weekday()
    dict_req = {'key':row['key']}
    cpgn_id = row['campaign_id']
    number_slots = row['number_slots']
    OmniExtension = ""
    campaign_priority_ls = []
    module_id_ls = []
    dynamic_content_value = {}

    for slot_position in slot_list:
        
        
        dynamic_content_for_slot = {}
        map_dict1 = row['map_dict1'][str(slot_position)]  #moduletypeid##priority->moduleid
        slot_position = str(slot_position)
        df = pd.DataFrame()
        map_dict1 = {key_mod:map_dict1[key_mod] for key_mod in map_dict1 if map_dict1[key_mod] not in module_id_ls} #trying to map for each unique module id
        flag_dummy = 0

        if (len(map_dict1.keys())>0) and (list(map_dict1.keys())[0].find("dummy")>=0)  :flag_dummy = 1


        if flag_dummy == 0:
            for key1 in  map_dict1:
                map_dict = row['map_dict'][map_dict1[key1]] #var

                var_positions = slot_position_map[int(slot_position)]

                x_dict = {('S'+slot_position+'_P'+str(int(v))):'' for v in range(1,var_positions+1)}  #{S1P1:'',S1P2:''...}

                x_dict['S'+slot_position+'_module_priority'] = int(key1.split('#')[1]) #Gets the module priority as the value
                x_dict['S'+slot_position+'_module_id'] = int(map_dict1[key1])  #Gets the module id as the value. This is where if there are two values(module_type_id eg.), they are mapped randomly
                x_dict['S'+slot_position+'_att_option'] = ''

                for key in map_dict:

                    join_key =  key.split('#')[0]
                    var_pos =str(key.split('#')[1])
                    var_source_connection = key.split('#')[2]
                    column_values = []
                    is_dynamic_column_present = False

                    slot_var_pos = 'S'+slot_position+'_P'+(var_pos) #Assigning the Slot_Pos Name

                    if( len(join_key.split('|')) > 1):
                        file_name = (join_key.split('|')[1].split(';')[0].split('.')[0]).lower()
                        left_keys = [col.split('.')[1] for col in join_key.split('|')[0].split(';')]
                        left_keys=[i.lower() for i in left_keys]
                        right_keys = [col.split('.')[1] for col in join_key.split('|')[1].split(';')]
                        right_keys=[i.lower() for i in right_keys]


                    else :
                        file_name =  'NA'
                        left_keys = 'NA'
                        right_keys = 'NA'

                    if map_dict[key] == None :
                        var_value = ""

                    elif (map_dict[key] != None) and (len(map_dict[key]) == 0) :
                        var_value = ""

                    elif(len(map_dict[key].split('%%')) == 1):
                        var_value = map_dict[key]

                    else :
                        z = [m.start() for m in re.finditer('%%', map_dict[key])]  #gives the index where '%%' is present in the form of a list
                        var_value = map_dict[key] #varsource##varposition->varstructure

                        j = 0
                        while (j <= len(z)-2):
                            temp = map_dict[key][z[j]+2:z[j+1]] # eg. traveler.origincity
                            df_name = temp.split('.')[0] #table
                            attribute_name = str(temp.split('.')[1])

                            if (df_name == 'traveler'): #when the table name is traveler
                                trav_attri = row[temp.split('.')[1]]   #getting the column name
                                if trav_attri == '':
                                    trav_attri = "value_not_found"
                                if(trav_attri is None):
                                    trav_attri = "value_not_found"
                                var_value = var_value.replace(temp,str(trav_attri))   #replacing the temp string in
                                # var_value with trav_attri which is the column name
                                set_dynamic_content_value(dynamic_content_value, "{}_{}_{}_1".format(str(x_dict[
                                                                                                             'S'+slot_position+'_module_id']),str(slot_var_pos),
                                                                                                     attribute_name),str(trav_attri))
                            else :

                                travel_data = "".join([str(row[i]) for i in left_keys ])  #Gets the data from the columns mentioned in left_keys
                                mod_id_map = x_dict['S'+slot_position+'_module_id']  #module id
                                try :
                                    value_re = mis_content_dict[str(mod_id_map)+df_name.lower()+''.join(right_keys)+var_pos][temp.split('.')[1]][travel_data]
                                #value_re contains the data/value. It first accesses the dictionary, finds the column and obtains the data for the column
                                except : value_re = ''
                                if value_re == '':
                                    value_re = "value_not_found"
                                if value_re is None:
                                    value_re = "value_not_found"
                                var_value = var_value.replace(temp,str(value_re))
                                set_dynamic_content_value(dynamic_content_value,"{}_{}_{}_{}".format(str(x_dict[
                                                                                                             'S'+slot_position+'_module_id']),str(slot_var_pos),
                                                                                                     attribute_name,var_source_connection), str(value_re))
                            j+=2

                    x_total = var_value.replace('%%','')

                    ttl_options = len(x_total.split("|"))  #no. of options

                    if (ttl_options > 2):
                        att = int((int(row['test_keys'])+ seed%(ttl_options-1))%(ttl_options-1))  #changes on a daily basis.
                    else:
                        att = 0

                    x_dict[slot_var_pos] = x_total.split('|')[0]


                    if (ttl_options<=1):
                        if ( slot_var_pos == "S1_P1"):
                            x_dict[slot_var_pos] = x_total.split('|')[0]       #for att 'default' it has been changed to 'None'
                        else:
                            x_dict[slot_var_pos] = "None"      #for att 'default' it has been changed to 'None'
                        att = 'default'
                    elif ((ttl_options==2)):
                        x_dict[slot_var_pos] = x_total.split('|')[1]
                    else :
                        list_value = x_total.split('|')[1:]
                        x_dict[slot_var_pos] = list_value[att]

                    x = x_dict[slot_var_pos]
                    placement_type_mp = suppress_dict['placement_type'][int(x_dict['S'+slot_position+'_module_id'])]
                    default_flag_mp = suppress_dict['default'][int(x_dict['S'+slot_position+'_module_id'])]

                    if ((x.lower().find('value_not_found') >= 0) or (x.lower().find('none') >= 0  and len(x) == 4) or (x.lower().find('nan') >= 0) or (x.lower().find('null') >= 0) ):
                        if ( slot_var_pos == "S1_P1"):
                            x_dict[slot_var_pos] = x_total.split('|')[0]       #for att 'default' it has been changed to 'None'
                        else:
                            x_dict[slot_var_pos] = "None"      #for att 'default' it has been changed to 'None'
                        att = 'default'
                        if ((placement_type_mp in ('hero','banner')) and (default_flag_mp == 0)) :
                            x_dict['S'+slot_position+'_module_priority'] = 9999


                    x_dict['S'+slot_position+'_module_type_id'] = str(key1.split('#')[0])

                    x_dict['S'+slot_position+'_att_option'] = x_dict['S'+slot_position+'_att_option'] +'#'+slot_var_pos+'.'+str(att)


                temp = pd.DataFrame(x_dict,index=[x_dict['S'+slot_position+'_module_priority']])
                df = df.append(temp)

            #here module_type_id can be randomly allocated as it doesn't contain the logic of choosing the module_type_id based on its priority as can be found in the else function below
            #sometimes the slot might be empty having no modules or dataframe
            if len(df) == 0:
                var_positions = slot_position_map[int(slot_position)]
                x_dict = {('S'+slot_position+'_P'+str(int(v))):"None" for v in range(1,var_positions+1)}  #slot var pos has been changed to none
                x_dict['S'+slot_position+'_module_priority'] = "pixel_module"
                mod_id = '999'
                x_dict['S'+slot_position+'_module_id'] = int(mod_id)   #haven't changed pixel_module to null. only slot var position has to be changed
                x_dict['S'+slot_position+'_att_option'] = "pixel_module"
                x_dict['S'+slot_position+'_module_type_id'] = "pixel_module"
                campaign_priority = int(cpgn_prio_dict[cpgn_id])  #campaign id is mapped to campaign priority
                campaign_priority_ls += [campaign_priority] #List of campaign priority is made
                dict_req.update(x_dict)

            else :
                final_df = df.loc[[df['S'+slot_position+'_module_priority'].idxmin()]]
                col_name = list(final_df['S'+slot_position+'_module_priority'])[0]
                mod_id = final_df['S'+slot_position+'_module_id'].iloc[0]
                final_dict = final_df.transpose().to_dict()[col_name]
                module_id = str(final_dict['S{}_module_id'.format(slot_position)])
                placement_type = suppress_dict['placement_type'][int(module_id)]
                module_type = suppress_dict['module_type'][int(module_id)]
                default_flag = suppress_dict['default'][int(module_id)]
                campaign_priority = int(cpgn_prio_dict[cpgn_id])

                if final_df['S'+slot_position+'_module_priority'].iloc[0] == 9999 :

                    if ((default_flag == 0) and (placement_type == 'hero')) :
                        campaign_priority = 9999
                    elif ((default_flag == 0) and (placement_type == 'banner')):
                        mod_id = '999'
                        final_dict['S'+slot_position+'_module_id'] = int(mod_id)
                        for i in range(1,var_positions+1):
                            final_dict['S'+slot_position+'_P'+str(int(i))] = "None" #has been changed to 'None' for default flag =0 and placement_type = 'banner'

                campaign_priority_ls += [campaign_priority]
                dict_req.update(final_dict)

        else :   #filling the slots where flag_dummy=1
            var_positions = slot_position_map[int(slot_position)]
            x_dict = {('S'+slot_position+'_P'+str(int(v))):"None" for v in range(1,var_positions+1)}    #when flag_dummy =1 then slot var pos has been changed to 'None'
            x_dict['S'+slot_position+'_module_priority'] = "None"
            mod_id = "999"        #dummy still exists for module priority, att option and module type id
            x_dict['S'+slot_position+'_module_id'] = int(mod_id)
            x_dict['S'+slot_position+'_att_option'] = "None"
            x_dict['S'+slot_position+'_module_type_id'] = "None"
            campaign_priority = int(cpgn_prio_dict[cpgn_id])
            campaign_priority_ls += [campaign_priority]
            dict_req.update(x_dict)

        if (int(slot_position) < number_slots): OmniExtension += (slot_position+'.'+str(mod_id)+'_')
        else : OmniExtension += (slot_position+'.'+str(mod_id))
        module_id_ls.append(str(mod_id))
        #Removing the columns for Slot_postion_Map_Position as they are no longer required as per https://jira.sea.corp.expecn.com/jira/browse/RMS-16674
        for i in range(1, var_positions+1):
            key = 'S'+slot_position+'_P'+str(int(i))
            if(key != 'S1_P1'):
                dict_req.pop(key)

    #dict_req['OmniExtension'] = OmniExtension
    dict_req['OmniExtNewFormat'] = OmniExtension.replace('.','-')

    campaign_priority_ls.sort(reverse=True)

    dict_req['campaign_priority'] = campaign_priority_ls[0]
    dictionary_with_filtered_content = {}
    selected_modules = []
    for i in dict_req['OmniExtNewFormat'].split("_"):
        module_id = i.split("-")[1]
        if(module_id != '999'):
            selected_modules.append(module_id)
    selected_slot_positions = []
    for slot_position in slot_list:
        att_options = str(dict_req["S" + str(slot_position)+ "_att_option"]).split("#")
        if(len(att_options) > 1):
            for i in range(1,len(att_options)):
                selected_slot_positions.append(str(att_options[i]).split(".")[0])

    for key in dynamic_content_value:
        for module_id in selected_modules:
            if(key.startswith(module_id)):
                value = dynamic_content_value[key]
                key_without_prefix = key[(len(module_id)+1):]
                for att_option in selected_slot_positions:
                    if(key_without_prefix.startswith(att_option) and (not key_without_prefix[(len(
                            att_option)+1):].startswith("_"))):
                        value = dynamic_content_value[key]
                        dictionary_with_filtered_content[key_without_prefix[(len(att_option)+1):].lower()] = value


    dict_req['dynamic_content'] = json.dumps(dictionary_with_filtered_content, ensure_ascii=False)

    return Row(**dict_req)



#function converts campaign_id from string back to int
def int_maker(campaign_id):
    id = int(campaign_id)
    return(id)
    
int_maker_udf = udf(int_maker,IntegerType())



def generateFinalResult():
    
    startDate = get_pst_date()
    content_map_data = (traveler_data_content.join(df_slot_fun_module,['campaign_id','test_keys','segment_type_id'],'inner').repartition(rep).cache())

    ## defining final col list required in alpha output
    cols_occ = ['tpid','eapid','email_address','expuserid','first_name','mer_status','lang_id','last_name','locale','paid','test_keys']

    cols_occ_final = cols_occ + ['campaign_id','key']
    final_df_for_alpha_key_occ = final_df_for_alpha_key.select(cols_occ_final)

    ### Applying campaign suppression filter and assigning campaign according to the priority
    window_rank = Window.partitionBy("email_address").orderBy("campaign_priority")

    print("------------calling content map function")
    try :
        #grab only campaign_id, locale, LaunchDateTimeUTC
        #dfMetaCampaignData1 is already filtered for campaigns going that day
        
        dfCampaginLaunchData = (dfMetaCampaignData1.select("campaign_id", "locale", "LaunchDateTimeUTC"))
        final_result = (content_map_data.withColumn('test_keys',content_map_data['test_keys'].cast(IntegerType()))
                            .rdd.map(lambda x : content_map(x)).toDF()
                            .join(final_df_for_alpha_key_occ,'key','inner').drop('key')
                            .withColumn('ModuleCount',lit(slots))         #adds number of slots
                            .join(dfCampaginLaunchData, ["campaign_id","locale"],"inner")
                            .withColumn("cpgn_rank",rank().over(window_rank))   #rank
                            .filter("cpgn_rank = 1").drop("cpgn_rank")
                            .filter("campaign_priority != 9999")
                            .filter("S1_module_id!=999") ### removing users having no module mapping for subjectline 07/06/2017
                            .filter("S2_module_id!=999") ### removing users having no module mapping for hero 07/06/2017
                            .repartition(rep)) 
            
            
        
        final_result = final_result.withColumn("campaign_id",int_maker_udf("campaign_id"))
            #final_result.cache()

        log_rowcount = final_result.count()
        print("log_rowcount -> ",log_rowcount)
            
        log_df_update(sqlContext,1,'Content map function completed',get_pst_date(),' ',str(log_rowcount),StartDate,' ',AlphaProcessDetailsLog_str)
    
    except :
        log_df_update(sqlContext,0,'Content map function completed',get_pst_date(),'Error in content map function','0',StartDate,' ',AlphaProcessDetailsLog_str)
        log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
        log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
        code_completion_email("failed due to error in content map","Alpha process update for "+locale_name,pos,locale_name)
        raise Exception("Error in content map!!!")
        
    return final_result
        

                             
final_result = generateFinalResult()
#final_result.write.mode("overwrite").parquet("s3://occ-decisionengine/MetaDataCreation_Output/finalResult")



### Adding Seed Emails Starts Here ###

def SeedEmailAddition(final_result_moduleCount):

    startDate = get_pst_date()
    seeds = (importSQLTable("AlphaStaging","AlphaSeedEmails")).filter("CampaignCategory = 'Merch' and IsActive = 1")
    SeedWindow = Window.orderBy("SeedEmail")
    AlphaOutputWindow = Window.partitionBy("campaign_id").orderBy("test_keys","expuserid")
    seedEmails = (seeds.filter(pos_filter_cond).select("tpid","eapid","locale","SeedEmail")
                    .distinct().withColumn("row_id1",row_number().over(SeedWindow)))
                    
    seedCounts = seedEmails.count()
    #print("seedCounts ->",seedCounts)
    sampleForSeed = (final_result_moduleCount.withColumn("row_id1",row_number().over(AlphaOutputWindow))).filter("row_id1 <= "+str(seedCounts))
    
    sampleAfterSeed = (sampleForSeed.join(broadcast(seedEmails), ["tpid","eapid","locale","row_id1"], "inner")
                                    .drop("row_id1").drop("email_address")
                                    .withColumnRenamed("SeedEmail","email_address")
                                    .withColumn("PAID",lit(999999999)))
                                    
    finalOutputColumns = [col for col in final_result_moduleCount.columns]
    
    #print(finalOutputColumns)
    sampleAfterSeed = sampleAfterSeed.select(finalOutputColumns)
    final_result_moduleCount = final_result_moduleCount.select(finalOutputColumns).unionAll(sampleAfterSeed)
    log_rowcount = final_result_moduleCount.count()
    
    log_df_update(sqlContext,1,'Seed Emails added',get_pst_date(),' ',str(log_rowcount),StartDate,' ',AlphaProcessDetailsLog_str)
    
    return final_result_moduleCount
    


print("------------Seed Emails added")
### Adding Seed Emails Ends Here ###    


print("------------Seed Emails added")
### Adding Seed Emails Ends Here ###




def generateFinalResultModuleCount(final_result,dfLoyalty):

    StartDate = get_pst_date()
    ##Converting None to null type
    from pyspark.sql.functions import col, when


    final_RID_path = path_dict['RecipientID_Output']+"final_df_for_alpha"
    userTokens_path = path_dict['User_Token_Output']+"final_df_for_alpha"

    final_RID = sqlContext.read.parquet(final_RID_path)
    final_RID = final_RID.distinct()
    #print("final result count before userToken ->",final_result.count())
    
    final_UserTokens =  sqlContext.read.parquet(userTokens_path)
    
    #final_UserTokens = final_UserTokens.distinct()
    final_result=(final_result.join(final_UserTokens,["paid","email_address","campaign_id"],"left"))
    #print("final result count after userToken and before seeds ->",final_result.count())
    #print("paid 999999999 count b4 seeds->",final_result.filter(final_result["paid"] == 999999999).count())
    
    print("------------Calling module count map")
    module_count_map = (dfMetaCampaignData_VarDef.groupBy("campaign_id","AudienceTableName").agg(countDistinct(dfMetaCampaignData_VarDef.slot_position).alias("ModuleCount")))
    
    final_result_moduleCount1 = final_result.drop("ModuleCount").join(broadcast(module_count_map),"campaign_id","inner").dropDuplicates(["email_address", "paid"])
    final_result_moduleCount1.cache()
    #final_result_moduleCount1.limit(1).show()
    #print("final_result_moduleCount1 ",final_result_moduleCount1.count())
    #print("paid 999999999 count after  module_count_map->",final_result_moduleCount1.filter(final_result_moduleCount1["paid"] == 999999999).count())
    
    log_rowcount = final_result_moduleCount1.count()
    log_df_update(sqlContext,1,'Duplicates have been dropped',get_pst_date(),' ',str(log_rowcount),StartDate,' ',AlphaProcessDetailsLog_str)
    
    print("------------Duplicates dropped")

    ### Default Value has been NULLED ###

    StartDate = get_pst_date()


    #function to replace None with null
    def blank_as_null(x):
        return when(col(x) == "None", None).otherwise(col(x))
    
    #taking the distinct columns from final_result_moduleCount1  
    final_result_moduleCount1_distinct_columns = set(final_result_moduleCount1.columns) # Some set of columns
    
    exprs = [blank_as_null(x).alias(x) if x in final_result_moduleCount1_distinct_columns else x for x in final_result_moduleCount1.columns]
    
    
    final_result_moduleCount = final_result_moduleCount1.select(*exprs)
    

    final_result_moduleCount.repartition(rep)
    
    final_result_moduleCount.cache()
    log_rowcount = final_result_moduleCount.count()
    
    log_df_update(sqlContext,1,'Default Value nulled',get_pst_date(),' ','0',StartDate,' ',AlphaProcessDetailsLog_str)
    
    print("------------Default Value NULLED")
    
    ### Default Value has been NULLED ###
    
    final_result_moduleCount = SeedEmailAddition(final_result_moduleCount)
    
    #print("final result count after seeds ->",final_result_moduleCount.count())
    #print("paid 999999999 count after seeds->",final_result_moduleCount.filter(final_result_moduleCount["paid"] == 999999999).count())
    
    #dfLoyalty = dfLoyalty.distinct()

    final_result_moduleCount=(final_result_moduleCount.join(dfLoyalty,["tpid","paid","campaign_id"],"left")
                                                        .join(final_RID,["paid","email_address","campaign_id"],"left"))
                        

    #print("final result count after recipient and loyalty ->",final_result_moduleCount.count())
    #print("paid 999999999 count after recipient and loyalty ->",final_result_moduleCount.filter(final_result_moduleCount["paid"] == 999999999).count())                    

    final_result_moduleCount = (final_result_moduleCount.withColumn("RecipientID1" ,concat(col("RecipientID"),lit(""),col("OmniExtNewFormat")))
                        .drop("RecipientID")
                        .withColumnRenamed("RecipientID1","RecipientID"))    
    
    
    #print("final result count b4 duplicates ->",final_result_moduleCount.count())
    #print("paid 999999999 count ab4 duplicates ->",final_result_moduleCount.filter(final_result_moduleCount["paid"] == 999999999).count())
    
    ## Dropping unwanted columns
    final_result_moduleCount = (final_result_moduleCount
        .drop("AudienceTableName")
        .drop("LRMStatus")
        .drop("campaign_priority"))
    
    
    print("------------Dropping unwanted columns")
    
    
    print("------------Loacale: ", search_string)
    
    final_result_moduleCount = final_result_moduleCount.drop("locale").withColumn("Locale",lit(search_string))
    
    return module_count_map,final_result_moduleCount
    
(module_count_map,final_result_moduleCount) = generateFinalResultModuleCount(final_result,dfLoyalty)




###Creation of dictionaries for the inclusion of Test ID and Version in OmniCode/RecipientID

try:

    def DictCreation_TestVers(final_df):
        tests_on_current_campaign=final_df.select("test_id").distinct().rdd.flatMap(lambda x: x).collect()
        moduletypeMappingDict={}
        slotModType_dict={}
        testIdModule_dict={}
        testKeysVers_dict={}

        if(module_version_test_flag == '1'):
            

            for i in tests_on_current_campaign: #tests_on_current_campaign is a list containing the test ids of all the active module version tests
                newdict2={}
                filter_cond="test_id="+str(i)   #This is done for one test at a time
                newdf=final_df.filter(filter_cond)  #The final_df (containing the sampling output) is filtered for that test id
                modid_list=newdf.select("module_id").distinct().rdd.flatMap(lambda x:x).collect()   #list of all the test published module ids for that test id containing Null
                modid_list=[int(i) for i in modid_list if i is not None]    #list of all the test published module ids for that test id not containing Null
                newdict2=newdf.select(["test_keys","Control_Test_Flag"]).distinct().toPandas().set_index("test_keys").to_dict()["Control_Test_Flag"]    #dictionary containing key as the test key and the value as the control test flag/treatment group
                
                
                testIdModule_dict[i]=modid_list   #dictionary with key as test id and value as list of test published modules for that test
                testKeysVers_dict[i]=newdict2     #dictionary with key as test id and value as another dictionary newdict2 which has test key to treatment group mapping for every test key
                
            moduletypeMappingDict=dfMetaCampaignData_VarDef.select(["module_id","module_type_id"]).distinct().toPandas().set_index("module_id").to_dict()["module_type_id"]
            #moduletypeMappingDict is a dictionary having module id to module type mapping for all the active published modules

            df=final_df.select(["module_type_id","slot_position","test_id"]).filter("slot_position is not Null").distinct().toPandas()
            col_req=["slot_position","module_type_id"]
            df.index = (df[col_req].astype(str).apply(lambda x : '#'.join(x), axis=1))
            slotModType_dict = df[['test_id']].to_dict()['test_id']
            #slotModType_dict is a dictionary with key of format slot#module_type_id and value as the test id
            #This dictionary is made so that for travelers not getting a test published module id we can figure out the test he is falling in control group for
            #by using the module type and slot combination to uniquely identify the test id
            
        return (testIdModule_dict,testKeysVers_dict,moduletypeMappingDict,slotModType_dict)
        
    (testIdModule_dict,testKeysVers_dict,moduletypeMappingDict,slotModType_dict)=DictCreation_TestVers(final_df)

except:
    log_df_update(sqlContext,0,'Error in creating dictionary for the introduction of Test and Version in Recipient ID',get_pst_date(),' ','0',StartDate,' ',AlphaProcessDetailsLog_str)
    log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
    log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
    code_completion_email("failed due to error in creating dictionary for Test and Version in Recipient ID","Alpha process update for "+locale_name,pos,locale_name)
    raise Exception("Error in creating dictionary for the introduction of Test and Version in Recipient ID!!!")


###function to add a column containing the Test ID and Vers in the format test_id#Vers

try:


    def testIdMap(OmniExtNewFormat,test_keys):
        
     
        modules_list=list(set(int(i.split("-")[1]) for i in OmniExtNewFormat.split("_")))   #creating a list of modules assigned to the traveller, contains 999
        modules_list=[i for i in modules_list if i !=999]   #removing 999 from the list of modules
        tests_list=[]
        
        for i in testIdModule_dict:
            test_mods=list(set(testIdModule_dict[i]).intersection(set(modules_list)))   
            #creating a list of modules ids which are test published and are being assigned to travellers and reverse mapping the module id to test id
            if(len(test_mods)!=0):
                tests_list.append(i)    #tests_list is a list with all the tests that are running on the traveler, ideally the length should not be more than 1
        
        if(len(tests_list)==0):
           
           #the length of tests_list is 0 only when the traveler is not eligible for a test or he falls in the control group for the tests
            
            
            a=[i for i in OmniExtNewFormat.split("_")] #a is a combination of slot and module id
            moduleTypes_Slot_list=[i.replace(i.split("-")[1],str(moduletypeMappingDict[int(i.split("-")[1])])) for i in a if i.split("-")[1]!="999"]
            moduleTypes_Slot_list=[i.replace("-","#") for i in moduleTypes_Slot_list]
            #moduleTypes_Slot_list is list with elements in format slot_position#module_type_id and here moduletypeMappingDict is used to replace module_id with module_type_id
            testForControl=[]
            for i in slotModType_dict:  #iterating through every slot_position#module_type_id for tests
                if (i in moduleTypes_Slot_list):
                    testForControl.append(slotModType_dict[i])  #testForControl will have all the tests that the person is eligible for but for which he falls in control group
            if(len(testForControl)!=0):
                Test = random.choice(testForControl)    #if a traveler is eligible and falls in control group for more than one test than the test_id is chosen randomly
                Vers="0"    #Vers for control is 0
            else:
                Test=1      #if a traveler is not getting a test published module and he is not under control group then he is not eligible for that test so default value is plugged
                Vers="X"    #Vers for default is X

            
            

        elif(len(tests_list)>=1):   #this means that a traveler is getting a test published module id
            Test = random.choice(tests_list)    #the test-id is randomly chosen if more than one tests are running on him
            Vers=testKeysVers_dict[Test][int(test_keys)]    #Vers for that test is selected by using testKeysVers_dict shich maps the test key to treatment group/Vers
        
        x=str(Test)+"#"+str(Vers)

            
        return x
            
    testIdMap_udf=udf(testIdMap,StringType())
    
    if(module_version_test_flag == '1'):  
        final_result_moduleCount=final_result_moduleCount.withColumn("Test_Vers",testIdMap_udf("OmniExtNewFormat","test_keys"))

except:
    
    log_df_update(sqlContext,0,'Error in adding new column for Test and Version',get_pst_date(),' ','0',StartDate,' ',AlphaProcessDetailsLog_str)
    log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
    log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
    code_completion_email("failed due to error in adding new column for Test and Version","Alpha process update for "+locale_name,pos,locale_name)
    raise Exception("Error in adding new column for Test and Version!!!")

###Function to replace the default Test and Vers component of Recipient ID with the actual values which is picked from Test_Vers column

try:

    if(module_version_test_flag == '1'): 
        
        def ModifyRecipientID(RecipientID,Test_Vers):
            if(RecipientID is not None):
                Test=Test_Vers.split("#")[0]
                Vers=Test_Vers.split("#")[1]
                TestVers_str="TEST"+Test+".VERS"+Vers
                RecipientID1=RecipientID.replace("TEST1.VERSX",TestVers_str)
            else:
                RecipientID1=RecipientID

            return RecipientID1


        ModifyRecipientID_udf=udf(ModifyRecipientID,StringType())

        final_result_moduleCount=(final_result_moduleCount.withColumn("RecipientID1",ModifyRecipientID_udf("RecipientID","Test_Vers"))
                                                            .drop("RecipientID").drop("Test_Vers")
                                                            .withColumnRenamed("RecipientID1","RecipientID"))

       

        log_rowcount = final_result_moduleCount.count()
        print("Recipient ID modified")
        log_df_update(sqlContext,1,'Recipient ID modified',get_pst_date(),' ',str(log_rowcount),StartDate,' ',AlphaProcessDetailsLog_str)

    else:
        log_rowcount = final_result_moduleCount.count()
        print("RecipientID modification not needed as no tests present")
        log_df_update(sqlContext,1,'RecipientID modification not needed as no tests present',get_pst_date(),' ',str(log_rowcount),StartDate,' ',AlphaProcessDetailsLog_str)

except:
    
    log_df_update(sqlContext,0,'Error in RecipientID modification',get_pst_date(),' ','0',StartDate,' ',AlphaProcessDetailsLog_str)
    log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
    log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
    code_completion_email("failed due to error in modifying RecipientID","Alpha process update for "+locale_name,pos,locale_name)
    raise Exception("Error in RecipientID modification!!!")






def sampling():

    StartDate = get_pst_date()
    ###Sampling 5 rows for each omni extension
    if job_type == 'test':
        try:
            if test_type in ('AUTOTEST','MANUAL'):
                window = Window.partitionBy(final_result_moduleCount['OmniExtNewFormat']).orderBy(rand())
                final_result_moduleCount=final_result_moduleCount.withColumn("rank",rank().over(window)).filter(col('rank')<=5).drop('rank')
                final_result_moduleCount.cache()
                rec_cnt = final_result_moduleCount.count()
                log_df_update(sqlContext,1,'Omni-Extension sampling done',get_pst_date(),' ',rec_cnt,StartDate,' ',AlphaProcessDetailsLog_str)  
            
        except:
            log_df_update(sqlContext,1,'Error in Omni-Extension sampling',get_pst_date(),'Error',rec_cnt,StartDate,' ',AlphaProcessDetailsLog_str) 
            log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',StartDate,' ',AlphaProcessDetailsLog_str)
            log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
            code_completion_email("failed due to error in Omni-Extension Sampling","Alpha process update for "+locale_name,pos,locale_name)
            raise Exception("Error in Omni-Extension sampling!!!")
        
        try:
            if test_type in ('AUTOTEST','MANUAL'):
                final_result_moduleCount=final_result_moduleCount.withColumn('EmailAddress',lit(email_address))
                log_df_update(sqlContext,1,'Email addresses changed',get_pst_date(),' ',rec_cnt,StartDate,' ',AlphaProcessDetailsLog_str) 
            
        except:
            log_df_update(sqlContext,1,'Error in Email address change',get_pst_date(),'Error',rec_cnt,StartDate,' ',AlphaProcessDetailsLog_str)
            log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',StartDate,' ',AlphaProcessDetailsLog_str)
            log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
            code_completion_email("failed due to error in email address change","Alpha process update for "+locale_name,pos,locale_name)
            raise Exception("Error in Email address change!!!")   

            

def WriteToS3(final_result_moduleCount):

    StartDate = get_pst_date()
    ###Jenkins change starts here
    print("------------Final file will be split by campaign id and written directly to EC2B")
    

    final_result_moduleCount = (final_result_moduleCount
                        .withColumnRenamed('tpid','TPID')
                        .withColumnRenamed('eapid','EAPID')
                        .withColumnRenamed('first_name','FIRST_NAME')
                        .withColumnRenamed('last_name','LAST_NAME')
                        .withColumnRenamed('lang_id','LANG_ID')
                        .withColumnRenamed('mer_status','IsMER')
                        .withColumnRenamed('paid','PAID')
                        .withColumnRenamed('email_address','EmailAddress')
                        .withColumn('SubjectLine',final_result_moduleCount["S1_P1"])
                        .drop("S1_P1"))
                        

    try:
        if env_type == 'test' and test_type not in ('AUTOTEST','MANUAL'):
            blackholeEmail = 'username@blackhole.messagegears.com'
            final_result_moduleCount=final_result_moduleCount.withColumn('EmailAddress',lit(blackholeEmail))
            log_df_update(sqlContext,1,'Email addresses changed to blackhole address for Test Environment',get_pst_date(),' ',log_rowcount,StartDate,' ',AlphaProcessDetailsLog_str) 
    except:
        log_df_update(sqlContext,1,'Error in Email address change to blackhole address for Test Environment',get_pst_date(),'Error',log_rowcount,StartDate,' ',AlphaProcessDetailsLog_str)
        log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',StartDate,' ',AlphaProcessDetailsLog_str)
        log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
        code_completion_email("failed due to error in email address change to blackhole address for Test Environment","Alpha process update for "+locale_name,pos,locale_name)
        raise Exception("Error in Email address change!!!")  

    ### removing cols not required in output
    col_final_ls = [col for col in final_result_moduleCount.columns if col.find("authrealm")<0]
    
    ### removing columns which will be moved to modules table
    #col_modules = ["ModuleCount","LaunchDateTimeUTC","Locale","TPID","EAPID","LANG_ID","IsMER"]
    #col_final_ls=[c for c in col_final_ls if c not in col_modules]
    
    OmnitureMaster_path = path_dict['RecipientID_Output']+"OM"
    OM = sqlContext.read.parquet(OmnitureMaster_path)
    
    df_split = (module_count_map.select("campaign_id","AudienceTableName","ModuleCount").distinct()).join(broadcast(OM),"campaign_id","inner")
    df_split.show(100, False)
    
    cid = df_split.select("campaign_id").rdd.flatMap(lambda x: x).collect()
    atn = df_split.select("AudienceTableName").rdd.flatMap(lambda x: x).collect()
    sid = df_split.select("SID").rdd.flatMap(lambda x: x).collect()
    mc = df_split.select("ModuleCount").rdd.flatMap(lambda x: x).collect()
    print("------------Writing data into Bucket")
    
    ## writing output in S3 bucket
    path_ls = []
    cid_ls = []
    filename_ls = []
    sid_ls = []
    filename_mod_ls = []
    path_mod_ls = []
    cid_unsup = []
    sid_unsup = []
    
    for i in cid:
        
        if job_type == 'test':  
            if(test_type=='MANUAL'):
                file_name_s3 = atn[cid.index(i)]+"_MTS"
            elif(test_type=='AUTOTEST'):
                file_name_s3 = atn[cid.index(i)]+"_ATS"
            elif(test_type=='BACKUP'):
                file_name_s3 = atn[cid.index(i)]+"_BACKUP_"+file_dt
            elif(test_type not in ('AUTOTEST','MANUAL','BACKUP')):
                file_name_s3 = atn[cid.index(i)]+"_"+str(test_type)
        elif job_type == 'prod':
            file_name_s3 = atn[cid.index(i)]
            
        sid_cid = sid[cid.index(i)]
        mod_cnt = mc[cid.index(i)]
        file_name_s3_modules = file_name_s3 + str("_modules")
        
        print("campaign_id:", i)
        print("sid: ", sid_cid)
        print("#slots:", mod_cnt)
        print("file_name_s3: ",file_name_s3)
        print("file_name_s3_modules: ", file_name_s3_modules)
        
        path = "s3n://big-data-analytics-scratch-prod/project_traveler_profile/affine/alphaToSqlTest/{}/{}/{}".format(LaunchDate,file_name_s3,sid_cid)
        print("staging path:", path)
        
        path_mod = "s3n://big-data-analytics-scratch-prod/project_traveler_profile/affine/alphaToSqlTest/{}/{}/{}".format(LaunchDate,file_name_s3,"modules")
        print("modules path:", path_mod)
        
        df_write = final_result_moduleCount.select(col_final_ls).filter("campaign_id = {}".format(i))
        df_write = df_write.drop("campaign_id")
        df_write.cache()
        ### Creating a data frame containing details from the alpha output to be moved to modules table
        
        #df_write_modules = final_result_moduleCount.filter("campaign_id = {}".format(i)).select(col_modules).distinct()
        num_rows = df_write.count()
        print("#records:", num_rows)
        
        if num_rows == 0:
            print("Campaign {} was suppressed".format(i))
            
            log_df_update(sqlContext,0,'Campaign {} was suppressed'.format(i),get_pst_date(),' ','0',StartDate,' ',AlphaProcessDetailsLog_str)
            
            try:
                (df_write.coalesce(10).write.mode("overwrite").parquet(path))
            except:
                code_completion_email("failed due to error in writing parquet files","Alpha process update for "+locale_name,pos,locale_name)
                raise Exception("Writing parquet file to S3 error!!!")
                    
            log_df_update(sqlContext,1,'Writing parquet files for CID{} to s3 is Done'.format(i),get_pst_date(),' ',str(num_rows),StartDate,path,AlphaProcessDetailsLog_str)
            
            ls1 = path+"/*"
            path_ls.append(ls1)
            cid_ls.append(i)
            filename_ls.append(file_name_s3)
            sid_ls.append(sid_cid)
        else:
            try:
                (df_write.coalesce(10).write.mode("overwrite").parquet(path))
            except:
                code_completion_email("failed due to error in writing parquet files","Alpha process update for "+locale_name,pos,locale_name)
                raise Exception("Writing parquet file to S3 error!!!")
                
            log_df_update(sqlContext,1,'Writing parquet files for CID{} to s3 is Done'.format(i),get_pst_date(),' ',str(num_rows),StartDate,path,AlphaProcessDetailsLog_str)
            
            StartDate = get_pst_date()
            mod_ls = []
            
            for j in range(1,mod_cnt+1,1):
                dis_mod_ls = df_write.select("S{}_module_id".format(j)).distinct().rdd.flatMap(lambda x: x).collect()
                mod_ls.append(dis_mod_ls)
                
            flat_list = [item for sublist in mod_ls for item in sublist]
            dist_mod_list = list(set(flat_list))
            dis_mod = sqlContext.createDataFrame(pd.DataFrame(dist_mod_list)).withColumnRenamed("0","modules")
            
            ### Appending columns not requied in the final alpha output to the modules table
            #df_write_modules=df_write_modules.withColumn("join_id",lit(1))
            #dis_mod=dis_mod.withColumn("join_id",lit(1)).join(df_write_modules,"join_id",'left').drop("join_id")
            dis_mod.show()
            
            (dis_mod.coalesce(1).write.mode("overwrite").parquet(path_mod))
            
            log_df_update(sqlContext,1,'Writing distinct modules for CID{} to s3 is Done'.format(i),get_pst_date(),' ',str(dis_mod.count()),StartDate,path,AlphaProcessDetailsLog_str)
            
            ls1 = path+"/*"
            path_ls.append(ls1)
            ls2 = path_mod+"/*"
            path_mod_ls.append(ls2)
            cid_ls.append(i)
            filename_ls.append(file_name_s3)
            sid_ls.append(sid_cid)
            filename_mod_ls.append(file_name_s3_modules)
            cid_unsup.append(i)
            sid_unsup.append(sid_cid)
            
    StartDate = get_pst_date()
    
    
    print("------------Finished writing the data for the individual campaigns by Date/SID in S3")
    
    #print("Path: ", path_ls)
    #print("CID: ", cid_ls)
    #print("Filename: ", filename_ls)
    #print("SID: ", sid_ls)
    #print("Path modules: ", path_mod_ls)
    #print("Filename modules: ", filename_mod_ls)
    #print("CID_UnSuppressed: ", cid_unsup)
    #print("SID_UnSuppressed: ", sid_unsup)
    
    list1_concat = list(zip(cid_ls,filename_ls,path_ls,sid_ls))
    #print(list1_concat)
    
    df11 = sqlContext.createDataFrame(pd.DataFrame(list1_concat)).withColumnRenamed("0","campaign_id").withColumnRenamed("1","staging_filename").withColumnRenamed("2","path").withColumnRenamed("3","sid")
    
    list2_concat = list(zip(cid_unsup,filename_mod_ls,path_mod_ls,sid_unsup))
    #print(list2_concat)
    
    df22 = sqlContext.createDataFrame(pd.DataFrame(list2_concat)).withColumnRenamed("0","campaign_id").withColumnRenamed("1","staging_filename").withColumnRenamed("2","path").withColumnRenamed("3","sid")
    
    df12 = df11.unionAll(df22)
    #df12.show(100, False)
    
    launchServiceDf = importSQLTable(ocelotDb, "launchServiceLocalesEnabled")

    # no need to fail if no locales just go old way
    if (launchServiceDf.count() != 0):
        launchServiceLocales = launchServiceDf.select("locale").rdd.map(lambda x: str(x[0]).lower()).collect()
    else:
        log_df_update(sqlContext,1,'there are no launch service locales available',get_pst_date(),'Error',log_rowcount,StartDate,' ',AlphaProcessDetailsLog_str)
        log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',StartDate,' ',AlphaProcessDetailsLog_str)
        log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
        launchServiceLocales = []

    if any(locale_name in x for x in launchServiceLocales):
        print("locale is using new launch service change")
        cpn_schedule = (importSQLTable(ocelotDb, "tblCampaignLaunchDetails").filter("locale = '{}' and launchDate = '{}' ".format(locale_name,LaunchDate))
            .select("CampaignID","scheduledLaunchTime")
            .withColumnRenamed("CampaignID","campaign_id")
        )
    else:
        if env_type == "prod":
            jobMappingTable = "AlphaJobMappingProd"
        else:
            jobMappingTable = "AlphaJobMappingTest"

        cpn_schedule = (importSQLTable("OcelotStaging", jobMappingTable).filter("locale = '{}' ".format(locale_name))
            .select("AlphaCampaignID","ScheduledLaunchTime")
            .withColumnRenamed("AlphaCampaignID","campaign_id")
            .withColumnRenamed("ScheduledLaunchTime","scheduledLaunchTime")
        )


    ## if schedule doesn't exist fail. 
    if (cpn_schedule.count() != 0):
        rdd_path = df12.join(cpn_schedule,"campaign_id","inner").sort("scheduledLaunchTime","staging_filename")
    else:
        log_df_update(sqlContext,1,'could not find any schedule for campaigns',get_pst_date(),'Error',log_rowcount,StartDate,' ',AlphaProcessDetailsLog_str)
        log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',StartDate,' ',AlphaProcessDetailsLog_str)
        log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
        code_completion_email("failed due to error during wrtite to s3. Could not find any schedule for campaigns in locale: "+locale_name,pos,locale_name)
        raise Exception("Could not find any schedule for campaigns!!!")  
    
    #writing the path of the staging files for S3-to-SQL writer to read from
    if job_type == "prod":
        output_rdd_path = "s3n://big-data-analytics-scratch-prod/project_traveler_profile/affine/alphaToSql/Prod/rdd_path" \
                          "/{}/{}".format(locale_name,LaunchDate)
    else:
        output_rdd_path = "s3n://big-data-analytics-scratch-prod/project_traveler_profile/affine/alphaToSql/Test/{}/" \
                          "rdd_path/{}/{}".format(test_type,locale_name,LaunchDate)
    
    print("Path for s3-to-sql writer to read: ", output_rdd_path)
    print("------------Writing the paths where campaign and module information are stored in S3")
    
    try:
        rdd_path.coalesce(1).write.mode("overwrite").parquet(output_rdd_path)
    except:
        code_completion_email("failed due to error in writing campaign and module information stored in S3","Alpha process update for "+locale_name,pos,locale_name)
        raise Exception("Writing campaign and module information stored in S3 error!!!")
        
    print("------------writing campaign meta data")
    ### writing meta campaign data as source tables keep on changing in SQL sever , this data is required for campaign analysis
    
    metadata_path = "s3n://big-data-analytics-scratch-prod/project_traveler_profile/affine/config_file/{}/{}".format(locale_name,current_date)
    print(metadata_path)
    
    try:
        dfMetaCampaignData_VarDef.write.mode("overwrite").parquet(metadata_path)
    except:
        code_completion_email("failed due to error in writing meta campaign data as source tables","Alpha process update for "+locale_name,pos,locale_name)
        
    log_df_update(sqlContext,1,'Writing campaign meta data',get_pst_date(),' ','0',StartDate,metadata_path,AlphaProcessDetailsLog_str)
    log_df_update(sqlContext,1,success_str,get_pst_date(),' ','0',AlphaStartDate,output_rdd_path,AlphaProcessDetailsLog_str)
    log_df_update(sqlContext,1,success_str,get_pst_date(),' ','0',AlphaStartDate,output_rdd_path,CentralLog_str)
    
    if job_type == 'prod':
        print("------------Campaign Suppression Completed")
    elif job_type == 'test':
        if test_type == 'BACKUP':
            print("------------Campaign Suppression Completed")
        elif test_type != 'BACKUP':
            print("------------Test Sends Completed")
            
    code_completion_email("completed","Alpha process update for "+locale_name,pos,locale_name)
    
WriteToS3(final_result_moduleCount)
log_df_update(sqlContext,1,"Content Map module has ended for {} Locale for {} LaunchDate".format(locale_name,LaunchDate),get_pst_date(),' ','0',StartDate,' ',AlphaProcessDetailsLog_str)