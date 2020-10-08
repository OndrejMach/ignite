package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcseu.Application.natco
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._

trait FactsProcessing extends Logger{
  def getProvisionedDaily(provisionData: DataFrame,period_for_process:String): DataFrame /*aj tu som pridala period for process*/
}

//functions with "Daily" suffix, but in fact these are aggregating data based on the previous filtering
//from ProcessingCore

class Facts extends FactsProcessing {
//aggregating unique number of provisioned users in the specific period_for_process and natco

  def getProvisionedDaily(provision: DataFrame,period_for_process:String): DataFrame = {
    val provisionedDaily = provision
      .withColumn("ConKeyP1",lit(period_for_process))
      .withColumn("NatCo", lit(natco))
      .withColumn("ConKeyP1", concat_ws("|",col("ConKeyP1"),col("NatCo")))
      .dropDuplicates("msisdn")
      .groupBy("ConKeyP1").count().withColumnRenamed("count","Provisioned_daily")
    provisionedDaily
  }


  def getRegisteredDaily(register_requests: DataFrame,fullUserAgents: DataFrame,period_for_process:String): DataFrame = {
    //aggregating unique number of registered users in the specific period_for_process and natco
    // and for the specific user agent
    //then joining with user agent id from Dimension processing

    val dfRMT1=register_requests
      .withColumn("ConKeyR1",lit(period_for_process))
      .withColumn("NatCo", lit(natco))
      .withColumn("ConKeyR1", concat_ws("|",col("ConKeyR1"),col("NatCo")))
      .groupBy("msisdn")
      .agg(max("user_agent").alias("UserAgent"),max("ConKeyR1").alias("ConKeyR1"))
      //.withColumn("ConKeyR1",regexp_replace(col("ConKeyR1"), "_", "|"))
      //.withColumn("ConKeyR1",concat("ConKeyR1", lit("|"), "user_agent").alias("ConKeyR1"))
      .join(fullUserAgents, "UserAgent")
      .withColumn("ConKeyR1", concat_ws("|",col("ConKeyR1"),col("_UserAgentID")))
      .select("_UserAgentID","ConKeyR1","msisdn")


    val dfRMT2=dfRMT1
      .groupBy("_UserAgentID").count().alias("_UserAgentID")

    val joinedDS = dfRMT1.join(dfRMT2, "_UserAgentID")
      .groupBy("_UserAgentID")
      .agg(max("ConKeyR1").alias("ConKeyR1"),max("count").alias("Registered_Daily"))
      .select("ConKeyR1","Registered_Daily")
    joinedDS
  }

  //TODO: add NatcoNetwork
  def getActiveDaily(activity: DataFrame,fullUserAgents: DataFrame,period_for_process:String,natcoNetwork:String): DataFrame = {
    //aggregated according to the QlikSense script from Jarda

    ////// 1. ORIGINATED and TERMINATED
    // a.) SUCCESSFULY

    // successfully originated for CHAT
    val df1 = activity
     // .na.fill("NULL",Seq("user_agent"))

    val df2 =df1
      .filter(df1("sip_code") <=> 200  and col("from_user").startsWith("+")and (df1("from_network") <=> natcoNetwork or df1("from_network") === "dt.jibecloud.net"))
      .select("from_user","user_agent","creation_date")
      .withColumnRenamed("from_user","uau")

    //successfully originated for FILES
    val df3 =df1
      .filter(df1("type") <=> "FT_POST"  and col("from_user").startsWith("+")and (df1("from_network") <=> natcoNetwork or df1("from_network") === "dt.jibecloud.net"))
      .select("from_user","user_agent","creation_date")
      .withColumnRenamed("from_user","uau")

    //successfully terminated (received by another user) for CHAT
    val df4 =df1
      .filter(df1("sip_code") <=> 200  and col("to_user").startsWith("+")and (df1("to_network") <=> natcoNetwork or df1("to_network") === "dt.jibecloud.net"))
      .withColumn("user_agent",  lit(null))
      .withColumnRenamed("to_user","uau")
      .select("uau","user_agent","creation_date")

    /////////////////////////////////////////////////
    val ft_get = df1
      .filter(df1("type") <=> "FT_GET"  and col("from_user").startsWith("+"))
      .select("call_id","user_agent","creation_date")
      .withColumnRenamed("call_id", "call_id1")

    val ft_post = df1
      .filter(df1("type") <=> "FT_POST"  and col("from_user").startsWith("+")and (df1("to_network") <=> natcoNetwork or df1("to_network") === "dt.jibecloud.net"))
      .select("call_id","from_user","to_network")

    //successfully terminated (received by another user) for FILES
    val df5 = ft_get
      .join(ft_post, ft_get("call_id1") <=> ft_post("call_id"))
      .withColumnRenamed("from_user", "uau")
      .select("uau","user_agent","creation_date")

    //create one table from the four preceding
    val result = df2
      .union(df3)
      .union(df4)
      .union(df5)

    //filter user_agents starting with IM_client
    val result1=result
      .filter(col("user_agent").startsWith("IM-client"))

    //filter null user_agents
    val result2=result
      //.filter(result("user_agent") <=> "NULL")
      .filter("user_agent is null")

    //create one table from IM-client and null user_agents
    val result3 = result1
      .union(result2)
      .groupBy("uau")
      .agg(max("creation_date").alias("creation_date"),max("user_agent").alias("user_agent"))
      .withColumnRenamed("uau","uau_temp")

    //final count of successful users
    val result4=result3
      .groupBy("user_agent").count()

    ////////////////////////////////////////////////////////////

    //b.) UNSUCCESSFULY
    //UNsuccessfully originated for CHAT (UNsuccessfully originated FILES data are not present in the input data)
    val resultU =df1
      .select("from_user","user_agent","creation_date")
      //.filter($"from_user".startsWith("+") && $"from_network".startsWith("dt-slovak-telecom") && not($"sip_code".contains("200") || $"type".contains("FT_POST") || $"type".contains("FT_GET")))yyy
      .filter(col("from_user").startsWith("+") && (col("from_network").contains(natcoNetwork) or col("from_network").contains("dt.jibecloud.net") ) && not(col("type").contains("FT_POST")) && not(col("type").contains("FT_GET")) && ((col("sip_code") =!= "200") || (col("sip_code").isNull)))
      .withColumnRenamed("from_user","uau_UNS")
      .withColumnRenamed("user_agent","user_agent_UNS")
      .withColumnRenamed("creation_date","creation_date_UNS")

    //filter user_agents starting with IM_client
    val resultU1=resultU
      .filter(col("user_agent_UNS").startsWith("IM-client"))

    //filter null user_agents
    val resultU2=resultU
      .filter("user_agent_UNS is null")

    //create one table from IM-client and null user_agents
    val resultU3 = resultU1
      .union(resultU2)
      .groupBy("uau_UNS")
      .agg(max("creation_date_UNS").alias("creation_date_UNS"),max("user_agent_UNS").alias("user_agent_UNS"))

    //filtering out UNsuccessful users that are also present in the successful table
    //they are counted as successful
    val dfx1=result3
      .join(resultU3,result3("uau_temp") <=>  resultU3("uau_UNS"),"inner")
      .select("uau_temp","user_agent","creation_date")


    val dfx2=resultU3.join(dfx1,resultU3("uau_UNS") <=>  dfx1("uau_temp"),"left_anti")
    // .withColumnRenamed("uau_temp","uau")

    //final count of UNsuccessful users
    val resultU4=dfx2
      .groupBy("user_agent_UNS").count()
      .withColumnRenamed("count","count_UNS")

    //final join
    val finaldf=result4
      .join(resultU4,result4("user_agent") <=>  resultU4("user_agent_UNS"),"outer")
      .select("user_agent","count","count_UNS")


    ///////////////////////////////////////////////////////
    /////2. ORIGINATED
    //a.)SUCCESSFULY

    //successfuly originated for  CHAT
    val df2O =df1
      .filter(df1("sip_code") <=> 200  and col("from_user").startsWith("+")and (df1("from_network") <=> natcoNetwork) or df1("from_network") === "dt.jibecloud.net")
      .select("from_user","user_agent","creation_date")
      .withColumnRenamed("from_user","uau")

    //successfuly originated for FILES
    val df3O =df1
      .filter(df1("type") <=> "FT_POST"  and col("from_user").startsWith("+")and (df1("from_network") <=> natcoNetwork) or df1("from_network") === "dt.jibecloud.net")
      .select("from_user","user_agent","creation_date")
      .withColumnRenamed("from_user","uau")

    //joining tables

    val resultO = df2O
      .union(df3O)

    //filter user_agents starting with IM_client
    val result1O=resultO
      .filter(col("user_agent").startsWith("IM-client"))

    //filtering null user agents
    val result2O=resultO
      .filter("user_agent is null")

    //create one table from IM-client and null user_agents
    val result3O = result1O
      .union(result2O)
      .groupBy("uau")
      .agg(max("creation_date").alias("creation_date"),max("user_agent").alias("user_agent"))

    //final count of successful users
    val result4O=result3O
      .groupBy("user_agent").count()


    ////////////////////////////////////////////////////////////úúú
    //b.) UNSUCCESSFULY
    val resultUO =df1
      //.filter($"from_user".startsWith("+") && $"from_network".startsWith("dt-slovak-telecom") && not($"sip_code".contains("200") || $"type".contains("FT_POST") || $"type".contains("FT_GET")))yyy
      .filter(col("from_user").startsWith("+") && (col("from_network").contains(natcoNetwork) or col("from_network").contains("dt.jibecloud.net")) && not(col("type").contains("FT_POST")) && not(col("type").contains("FT_GET")) && ((col("sip_code") =!= "200") || (col("sip_code").isNull)))
      .select("from_user","user_agent","creation_date")
      .withColumnRenamed("from_user","uau_UNS")
      .withColumnRenamed("user_agent","user_agent_UNS")
      .withColumnRenamed("creation_date","creation_date_UNS")

    //filter user_agents starting with IM_client
    val resultU1O=resultUO
      .filter(col("user_agent_UNS").startsWith("IM-client"))

    //filtering null user agents
    val resultU2O=resultUO
      .filter("user_agent_UNS is null")

    //create one table from IM-client and null user_agents
    val resultU3O = resultU1O
      .union(resultU2O)
      .groupBy("uau_UNS")
      .agg(max("creation_date_UNS").alias("creation_date_UNS"),max("user_agent_UNS").alias("user_agent_UNS"))

    val dfxx1=result3O
      .join(resultU3O,result3O("uau") <=>  resultU3O("uau_UNS"),"inner")
      .select("uau","user_agent","creation_date")

    val dfxx2=resultU3O.join(dfxx1,resultU3O("uau_UNS") <=>  dfxx1("uau"),"left_anti")

    //final count of UNsuccessful users
    val resultU4O=dfxx2
      .groupBy("user_agent_UNS").count()
      .withColumnRenamed("count","count_UNS")

//then joining all counted values and renaming them to the final table
    //also joining with the full user agents dimension to get the user agent id
    val finaldfO=result4O
      .join(resultU4O,result4O("user_agent") <=>  resultU4O("user_agent_UNS"),"outer")
      .select("user_agent","count","count_UNS")
      .withColumnRenamed("count","Active_daily_succ_orig")
      .withColumnRenamed("count_UNS","Active_daily_unsucc_orig")
      .withColumnRenamed("user_agent","user_agent_UNS")

    val finalTable=finaldf
      .join(finaldfO,finaldf("user_agent") <=>  finaldfO("user_agent_UNS"),"left_outer")
      .withColumnRenamed("count","Active_daily_succ_origterm")
      .withColumnRenamed("count_UNS","Active_daily_unsucc_origterm")
      .drop("user_agent_UNS")

    val keyTable= activity
      .withColumn("ConKeyA1",lit(period_for_process))
      .withColumn("NatCo", lit(natco))
      .withColumn("ConKeyA1", concat_ws("|",col("ConKeyA1"),col("NatCo")))
      .select("ConKeyA1","user_agent").distinct()

    val finalFinal=finalTable
      .join(fullUserAgents,finalTable("user_agent") <=>  fullUserAgents("UserAgent"),"left_outer").distinct()
      .join(keyTable,finalTable("user_agent") <=>  keyTable("user_agent"),"left_outer")
      .withColumn("ConKeyA1", concat_ws("|",col("ConKeyA1"),col("_UserAgentID")))
      .drop("_UserAgentID","OEM","Device","Client","FW","Client_vs","user_agent")

    finalFinal

  }

  def getServiceFactsDaily(activity: DataFrame): DataFrame = {
    //
    //aggregated according to the QlikSense script from Jarda
    //count = number of specific service used OnNet or Offnet within the specific date and natco
    /*
    Image share-OnNet: does not exist in v5 files
    Image share-OffNet: does not exist in v5 files
    Files sent-OnNet: count(*) WHERE type=FT_POST AND from_network=to_network
    Files sent-OffNet: count(*) WHERE type=FT_POST AND from_network!=to_network
    Files received-OnNet: in source files to_network is always null where type = FT_GET,
                          Therefore we have to link to_network from records where type = FT_POST according to call_id (same as user_agent for unique active users)
    Files received-OffNet: same as "Files received-OnNet"
    GroupChat sent-OnNet: (sum(messages_sent) WHERE type='GROUP_CHAT' AND LEFT(from_user, 1)='+') AND sip_code='200' + (sum(messages_received) WHERE type='GROUP_CHAT' AND LEFT(to_user, 1)='+' AND sip_code='200')
    GroupChat received-OnNet: (sum(messages_received) WHERE type='GROUP_CHAT' AND LEFT(from_user, 1)='+' AND sip_code='200') + (sum(messages_sent) WHERE type='GROUP_CHAT' AND LEFT(to_user, 1)='+' AND sip_code='200')
    GroupChat sent-OffNet: not able to distinguish in v5 files
    GroupChat received-OffNet: not able to distinguish in v5 files
    Chat sent-OnNet: sum(messages_sent) WHERE type=CHAT and sip_code=200 AND from_network=to_network
    Chat sent-OffNet: sum(messages_sent) WHERE type=CHAT and sip_code=200 AND from_network!=to_network
    Chat received-OnNet: sum(messages_received) WHERE type=CHAT AND sip_code=200 AND from_network=to_network
    Chat received-OffNet: sum(messages_received) WHERE type=CHAT AND sip_code=200 AND from_network!=to_network

     */
    val sf1=activity
    //Files SENT-OnNet:
    val sf2 =sf1
      .withColumn("_NetworkingID",lit("1"))
      .withColumn("_ServiceID",lit("5"))
      .filter(sf1("type") === "FT_POST" and sf1("from_network") === sf1("to_network"))
      //.select("from_user","user_agent","creation_date")
      .groupBy("_NetworkingID","_ServiceID")
      .count()

    //.agg(max("_NetworkingID").alias("_NetworkingID"),max("user_agent_UNS").alias("user_agent_UNS"))

    //Files SENT-OffNet:
    val sf3 =sf1
      .withColumn("_NetworkingID",lit("2"))
      .withColumn("_ServiceID",lit("5"))
      .filter((sf1("type") === "FT_POST") and  not(sf1("from_network") === sf1("to_network")))
      //.select("from_user","user_agent","creation_date")
      .groupBy("_NetworkingID","_ServiceID")
      .count
    //Files RECEIVED-OnNet:
    val sf_get = sf1
      .withColumnRenamed("call_id", "call_id1")
      .filter(sf1("type") === "FT_GET")
      .select("call_id1","user_agent","creation_date")


    val sf_post = sf1
      .filter(sf1("type") === "FT_POST"  and sf1("from_network") === sf1("to_network"))
      .select("call_id","to_network")


    val sf4 = sf_get
      .join(sf_post, sf_get("call_id1") === sf_post("call_id"))
      .withColumnRenamed("from_user", "uau")
      .withColumn("_NetworkingID",lit("1"))
      .withColumn("_ServiceID",lit("6"))
      //.select("uau","user_agent","creation_date")
      .groupBy("_NetworkingID","_ServiceID")
      .count

    //Files RECEIVED-OffNet:
    val sf_get1 = sf1
      .withColumnRenamed("call_id", "call_id1")
      .filter(sf1("type") === "FT_GET"  and col("from_user").startsWith("+"))
      .select("call_id1","user_agent","creation_date")


    val sf_post1 = sf1
      .filter(sf1("type") === "FT_POST"  and col("from_user").startsWith("+") and sf1("from_network") =!= sf1("to_network"))
      .select("call_id","to_network")

    val sf5 = sf_get1
      .join(sf_post1, sf_get1("call_id1") === sf_post1("call_id"))
      .withColumnRenamed("from_user", "uau")
      .withColumn("_NetworkingID",lit("2"))
      .withColumn("_ServiceID",lit("6"))
      //.select("uau","user_agent","creation_date")
      .groupBy("_NetworkingID","_ServiceID")
      .count
    //GroupChat SENT-OnNet:
    val sf6 =sf1
      .filter(sf1("type") === "GROUP_CHAT" and col("from_user").startsWith("+") and sf1("sip_code") === 200)
      //.filter(sf1("type") === "GROUP_CHAT" and col("to_user").startsWith("+") and sf1("sip_code") === 200)
      .select("from_user","user_agent","creation_date","messages_sent")
      .agg(sum("messages_sent").alias("count"))
      .withColumn("_NetworkingID",lit("1"))
      .withColumn("_ServiceID",lit("3"))
      .select("_NetworkingID","_ServiceID","count")

    //GroupChat RECEIVED-OnNet:
    val sf7 =sf1
      .filter(sf1("type") === "GROUP_CHAT" and col("from_user").startsWith("+") and sf1("sip_code") === 200)
      //.filter(sf1("type") === "GROUP_CHAT" and col("to_user").startsWith("+") and sf1("sip_code") === 200)
      .select("from_user","user_agent","creation_date","messages_received")
      .agg(sum("messages_received").alias("count"))
      .withColumn("_NetworkingID",lit("1"))
      .withColumn("_ServiceID",lit("4"))
      .select("_NetworkingID","_ServiceID","count")

    //Group chat OffNet nerozlišuje

    //Chat SENT-OnNet:
    val sf8 =sf1
      .filter(sf1("type") === "CHAT" and sf1("sip_code") === 200 and sf1("from_network") === sf1("to_network"))
      .select("from_user","user_agent","creation_date","messages_sent")
      .agg(sum("messages_sent").alias("count"))
      .withColumn("_NetworkingID",lit("1"))
      .withColumn("_ServiceID",lit("1"))
      .select("_NetworkingID","_ServiceID","count")

    //Chat SENT-OffNet:
    val sf9 =sf1
      .filter(sf1("type") === "CHAT" and sf1("sip_code") === 200 and sf1("from_network") =!= sf1("to_network"))
      .select("from_user","user_agent","creation_date","messages_sent")
      .agg(sum("messages_sent").alias("count"))
      .withColumn("_NetworkingID",lit("2"))
      .withColumn("_ServiceID",lit("1"))
      .select("_NetworkingID","_ServiceID","count")


    //Chat RECEIVED-OnNet:
    val sf10 =sf1
      .filter(sf1("type") === "CHAT" and sf1("sip_code") === 200 and sf1("from_network") === sf1("to_network"))
      .select("from_user","user_agent","creation_date","messages_received")
      .agg(sum("messages_received").alias("count"))
      .withColumn("_NetworkingID",lit("1"))
      .withColumn("_ServiceID",lit("2"))
      .select("_NetworkingID","_ServiceID","count")

    //Chat RECEIVED-OffNet:
    val sf11 =sf1
      .filter(sf1("type") === "CHAT" and sf1("sip_code") === 200 and sf1("from_network") =!= sf1("to_network"))
      .select("from_user","user_agent","creation_date","messages_received")
      .agg(sum("messages_received").alias("count"))
      .withColumn("_NetworkingID",lit("2"))
      .withColumn("_ServiceID",lit("2"))
      .select("_NetworkingID","_ServiceID","count")


    val finalsf = sf2.union(sf3)
      .union(sf4)
      .union(sf5)
      .union(sf6)
      .union(sf7)
      .union(sf8)
      .union(sf9)
      .union(sf10)
      .union(sf11)

    finalsf


  }


}