package com.tmobile.sit.ignite.rcseu.pipeline

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import com.tmobile.sit.ignite.rcseu.pipeline.FactsProcessing

case class ActivityEntry(
                          creation_date: String,
                          from_user: String,
                          to_user: String,
                          from_network: String,
                          to_network: String,
                          `type`: String,
                          duration: Int,
                          bytes_sent: Int,
                          bytes_received: Int,
                          call_id: String,
                          contribution_id: String,
                          src_ip: String,
                          sip_code: Int,
                          sip_reason: String,
                          user_agent: String,
                          messages_sent: Int,
                          messages_received: Int,
                          from_tenant: String,
                          to_tenant: String,
                          FileDate: String
                        )
object ActivityEntry {
  val sampleActivityEntry: ActivityEntry = ActivityEntry(
    creation_date = "2022-04-18T10:00:00.000Z",
    from_user ="from_usr1",
    to_user = "to_usr1",
    from_network = "telecom1",
    to_network = "telecom2",
    `type` = "CHAT",
    duration = 100000,
    bytes_sent = 20,
    bytes_received = 0,
    call_id = "call_id1",
    contribution_id = "contribution_id1",
    src_ip = null,
    sip_code = 1,
    sip_reason = "reason_OK",
    user_agent = "phone_type_1",
    messages_sent = 1,
    messages_received = 0,
    from_tenant = "telecom1",
    to_tenant = "telecom2",
    FileDate = "2022-04-18"
  )
}

case class UserAgent(
                      UserAgent: String,
                      OEM: String,
                      Device: String,
                      Client: String,
                      FW: String,
                      Client_vs: String,
                      _UserAgentID: Int
                    )
object UserAgent{
  val sampleUserAgent: Int => UserAgent = {
    i =>
      UserAgent(
        UserAgent = s"phone_type_$i",
        OEM = "PHN",
        Device = s"PHN$i",
        Client = s"CLI$i",
        FW = s"FW$i",
        Client_vs = s"Phone type $i",
        _UserAgentID = i
      )
  }
}

@RunWith(classOf[JUnitRunner])
class FactsProcessingTest extends FlatSpec with BeforeAndAfterAll { // with DataFrameSuiteBase {
//  implicit lazy val spark_session: SparkSession = spark
//  import spark_session.implicits._
  val sparkConfig = new SparkConf()
  sparkConfig.set("spark.master", "local")
  @transient lazy val ss: SparkSession = SparkSession.builder().config(sparkConfig).getOrCreate()

//  @transient var sc: SparkContext = null

  import ActivityEntry._
  import UserAgent._

//  @Before
//  override def beforeAll(): Unit = {
//    ss = SparkSession.builder().config(sparkConfig).getOrCreate()
//  }


  "facts" should "be processed" in {
    import ss.implicits._

    val sampleActivities = List(
      sampleActivityEntry,
      sampleActivityEntry.copy(creation_date = "2022-04-18T11:00:00.000Z", from_user = "from_usr2", to_user = "to_usr2",
        call_id = "call_id2", contribution_id = "contribution_id2", user_agent = "phone_type_2")
    ).toDF()

    sampleActivities.show(5, false)
    val accCount = sampleActivities.count()
    println(s"activity - $accCount")
    val sampleUserAgents = List(
      sampleUserAgent(1),
      sampleUserAgent(2)
    ).toDF()


//    val res = FactsProcessing.getActiveDaily(sampleActivities, sampleUserAgents, "18-04-2022", "telecom1", "1")
//
//    println("\n============================================================================================\nRESULT:")
//    res.printSchema()
//    res.show(5, false)
    assert(false)
  }
}
