// Databricks notebook source
// MAGIC %md
// MAGIC # ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png)  3/ GOLD table: extract the sessions with Scala
// MAGIC 
// MAGIC ### Scala version
// MAGIC 
// MAGIC This notebook implement the same logic as the python [../03-Delta-session-GOLD]($../03-Delta-session-GOLD)  but using Scala.
// MAGIC 
// MAGIC As you'll see, the function signature is slightly different as we do not receive an iterator of Pandas Dataframe, but the logic remains identical.
// MAGIC 
// MAGIC <!-- tracking, please Collect usage data (view). Remove it to disable collection. View README for more details.  -->
// MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fstreaming%2Fsessionization%2Fscala_gold&dt=FEATURE_STREAMING_SESSIONIZATION">

// COMMAND ----------

// MAGIC %run ../_resources/00-setup $reset_all_data=false

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### To make things simple, we'll be using Scala case class to represent our Events and our Sesssions

// COMMAND ----------

import spark.implicits._
import java.sql.Timestamp
import org.apache.spark.sql.catalyst.plans.logical.EventTimeTimeout
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.GroupState
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.Trigger

// COMMAND ----------

//Event (from the silver table)
case class ClickEvent(user_id: String, event_id: String, event_datetime: Timestamp, event_date: Long, platform: String, action: String, uri: String)
 
//Session (from the gold table)
case class UserSession(user_id: String, var status: String = "online", var start: Timestamp, var end: Timestamp, var clickCount: Integer = 0)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Implementing the aggregation function to update our Session
// MAGIC 
// MAGIC In this simple example, we'll just be counting the number of click in the session.
// MAGIC 
// MAGIC The function `updateState` will be called for each user with a list of events for this user.

// COMMAND ----------

def updateState(user_id: String, events: Iterator[ClickEvent], state: GroupState[UserSession]): Iterator[UserSession] = {
  val prevUserSession = state.getOption.getOrElse {
    UserSession(user_id, "online", new Timestamp(6284160000000L), new Timestamp(6284160L))
  }

  //Just an update, not a timeout
  if (!state.hasTimedOut) {
    events.foreach { event =>
      updateUserSessionWithEvent(prevUserSession, event)
    }
    //Renew the session timeout by 45 seconds
    state.setTimeoutTimestamp(prevUserSession.end.getTime + 45 * 1000)
    state.update(prevUserSession)
  } else {
    //Timeout, we flag the session as offline
    if (prevUserSession.status == "online") {
      //Update the stet as offline
      prevUserSession.status = "offline"
      state.update(prevUserSession)
    } else {
      //Session has been flagged as offline during the last run, we can safely discard it
      state.remove()
    }
  }
   return Iterator(prevUserSession)
}

def updateUserSessionWithEvent(state: UserSession, input: ClickEvent): UserSession = {
  state.status = "online"
  state.clickCount += 1
  //Update then begining and end of our session
  if (input.event_datetime.after(state.end)) {
    state.end = input.event_datetime
  }
  if (input.event_datetime.before(state.start)) {
    state.start = input.event_datetime
  }
  //return the updated state
  state
}

// COMMAND ----------

val stream = spark
.readStream
    .format("delta")
    .table("events")  
  .as[ClickEvent]
  .groupByKey(_.user_id)
  .flatMapGroupsWithState(OutputMode.Append(), EventTimeTimeout)(updateState)

display(stream)

// COMMAND ----------

// MAGIC %md
// MAGIC ### We now have our sessions stream running!
// MAGIC 
// MAGIC We can set the output of this streaming job to a SQL database or another queuing system.
// MAGIC 
// MAGIC We'll be able to automatically detect cart abandonments in our website and send an email to our customers, our maybe just give them a call asking if they need some help! 
// MAGIC 
// MAGIC But what if we have to delete the data for some of our customers ? **[DELETE and UPDATE with delta](https://demo.cloud.databricks.com/#notebook/4440322)**
// MAGIC 
// MAGIC 
// MAGIC **[Go Back](https://demo.cloud.databricks.com/#notebook/4439040)**
