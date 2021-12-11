package com.frauddetectionaccountsharing

import com.frauddetectionaccountsharing.model.ServerLog
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

class FraudDetection extends KeyedProcessFunction[String, String, String]{

  private var loginState: ValueState[java.lang.Boolean] = _
  private var prevLoginCountry: ValueState[java.lang.String] = _
  private var timerState: ValueState[java.lang.Long] = _

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {

    val loginDescriptor = new ValueStateDescriptor("login-flag", Types.BOOLEAN)
    loginState = getRuntimeContext.getState(loginDescriptor)

    val prevCountryDescriptor = new ValueStateDescriptor("prev-country", Types.STRING)
    prevLoginCountry = getRuntimeContext.getState(prevCountryDescriptor)

    val timerStateDescriptor = new ValueStateDescriptor("timer-state", Types.LONG)
    timerState = getRuntimeContext.getState(timerStateDescriptor)
  }

  @throws[Exception]
  override def processElement(value: String, ctx: KeyedProcessFunction[String, String, String]#Context, collector: Collector[String]): Unit = {
    val logEvent: ServerLog = ServerLog.fromString(value)

    print(logEvent)
    val isLoggedIn = loginState.value
    val prevCountry = prevLoginCountry.value

    if ((isLoggedIn != null) && (prevCountry != null)){
      if ((isLoggedIn == true) && (logEvent.eventType == "login")) {
        if (prevCountry != logEvent.locationCountry) {
          val alert: String = f"Alert eventID: ${logEvent.eventId}%s, " +
            f"violatingAccountId: ${logEvent.accountId}%d, prevCountry: ${prevCountry}%s, " +
            f"currentCountry: ${logEvent.locationCountry}%s"
          collector.collect(alert)
        }
      }
    }
    else if (logEvent.eventType == "login"){
      loginState.update(true)
      prevLoginCountry.update(logEvent.locationCountry)

      // 5 * 60 * 1000L -> 5 min
      val timer = logEvent.eventTimeStamp + (5 * 60 * 1000L)
      ctx.timerService.registerProcessingTimeTimer(timer)
      timerState.update(timer)
    }
    if (logEvent.eventType == "log-out") {
      loginState.clear()
      prevLoginCountry.clear()

      val timer = timerState.value()
      if (timer != null){
        ctx.timerService.deleteProcessingTimeTimer(timer)
      }
      timerState.clear()
    }

  }

  @throws[Exception]
  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, String, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    timerState.clear()
    loginState.clear()
    prevLoginCountry.clear()
  }
}
