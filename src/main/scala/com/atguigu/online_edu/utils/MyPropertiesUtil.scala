package com.atguigu.online_edu.utils

import java.io.InputStreamReader
import java.util.Properties


object MyPropertiesUtil {

  def main(args: Array[String]): Unit = {

    val properties: Properties = MyPropertiesUtil.load("config.properties")
    println(properties.getProperty("kafka.broker.list"))
  }


  def load(fileName: String) = {

    val properties = new Properties
    properties.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(fileName), "UTF-8"))
    properties
  }

}
