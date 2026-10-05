package com.atguigu.online_edu.utils

import java.util.Properties

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object MyRedisUtil {

  private var jedisPool: JedisPool = null

  def getJedisClient() ={

    if (jedisPool == null){
      val pro: Properties = MyPropertiesUtil.load("config.properties")
      val host: String = pro.getProperty("redis.host")
      val port: String = pro.getProperty("redis.port")

      val jedisPoolConfig = new JedisPoolConfig
      jedisPoolConfig.setMaxTotal(100)  //最大连接数
      jedisPoolConfig.setMaxIdle(20)   //最大空闲
      jedisPoolConfig.setMinIdle(20)     //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true)  //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(500)//忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

      val pool = new JedisPool(jedisPoolConfig,host,port.toInt)

     jedisPool = pool

    }

    jedisPool.getResource

  }

}
