package com.lqf.sparkmall2018.common.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtil {

  var jedisPool:JedisPool=null

  def getJedisClient:Jedis={

    if (jedisPool==null){
      println("开辟一个连接池")
      val host=ConfigUtil.getValueByKey("redis.host")
      val port=ConfigUtil.getValueByKey("redis.port").toInt
      val jedisPoolConfig=new JedisPoolConfig
      jedisPoolConfig.setMaxTotal(100)//最大气连接数
      jedisPoolConfig.setMaxIdle(20)
      jedisPoolConfig.setMinIdle(20)
      jedisPoolConfig.setBlockWhenExhausted(true)
      jedisPoolConfig.setMaxWaitMillis(500)
      jedisPoolConfig.setTestOnBorrow(true)

      jedisPool = new JedisPool(jedisPoolConfig,host,port)

    }

    jedisPool.getResource


  }

}
