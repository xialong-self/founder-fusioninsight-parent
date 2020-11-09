package com.huawei.hadoop.security;

import org.apache.spark.serializer.KryoRegistrator;

/**
 * Define serializer class.
 */
public class HuaweiRegistrator implements KryoRegistrator {
  public void registerClasses(com.esotericsoftware.kryo.Kryo kryo) {
    kryo.register(org.apache.hadoop.hbase.io.ImmutableBytesWritable.class);
    kryo.register(org.apache.hadoop.hbase.client.Result.class);
    kryo.register(scala.Tuple2[].class);
    kryo.register(org.apache.hadoop.hbase.Cell[].class);
    kryo.register(org.apache.hadoop.hbase.NoTagsKeyValue.class);
    kryo.register(org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionLoadStats.class);
  }
}
