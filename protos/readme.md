# 操作流程
1. cd sparklab\protos
2. protoc ./test.proto --java_out=../src/main/java

# 注意事项
1. 注意protoc版本需要与spark使用的protocolbuffer的版本一致
2. 需要指定kyro序列化，sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")