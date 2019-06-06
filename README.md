# bigdata
本项目用来进行大数据方面的尝试及大数据组件的尝试，方便开展后续的工作及记录生活的点滴

# 这里提供了两种读取及写入hbase的方法：
1. 使用hadoop的api  -----  hbase_util_rdd 
   这种方法下，需要引入spark-example的jar包,需要指定spark-examples-1.6.0-cdh5.12.0-hadoop2.6.0-cdh5.12.0.jar的位置
2. 使用hortonworks的spark hbase connector （SHC） 
   shc-core-1.1.2-2.3-s_2.11-SNAPSHOT.jar
   可以使用 --packages 参数指定shc的jar包，但是这种经常不凑效
   pyspark --packages com.hortonworks:shc:1.0.0-1.6-s_2.10 --repositories http://repo.hortonworks.com/content/groups/public/  --files /etc/spark/conf.cloudera.spark_on_yarn/yarn-conf/hbase-site.xml 

   上面方法不适用时，可以自己编译shc包，这种绝对适用
   git clone  https://github.com/apache/hbase-connectors.git
   git checkout origin/branch-2.3
   修改对应组件的版本信息

[shc]$ git diff pom.xml
diff --git a/pom.xml b/pom.xml
index 4a4390e..da525c5 100644
--- a/pom.xml
+++ b/pom.xml
@@ -43,10 +43,10 @@

   <properties>
     <spark.version>2.3.0</spark.version>
-    <hbase.version>1.1.2</hbase.version>
-    <phoenix.version>4.9.0-HBase-1.1</phoenix.version>
+    <hbase.version>1.2.0-cdh5.12.0</hbase.version>
+    <phoenix.version>4.14.0-cdh5.12.2</phoenix.version>
     <test_classpath_file>${project.build.directory}/spark-test-classpath.txt</test_classpath_file>
-    <java.version>1.7</java.version>
+    <java.version>1.8</java.version>
     <scala.version>2.11.8</scala.version>
     <scala.macros.version>2.1.0</scala.macros.version>
     <scala.binary.version>2.11</scala.binary.version>
@@ -88,6 +88,17 @@
         <enabled>false</enabled>
       </snapshots>
     </repository>
+    <repository>
+      <id>cloudera-repo</id>
+     <name>cloudera Repository</name>
+    <url>https://repository.cloudera.com/artifactory/cloudera-repos</url>
+     <releases>
+     <enabled>true</enabled>
+    </releases>
+     <snapshots>
+    <enabled>false</enabled>
+    </snapshots>
+   </repository>
   </repositories>

   <dependencies>



# 后续可能按需添加scala版本的，具体可以参考 https://hbase.apache.org/book.html#_sparksql_dataframes 

