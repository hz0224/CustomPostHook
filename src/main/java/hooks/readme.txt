打包后使用：CustomPostHook-1.0-SNAPSHOT.jar 这个包即可（hive-exec不需要打到包里，因为集群上有这个包）
放到hive/lib目录下：然后设置
    set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.LineageLogger,hooks.CustomLineageToMysql 即可使用.
    这里一定要设置两个钩子，一个是自带的，一个是自定义的。至于为什么：查看 https://cloud.tencent.com/developer/ask/235118
