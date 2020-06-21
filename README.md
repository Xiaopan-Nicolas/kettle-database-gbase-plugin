GBase kettle数据源插件扩展

- 打包
```mvn package assembly:single ```
- pom.xml文件中的gbase驱动，使用中央仓库或者maven本地安装驱动，驱动文件在jdbc目录下
- 因为没有外部依赖的jar，所以直接使用打好的jar文件即可
- 将jdbc目录下的驱动文件放入kettle的lib目录下
- 本扩展jar在kettle的plugins下创建gbase目录，并放进去
- 修改lib下kettle-core-xxx.jar中的database-type.xml文件，添加GBase数据库类型