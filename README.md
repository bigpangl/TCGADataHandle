# 项目缘由

帮一个医学朋友做的TCGA 数据预处理程序

# 使用方式

在combine.ini 文件中配置gdc 下载目录以及meta.json 文件,然后配置需要提取的列名

注意，提取的列名分为两类,第一类是meta.json 文件中的，另一类是数据文件中的列。两者分开配置

# 日志输出

日志默认输出在log.log 中

# 示例

![示例](./pics/show.png)
gdc 下载目录中需要有MANIFEST.txt 文件存在
