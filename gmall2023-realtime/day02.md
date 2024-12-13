**数据倾斜:**
   当数量很多的时候,要求 每个地区的数据量,假如一个地区为千万级别,一个是百万级别的时候,这个时候就会发生严重的数据倾斜
    key值分布不均价:在进行join,group by操作的时候,key可能会导致多的数据分配到少的那个几点上
	解决:,
	with t1 as (
    select userid,
           order_no,
           concat(region,'-',rand()) as region,
           product_no,
           color_no,
           sale_amount,
           ts
    from date_east
),
    t2 as (
        select region,
               count(1) as cnt
        from t1
        group by region
    ),
    t3 as (
        select region,
               substr(region,1,2) as re,
               cnt
        from t2
    )
select re,
       count(1) as cnt
from t3
group by re;

**数仓**

dim层:

该层组要负责存储维度表 

使用cdc读取配置表中 数据,对配置流中的数据类型进行转换  json  -> 实体类对象

读取kafka数据,我们为了保证消费的准确一次性的时候,可以收哦的那个维护偏移量

```
//setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
```

读取数据后,封装为流,可以对业务流中的类型进行转化并且进行etl

将数据存储到HBase中

Constan常量类:

将一些配置信息统一的放在常量类里面

使用常量类可以提高查询的效率,也可以确保数据的一致性








