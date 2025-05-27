CREATE TABLE warehouse_balances.sales_by_day
(

    `date` Date,

    `nmId` UInt32,

    `orders` UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date,
 nmId)
SETTINGS index_granularity = 8192;



CREATE TABLE warehouse_balances.wb_stocks_buffer
(

    `date` Date,

    `nmId` UInt32,

    `warehouse_id` UInt32 DEFAULT 0,

    `stocks` UInt32
)
ENGINE = Buffer('warehouse_balances',
 'wb_stocks_main',
 8,
 30,
 60,
 10,
 1000,
 102400,
 10485760,
 60,
 20,
 204800);

CREATE TABLE warehouse_balances.wb_stocks_main
(

    `date` Date,

    `nmId` UInt32,

    `warehouse_id` UInt32 DEFAULT 0,

    `stocks` UInt32
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date,
 nmId,
 warehouse_id)
SETTINGS index_granularity = 8192;