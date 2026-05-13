# Flink 窗口类型 & 业务场景

## 1. Tumble（滚动窗口）

**特点**：固定大小，不重叠，到点触发一次输出。

```
10:00       11:00       12:00       13:00
  |===========|===========|===========|
    窗口1        窗口2        窗口3
```

**SQL**：
```sql
TUMBLE(create_time, INTERVAL '1' HOUR)
```

**场景**：每小时 GMV 报表

```sql
SELECT
    TUMBLE_START(create_time, INTERVAL '1' HOUR) AS hour_start,
    SUM(price * quantity) AS gmv
FROM order_sales
GROUP BY TUMBLE(create_time, INTERVAL '1' HOUR)
```

| hour_start | gmv |
|-------------|------|
| 10:00 | 156,800 |
| 11:00 | 223,400 |
| 12:00 | 189,100 |

典型用途：日报、周报、月报等定时出数场景。窗口结束才输出，延迟最高为一个窗口大小。


## 2. Hop（滑动窗口）

**特点**：固定大小，有重叠，每滑动一次输出一次。slide < size 时窗口重叠，数据会被多条窗口包含。

```
size=1h, slide=30min

10:00    10:30    11:00    11:30    12:00
  |========|
       |========|
            |========|
                 |========|
```

**SQL**：
```sql
HOP(create_time, INTERVAL '30' MINUTE, INTERVAL '1' HOUR)
```

**场景**：实时"最近1小时热门商品 Top10"，每 5 分钟刷新

```sql
SELECT
    HOP_START(event_time, INTERVAL '5' MINUTE, INTERVAL '1' HOUR),
    product_name,
    COUNT(*) AS cnt
FROM click_log
GROUP BY HOP(event_time, INTERVAL '5' MINUTE, INTERVAL '1' HOUR),
         product_name
```

| window_start | product_name | cnt |
|-------------|-------------|------|
| 10:00 | iPhone 15 | 342 |
| 10:05 | iPhone 15 | 389 |
| 10:10 | MacBook Pro | 401 |

典型用途：近 N 分钟热榜、异常流量检测（近 5 分钟请求量是否超过阈值）、大促实时战报。


## 3. Session（会话窗口）

**特点**：无固定大小，按事件活跃间隔动态切分。相邻两条数据间隔超过 gap 就关窗。

```
gap=30min
|||  |||||  (35min 无事件)  |||  (40min 无事件)  ||
窗1                         窗2                 窗3
```

**SQL**：
```sql
SESSION(event_time, INTERVAL '30' MINUTE)
```

**场景**：用户访问 Session 分析

```sql
SELECT
    user_id,
    SESSION_START(event_time, INTERVAL '30' MINUTE) AS session_start,
    SESSION_END(event_time, INTERVAL '30' MINUTE) AS session_end,
    COUNT(*) AS pv,
    MAX(event_time) - MIN(event_time) AS duration
FROM user_click
GROUP BY user_id, SESSION(event_time, INTERVAL '30' MINUTE)
```

| user_id | session_start | duration | pv |
|---------|-------------|------|------|
| 1001 | 10:05 | 23min | 15 |
| 1001 | 11:20 | 8min | 6 |
| 1002 | 10:12 | 45min | 32 |

典型用途：用户行为路径分析（一个 session 里的页面跳转序列）、停留时长统计、直播观看 session 切分。


## 4. Cumulate（累积窗口）

**特点**：指定 step 和 max size。窗口内每 step 触发一次输出，一次比一次大，直到达到 max size 关窗重来。

```
max=1day, step=1hour
00:00 ───────────────────────────────────── 24:00
  |==|  ← 00:00~01:00
  |=====|  ← 00:00~02:00  
  |========|  ← 00:00~03:00
  ...
  |================================|  ← 00:00~24:00（最后一条）
第二天重新从0开始
```

**SQL**：
```sql
CUMULATE(create_time, INTERVAL '1' HOUR, INTERVAL '1' DAY)
```

**场景**：双11当天累计 GMV 大屏，每小时刷新

```sql
SELECT
    product_name,
    CUMULATE_START(create_time, INTERVAL '1' HOUR, INTERVAL '1' DAY),
    CUMULATE_END(create_time, INTERVAL '1' HOUR, INTERVAL '1' DAY),
    SUM(price * quantity) AS cumulative_gmv
FROM order_sales
GROUP BY
    product_name,
    CUMULATE(create_time, INTERVAL '1' HOUR, INTERVAL '1' DAY)
```

| product_name | cumulate_start | cumulate_end | cumulative_gmv |
|-------------|---------------|-------------|------|
| iPhone 15 | 00:00 | 09:00 | 890万 |
| iPhone 15 | 00:00 | 10:00 | 1020万 |
| iPhone 15 | 00:00 | 23:00 | 3400万 |

典型用途：促销活动当日累计、监控"从零点到现在的累计指标"。

---

## 对比总结

| 窗口 | 数据归属 | 输出频率 | 核心参数 | 一句话 |
|------|------|------|------|------|
| Tumble | 每条数据只属于一个窗口 | 窗口结束时 1 次 | size | 切面包，每片独立 |
| Hop | 每条数据可能属于多个窗口 | 每 slide 1 次 | size + slide | 重叠的滚动窗口 |
| Session | 按事件间隔动态切分 | 每个 session 结束时 | gap | 用户会话分群 |
| Cumulate | 每条数据只属于一个最大窗口 | 每 step 1 次，逐步扩大 | max + step | 当天累计，逐时放大 |
