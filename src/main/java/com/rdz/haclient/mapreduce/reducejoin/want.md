# reduce端表合并（数据倾斜）
## 输入数据
### order.txt
|  订单ID   | pid  | 数量  |
|  ----  | ----  | ----  |
| 1001  | 01 | 1  |
| 1002  | 02 | 2  |
| 1003  | 03 | 3  |
| 1004  | 01 | 1  |
| 1005  | 02 | 2  |

### pd.txt
| pid | 产品名称 |
| ----| ----|
| 01 | 小米 |
| 02 | 华为 |
| 03 | 格力 |

## 预期输出结果
|  订单ID   | 产品名称  | 数量  |
|  ----  | ----  | ----  |
| 1001  | 小米 | 1  |
| 1002  | 华为 | 2  |
| 1003  | 格力 | 3  |
| 1004  | 小米 | 1  |
| 1005  | 华为 | 2  |