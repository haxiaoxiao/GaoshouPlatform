# 表达式策略模板

适用于单因子或简单多因子线性组合。回测引擎按日计算因子值，分层买入。

## 示例

```
close / MA(close, 20) - 1
```

```
Mean($close, 5) / Std($close, 20)
```

## 格式要求

- 单行表达式
- 可用变量: $close/$open/$high/$low/$volume/$amount/$turnover
- 可用函数: Mean/Std/Sum/Min/Max/SMA/EMA/RSI/MACD/Corr
- 可用指标: indicator(name, series)
