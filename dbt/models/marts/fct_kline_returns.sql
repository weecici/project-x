{#
  fct_kline_returns — kline log returns.
#}

select
    {{ dbt_utils.generate_surrogate_key(['symbol', 'interval', 'open_at']) }} as kline_return_id,
    symbol,
    interval,
    open_at,
    close,
    log(toFloat64(close) / nullIf(toFloat64(lag(close) over (partition by symbol, interval order by open_at)), 0)) as log_return
from {{ ref('stg_crypto__klines') }}
order by
    symbol,
    interval,
    open_at
