{#
  fct_hourly_klines — hourly kline OHLCV facts.
#}

select
    {{ dbt_utils.generate_surrogate_key(['symbol', 'toStartOfHour(open_at)']) }} as hourly_kline_id,
    symbol,
    toStartOfHour(open_at) as hour_at,
    argMin(open, open_at) as open,
    max(high) as high,
    min(low) as low,
    argMax(close, open_at) as close,
    sum(volume) as volume,
    sum(quote_volume) as quote_volume,
    sum(num_trades) as num_trades
from {{ ref('stg_crypto__klines') }}
where interval = '1m'
group by
    symbol,
    hour_at
order by
    symbol,
    hour_at
