{#
  fct_daily_klines — daily kline OHLCV facts.
#}

select
    {{ dbt_utils.generate_surrogate_key(['symbol', 'toDate(open_at)']) }} as daily_kline_id,
    symbol,
    toDate(open_at) as trade_date,
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
    trade_date
order by
    symbol,
    trade_date
