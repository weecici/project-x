{#
  stg_crypto__klines — standard staging model for Binance kline data.
#}

select
    {{ dbt_utils.generate_surrogate_key(['symbol', 'interval', 'open_time']) }} as kline_id,
    symbol,
    interval,
    open_time as open_at,
    open,
    high,
    low,
    close,
    volume,
    close_time as close_at,
    quote_volume,
    num_trades,
    taker_buy_base_volume,
    taker_buy_quote_volume
from {{ source('silver', 'klines_raw') }} FINAL
