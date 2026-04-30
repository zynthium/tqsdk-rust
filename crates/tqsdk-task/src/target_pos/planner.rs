use std::collections::HashSet;

use tqsdk_core::{Order, Position, Quote, TradeDirection};

use crate::config::{PriceMode, TargetPosConfig, VolumeSplitPolicy};
use crate::plan::compute_plan;

use super::state::{DesiredBatch, DesiredOrder, LiveOrderReconciliation};

pub(super) fn desired_batch_for_target(
    symbol: &str,
    config: &TargetPosConfig,
    target_volume: i64,
    current_position: &Position,
    quote: &Quote,
) -> Option<DesiredBatch> {
    let exchange_id = quote_exchange_id(quote, symbol);
    let batch = compute_plan(
        &exchange_id,
        current_position,
        target_volume,
        config.offset_priority(),
    )
    .into_iter()
    .next()?;

    let mut orders = Vec::with_capacity(batch.orders.len());
    for order in batch.orders {
        orders.push(DesiredOrder {
            direction: order.direction,
            offset: order.offset,
            volume: split_order_volume(order.volume, config.split_policy()),
            limit_price: resolve_limit_price(quote, order.direction, config.price_mode())?,
        });
    }

    Some(DesiredBatch { orders })
}

pub(super) fn reconcile_live_orders(
    live_orders: &[Order],
    desired_batch: &DesiredBatch,
) -> LiveOrderReconciliation {
    let mut missing_orders = desired_batch.orders.clone();
    let mut stale_order_ids = HashSet::new();
    for order in live_orders {
        if !consume_compatible_desired_order(&mut missing_orders, order) {
            stale_order_ids.insert(order.order_id.clone());
        };
    }

    LiveOrderReconciliation {
        stale_order_ids,
        missing_batch: DesiredBatch {
            orders: missing_orders,
        },
    }
}

pub(super) fn quote_supports_pricing(quote: &Quote) -> bool {
    quote.ask_price1.is_finite() || quote.bid_price1.is_finite() || quote.last_price.is_finite()
}

fn consume_compatible_desired_order(missing_orders: &mut Vec<DesiredOrder>, order: &Order) -> bool {
    let exact_index = missing_orders
        .iter()
        .position(|desired_order| order_exactly_matches_desired(order, desired_order));
    let fallback_index = exact_index.or_else(|| {
        missing_orders
            .iter()
            .position(|desired_order| order_can_satisfy_desired(order, desired_order))
    });

    let Some(index) = fallback_index else {
        return false;
    };

    let live_volume = order.volume_left;
    if live_volume == missing_orders[index].volume {
        missing_orders.remove(index);
    } else {
        missing_orders[index].volume -= live_volume;
    }
    true
}

fn order_exactly_matches_desired(order: &Order, desired_order: &DesiredOrder) -> bool {
    order_can_satisfy_desired(order, desired_order) && order.volume_left == desired_order.volume
}

fn order_can_satisfy_desired(order: &Order, desired_order: &DesiredOrder) -> bool {
    order.direction == desired_order.direction.as_str()
        && order.offset == desired_order.offset.as_str()
        && order.volume_left > 0
        && order.volume_left <= desired_order.volume
        && order.limit_price == desired_order.limit_price
}

fn quote_exchange_id(quote: &Quote, symbol: &str) -> String {
    if !quote.exchange_id.is_empty() {
        return quote.exchange_id.clone();
    }

    symbol
        .split_once('.')
        .map(|(exchange_id, _)| exchange_id.to_string())
        .unwrap_or_default()
}

fn resolve_limit_price(quote: &Quote, direction: TradeDirection, mode: PriceMode) -> Option<f64> {
    let active_price = match direction {
        TradeDirection::Buy => first_finite(quote.ask_price1, quote.bid_price1, quote.last_price),
        TradeDirection::Sell => first_finite(quote.bid_price1, quote.ask_price1, quote.last_price),
    };
    let passive_price = match direction {
        TradeDirection::Buy => first_finite(quote.bid_price1, quote.ask_price1, quote.last_price),
        TradeDirection::Sell => first_finite(quote.ask_price1, quote.bid_price1, quote.last_price),
    };

    let price = match mode {
        PriceMode::Active => active_price?,
        PriceMode::Passive => passive_price?,
    };

    Some(price)
}

fn first_finite(primary: f64, secondary: f64, fallback: f64) -> Option<f64> {
    if primary.is_finite() {
        Some(primary)
    } else if secondary.is_finite() {
        Some(secondary)
    } else {
        fallback.is_finite().then_some(fallback)
    }
}

fn split_order_volume(volume: i64, split_policy: Option<VolumeSplitPolicy>) -> i64 {
    match split_policy {
        None => volume,
        Some(policy) if volume < policy.max_volume() => volume,
        Some(policy) => {
            let tail = volume - policy.max_volume();
            if tail > 0 && tail < policy.min_volume() {
                volume - policy.min_volume()
            } else {
                policy.max_volume()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tqsdk_core::{TradeDirection, TradeOffset};

    use super::*;
    use crate::config::VolumeSplitPolicy;

    #[test]
    fn reconcile_live_orders_prefers_exact_volume_match() {
        let live_orders = vec![Order {
            order_id: "order-1".to_string(),
            direction: "BUY".to_string(),
            offset: "OPEN".to_string(),
            volume_left: 4,
            limit_price: 10.0,
            ..Order::default()
        }];
        let desired_batch = DesiredBatch {
            orders: vec![
                DesiredOrder {
                    direction: TradeDirection::Buy,
                    offset: TradeOffset::Open,
                    volume: 6,
                    limit_price: 10.0,
                },
                DesiredOrder {
                    direction: TradeDirection::Buy,
                    offset: TradeOffset::Open,
                    volume: 4,
                    limit_price: 10.0,
                },
            ],
        };

        let reconciliation = reconcile_live_orders(&live_orders, &desired_batch);

        assert!(reconciliation.stale_order_ids.is_empty());
        assert_eq!(
            reconciliation.missing_batch,
            DesiredBatch {
                orders: vec![DesiredOrder {
                    direction: TradeDirection::Buy,
                    offset: TradeOffset::Open,
                    volume: 6,
                    limit_price: 10.0,
                }]
            }
        );
    }

    #[test]
    fn desired_batch_uses_real_action_types_and_split_policy() {
        let config =
            TargetPosConfig::new().with_split_policy(VolumeSplitPolicy::new(2, 5).unwrap());
        let quote = Quote {
            exchange_id: "SHFE".to_string(),
            ask_price1: 11.0,
            bid_price1: 10.0,
            last_price: 10.5,
            ..Quote::default()
        };
        let position = Position::default();

        let batch = desired_batch_for_target("SHFE.rb2601", &config, 8, &position, &quote)
            .expect("target increase should produce an order batch");

        assert_eq!(
            batch.orders,
            vec![DesiredOrder {
                direction: TradeDirection::Buy,
                offset: TradeOffset::Open,
                volume: 5,
                limit_price: 11.0,
            }]
        );
    }
}
