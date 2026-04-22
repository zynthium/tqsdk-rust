#![cfg_attr(not(test), forbid(unsafe_code))]

use tqsdk_core::{Position, TradeDirection, TradeOffset};

use crate::OffsetPriority;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedBatch {
    pub(crate) orders: Vec<PlannedOrder>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedOrder {
    pub(crate) direction: TradeDirection,
    pub(crate) offset: TradeOffset,
    pub(crate) volume: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum OffsetAction {
    Today,
    Yesterday,
    Open,
}

pub(crate) fn compute_plan(
    exchange_id: &str,
    position: &Position,
    target_volume: i64,
    priority: OffsetPriority,
) -> Vec<PlannedBatch> {
    let mut remaining = target_volume - net_position(position);
    if remaining == 0 {
        return Vec::new();
    }

    let mut batches = Vec::new();
    for group in offset_priority_groups(priority) {
        if remaining == 0 {
            break;
        }

        let mut pending_frozen = 0_i64;
        let mut orders = Vec::new();
        for action in group {
            if remaining == 0 {
                break;
            }

            let Some((order, frozen_delta)) =
                planned_order_for_action(action, remaining, pending_frozen, exchange_id, position)
            else {
                continue;
            };

            pending_frozen += frozen_delta;
            remaining -= signed_volume(order.direction, order.volume);
            orders.push(order);
        }

        if !orders.is_empty() {
            batches.push(PlannedBatch { orders });
        }
    }

    batches
}

fn offset_priority_groups(priority: OffsetPriority) -> Vec<Vec<OffsetAction>> {
    match priority {
        OffsetPriority::TodayYesterdayThenOpenWait => vec![
            vec![OffsetAction::Today, OffsetAction::Yesterday],
            vec![OffsetAction::Open],
        ],
        OffsetPriority::TodayYesterdayThenOpen => {
            vec![vec![
                OffsetAction::Today,
                OffsetAction::Yesterday,
                OffsetAction::Open,
            ]]
        }
        OffsetPriority::YesterdayThenOpen => {
            vec![vec![OffsetAction::Yesterday, OffsetAction::Open]]
        }
        OffsetPriority::OpenOnly => vec![vec![OffsetAction::Open]],
    }
}

fn planned_order_for_action(
    action: OffsetAction,
    remaining: i64,
    pending_frozen: i64,
    exchange_id: &str,
    position: &Position,
) -> Option<(PlannedOrder, i64)> {
    let direction = if remaining > 0 {
        TradeDirection::Buy
    } else {
        TradeDirection::Sell
    };
    let requested = remaining.abs();
    let is_shfe_like = matches!(exchange_id, "SHFE" | "INE");

    let (offset, available) = match action {
        OffsetAction::Open => (TradeOffset::Open, requested),
        OffsetAction::Today => {
            if is_shfe_like {
                let available = match direction {
                    TradeDirection::Buy => short_today(position) - short_frozen_today(position),
                    TradeDirection::Sell => long_today(position) - long_frozen_today(position),
                };
                (TradeOffset::CloseToday, available)
            } else {
                let available = match direction {
                    TradeDirection::Buy => short_today(position),
                    TradeDirection::Sell => long_today(position),
                } - pending_frozen
                    - total_close_frozen(position, direction);
                (TradeOffset::Close, available)
            }
        }
        OffsetAction::Yesterday => {
            if is_shfe_like {
                let available = match direction {
                    TradeDirection::Buy => short_his(position) - short_frozen_his(position),
                    TradeDirection::Sell => long_his(position) - long_frozen_his(position),
                };
                (TradeOffset::Close, available)
            } else {
                let frozen = pending_frozen + total_close_frozen(position, direction);
                let today_left = match direction {
                    TradeDirection::Buy => short_today(position) - frozen,
                    TradeDirection::Sell => long_today(position) - frozen,
                };
                if today_left > 0 {
                    (TradeOffset::Close, 0)
                } else {
                    let total_available = match direction {
                        TradeDirection::Buy => short_total(position),
                        TradeDirection::Sell => long_total(position),
                    };
                    (TradeOffset::Close, total_available - frozen)
                }
            }
        }
    };

    let volume = requested.min(available.max(0));
    if volume == 0 {
        return None;
    }

    let frozen_delta = if matches!(offset, TradeOffset::Open) {
        0
    } else {
        volume
    };

    Some((
        PlannedOrder {
            direction,
            offset,
            volume,
        },
        frozen_delta,
    ))
}

fn signed_volume(direction: TradeDirection, volume: i64) -> i64 {
    match direction {
        TradeDirection::Buy => volume,
        TradeDirection::Sell => -volume,
    }
}

pub(crate) fn net_position(position: &Position) -> i64 {
    if position.pos != 0 {
        position.pos
    } else {
        long_total(position) - short_total(position)
    }
}

fn long_today(position: &Position) -> i64 {
    if position.pos_long_today != 0 || position.pos_long_his != 0 {
        position.pos_long_today
    } else {
        position.volume_long_today
    }
}

fn long_his(position: &Position) -> i64 {
    if position.pos_long_today != 0 || position.pos_long_his != 0 {
        position.pos_long_his
    } else {
        position.volume_long_his
    }
}

fn long_total(position: &Position) -> i64 {
    if position.pos_long_today != 0 || position.pos_long_his != 0 {
        position.pos_long_today + position.pos_long_his
    } else if position.pos_long != 0 {
        position.pos_long
    } else if position.volume_long != 0 {
        position.volume_long
    } else {
        position.volume_long_today + position.volume_long_his
    }
}

fn short_today(position: &Position) -> i64 {
    if position.pos_short_today != 0 || position.pos_short_his != 0 {
        position.pos_short_today
    } else {
        position.volume_short_today
    }
}

fn short_his(position: &Position) -> i64 {
    if position.pos_short_today != 0 || position.pos_short_his != 0 {
        position.pos_short_his
    } else {
        position.volume_short_his
    }
}

fn short_total(position: &Position) -> i64 {
    if position.pos_short_today != 0 || position.pos_short_his != 0 {
        position.pos_short_today + position.pos_short_his
    } else if position.pos_short != 0 {
        position.pos_short
    } else if position.volume_short != 0 {
        position.volume_short
    } else {
        position.volume_short_today + position.volume_short_his
    }
}

fn long_frozen_today(position: &Position) -> i64 {
    position.volume_long_frozen_today
}

fn long_frozen_his(position: &Position) -> i64 {
    position.volume_long_frozen_his
}

fn short_frozen_today(position: &Position) -> i64 {
    position.volume_short_frozen_today
}

fn short_frozen_his(position: &Position) -> i64 {
    position.volume_short_frozen_his
}

fn total_close_frozen(position: &Position, direction: TradeDirection) -> i64 {
    match direction {
        TradeDirection::Buy => {
            if position.volume_short_frozen != 0 {
                position.volume_short_frozen
            } else {
                position.volume_short_frozen_today + position.volume_short_frozen_his
            }
        }
        TradeDirection::Sell => {
            if position.volume_long_frozen != 0 {
                position.volume_long_frozen
            } else {
                position.volume_long_frozen_today + position.volume_long_frozen_his
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tqsdk_core::{Position, TradeDirection, TradeOffset};

    use super::{PlannedBatch, PlannedOrder, compute_plan};
    use crate::OffsetPriority;

    #[test]
    fn open_only_plan_opens_from_flat_position() {
        let position = Position::default();

        let plan = compute_plan("SHFE", &position, 2, OffsetPriority::OpenOnly);

        assert_eq!(
            plan,
            vec![PlannedBatch {
                orders: vec![PlannedOrder {
                    direction: TradeDirection::Buy,
                    offset: TradeOffset::Open,
                    volume: 2,
                }],
            }]
        );
    }

    #[test]
    fn shfe_default_plan_prefers_close_today_then_close_yesterday_then_open() {
        let position = Position {
            pos_long_today: 1,
            pos_long_his: 1,
            ..Position::default()
        };

        let plan = compute_plan(
            "SHFE",
            &position,
            -1,
            OffsetPriority::TodayYesterdayThenOpen,
        );

        assert_eq!(
            plan,
            vec![PlannedBatch {
                orders: vec![
                    PlannedOrder {
                        direction: TradeDirection::Sell,
                        offset: TradeOffset::CloseToday,
                        volume: 1,
                    },
                    PlannedOrder {
                        direction: TradeDirection::Sell,
                        offset: TradeOffset::Close,
                        volume: 1,
                    },
                    PlannedOrder {
                        direction: TradeDirection::Sell,
                        offset: TradeOffset::Open,
                        volume: 1,
                    },
                ],
            }]
        );
    }

    #[test]
    fn non_shfe_default_plan_uses_close_for_today_and_yesterday() {
        let position = Position {
            pos_long_today: 1,
            pos_long_his: 1,
            ..Position::default()
        };

        let plan = compute_plan(
            "CFFEX",
            &position,
            -1,
            OffsetPriority::TodayYesterdayThenOpen,
        );

        assert_eq!(
            plan,
            vec![PlannedBatch {
                orders: vec![
                    PlannedOrder {
                        direction: TradeDirection::Sell,
                        offset: TradeOffset::Close,
                        volume: 1,
                    },
                    PlannedOrder {
                        direction: TradeDirection::Sell,
                        offset: TradeOffset::Close,
                        volume: 1,
                    },
                    PlannedOrder {
                        direction: TradeDirection::Sell,
                        offset: TradeOffset::Open,
                        volume: 1,
                    },
                ],
            }]
        );
    }

    #[test]
    fn yesterday_then_open_skips_today_position() {
        let position = Position {
            pos_long_today: 1,
            pos_long_his: 2,
            ..Position::default()
        };

        let plan = compute_plan("SHFE", &position, 0, OffsetPriority::YesterdayThenOpen);

        assert_eq!(
            plan,
            vec![PlannedBatch {
                orders: vec![
                    PlannedOrder {
                        direction: TradeDirection::Sell,
                        offset: TradeOffset::Close,
                        volume: 2,
                    },
                    PlannedOrder {
                        direction: TradeDirection::Sell,
                        offset: TradeOffset::Open,
                        volume: 1,
                    },
                ],
            }]
        );
    }
}
