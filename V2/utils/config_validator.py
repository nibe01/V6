"""
Config Validator
Validates config values before bot start.
Prevents runtime errors from invalid configs.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, List

from utils.logging_utils import get_logger
from utils.paths import LOGS_DIR, ensure_dirs

logger = get_logger(__name__)


@dataclass
class ValidationError:
    """Represents a validation error or warning."""
    section: str
    field: str
    value: Any
    reason: str
    severity: str  # "ERROR" or "WARNING"


class ConfigValidator:
    """
    Validates all config values.

    Checks:
    - Numeric bounds (min/max)
    - Logical consistency (SL vs TP)
    - Type correctness
    - Business rules
    """

    def __init__(self) -> None:
        self.errors: List[ValidationError] = []
        self.warnings: List[ValidationError] = []

    def validate(self, config: dict) -> bool:
        """
        Validates the complete config.

        Args:
            config: Config dict from get_config().

        Returns:
            True if valid (no errors, warnings allowed).
        """
        self.errors = []
        self.warnings = []

        # Validate each section
        self._validate_scanner_config(config.get("scanner"))
        self._validate_ib_config(config.get("ib"))
        self._validate_strategy_config(config.get("strategy"))
        self._validate_trading_config(config.get("trading"))

        # Cross-section validation
        self._validate_cross_section(config)

        return len(self.errors) == 0

    def _validate_scanner_config(self, cfg) -> None:
        """Validates scanner config."""
        if not cfg:
            self.errors.append(
                ValidationError(
                    section="scanner",
                    field="*",
                    value=None,
                    reason="Scanner config missing",
                    severity="ERROR",
                )
            )
            return

        # block_size
        if not isinstance(cfg.block_size, int):
            self.errors.append(
                ValidationError(
                    section="scanner",
                    field="block_size",
                    value=cfg.block_size,
                    reason=f"Must be int, got {type(cfg.block_size).__name__}",
                    severity="ERROR",
                )
            )
        elif cfg.block_size <= 0:
            self.errors.append(
                ValidationError(
                    section="scanner",
                    field="block_size",
                    value=cfg.block_size,
                    reason="Must be > 0",
                    severity="ERROR",
                )
            )
        elif cfg.block_size > 100:
            self.warnings.append(
                ValidationError(
                    section="scanner",
                    field="block_size",
                    value=cfg.block_size,
                    reason="Very large block_size may cause API rate limits",
                    severity="WARNING",
                )
            )

        # block_sleep_seconds
        if not isinstance(cfg.block_sleep_seconds, (int, float)):
            self.errors.append(
                ValidationError(
                    section="scanner",
                    field="block_sleep_seconds",
                    value=cfg.block_sleep_seconds,
                    reason=f"Must be numeric, got {type(cfg.block_sleep_seconds).__name__}",
                    severity="ERROR",
                )
            )
        elif cfg.block_sleep_seconds < 0:
            self.errors.append(
                ValidationError(
                    section="scanner",
                    field="block_sleep_seconds",
                    value=cfg.block_sleep_seconds,
                    reason="Cannot be negative",
                    severity="ERROR",
                )
            )
        elif cfg.block_sleep_seconds < 0.5:
            self.warnings.append(
                ValidationError(
                    section="scanner",
                    field="block_sleep_seconds",
                    value=cfg.block_sleep_seconds,
                    reason="Very fast scanning may cause API rate limits",
                    severity="WARNING",
                )
            )

        # max_symbols_per_block
        if cfg.max_symbols_per_block is not None:
            if not isinstance(cfg.max_symbols_per_block, int):
                self.errors.append(
                    ValidationError(
                        section="scanner",
                        field="max_symbols_per_block",
                        value=cfg.max_symbols_per_block,
                        reason=(
                            "Must be int or None, got "
                            f"{type(cfg.max_symbols_per_block).__name__}"
                        ),
                        severity="ERROR",
                    )
                )
            elif cfg.max_symbols_per_block <= 0:
                self.errors.append(
                    ValidationError(
                        section="scanner",
                        field="max_symbols_per_block",
                        value=cfg.max_symbols_per_block,
                        reason="Must be > 0 or None",
                        severity="ERROR",
                    )
                )

    def _validate_ib_config(self, cfg) -> None:
        """Validates IB config."""
        if not cfg:
            self.errors.append(
                ValidationError(
                    section="ib",
                    field="*",
                    value=None,
                    reason="IB config missing",
                    severity="ERROR",
                )
            )
            return

        # host
        if not isinstance(cfg.host, str):
            self.errors.append(
                ValidationError(
                    section="ib",
                    field="host",
                    value=cfg.host,
                    reason=f"Must be string, got {type(cfg.host).__name__}",
                    severity="ERROR",
                )
            )
        elif not cfg.host:
            self.errors.append(
                ValidationError(
                    section="ib",
                    field="host",
                    value=cfg.host,
                    reason="Cannot be empty",
                    severity="ERROR",
                )
            )

        # port
        if not isinstance(cfg.port, int):
            self.errors.append(
                ValidationError(
                    section="ib",
                    field="port",
                    value=cfg.port,
                    reason=f"Must be int, got {type(cfg.port).__name__}",
                    severity="ERROR",
                )
            )
        elif cfg.port < 1 or cfg.port > 65535:
            self.errors.append(
                ValidationError(
                    section="ib",
                    field="port",
                    value=cfg.port,
                    reason="Must be between 1 and 65535",
                    severity="ERROR",
                )
            )
        elif cfg.port not in [4001, 4002, 7496, 7497]:
            self.warnings.append(
                ValidationError(
                    section="ib",
                    field="port",
                    value=cfg.port,
                    reason=(
                        "Unusual IB port (standard: 7497 live, 7496 paper, "
                        "4001/4002 gateway)"
                    ),
                    severity="WARNING",
                )
            )

        # client_id
        if not isinstance(cfg.client_id, int):
            self.errors.append(
                ValidationError(
                    section="ib",
                    field="client_id",
                    value=cfg.client_id,
                    reason=f"Must be int, got {type(cfg.client_id).__name__}",
                    severity="ERROR",
                )
            )
        elif cfg.client_id < 0:
            self.errors.append(
                ValidationError(
                    section="ib",
                    field="client_id",
                    value=cfg.client_id,
                    reason="Cannot be negative",
                    severity="ERROR",
                )
            )

        # trader_client_id
        if not isinstance(cfg.trader_client_id, int):
            self.errors.append(
                ValidationError(
                    section="ib",
                    field="trader_client_id",
                    value=cfg.trader_client_id,
                    reason=f"Must be int, got {type(cfg.trader_client_id).__name__}",
                    severity="ERROR",
                )
            )
        elif cfg.trader_client_id < 0:
            self.errors.append(
                ValidationError(
                    section="ib",
                    field="trader_client_id",
                    value=cfg.trader_client_id,
                    reason="Cannot be negative",
                    severity="ERROR",
                )
            )

        # connection_check_interval_seconds
        if not isinstance(cfg.connection_check_interval_seconds, (int, float)):
            self.errors.append(
                ValidationError(
                    section="ib",
                    field="connection_check_interval_seconds",
                    value=cfg.connection_check_interval_seconds,
                    reason=(
                        "Must be numeric, got "
                        f"{type(cfg.connection_check_interval_seconds).__name__}"
                    ),
                    severity="ERROR",
                )
            )
        elif cfg.connection_check_interval_seconds <= 0:
            self.errors.append(
                ValidationError(
                    section="ib",
                    field="connection_check_interval_seconds",
                    value=cfg.connection_check_interval_seconds,
                    reason="Must be > 0",
                    severity="ERROR",
                )
            )
        elif cfg.connection_check_interval_seconds < 5:
            self.warnings.append(
                ValidationError(
                    section="ib",
                    field="connection_check_interval_seconds",
                    value=cfg.connection_check_interval_seconds,
                    reason="Very frequent connection checks may impact performance",
                    severity="WARNING",
                )
            )

    def _validate_strategy_config(self, cfg) -> None:
        """Validates strategy config."""
        if not cfg:
            self.errors.append(
                ValidationError(
                    section="strategy",
                    field="*",
                    value=None,
                    reason="Strategy config missing",
                    severity="ERROR",
                )
            )
            return

        # rule_operator
        valid_operators = ["ANY", "ALL"]
        if cfg.rule_operator not in valid_operators:
            self.errors.append(
                ValidationError(
                    section="strategy",
                    field="rule_operator",
                    value=cfg.rule_operator,
                    reason=f"Must be one of {valid_operators}",
                    severity="ERROR",
                )
            )

        # rules
        if cfg.rules is None or not isinstance(cfg.rules, list):
            self.errors.append(
                ValidationError(
                    section="strategy",
                    field="rules",
                    value=cfg.rules,
                    reason="Must be a list",
                    severity="ERROR",
                )
            )
        else:
            enabled_rules = [r for r in cfg.rules if r.enabled]
            if len(enabled_rules) == 0:
                self.errors.append(
                    ValidationError(
                        section="strategy",
                        field="rules",
                        value=cfg.rules,
                        reason="No enabled rules. Scanner will find nothing.",
                        severity="ERROR",
                    )
                )

            for i, rule in enumerate(cfg.rules):
                # threshold_pct
                if not isinstance(rule.threshold_pct, (int, float)):
                    self.errors.append(
                        ValidationError(
                            section="strategy",
                            field=f"rules[{i}].threshold_pct",
                            value=rule.threshold_pct,
                            reason=(
                                "Must be numeric, got "
                                f"{type(rule.threshold_pct).__name__}"
                            ),
                            severity="ERROR",
                        )
                    )
                elif rule.enabled:
                    if rule.threshold_pct <= 0:
                        self.errors.append(
                            ValidationError(
                                section="strategy",
                                field=f"rules[{i}].threshold_pct",
                                value=rule.threshold_pct,
                                reason="Must be > 0 for enabled rules",
                                severity="ERROR",
                            )
                        )
                    elif rule.threshold_pct > 50:
                        self.warnings.append(
                            ValidationError(
                                section="strategy",
                                field=f"rules[{i}].threshold_pct",
                                value=rule.threshold_pct,
                                reason=(
                                    "Very high threshold (>50%) may find no signals"
                                ),
                                severity="WARNING",
                            )
                        )

                # lookback_seconds
                if not isinstance(rule.lookback_seconds, int):
                    self.errors.append(
                        ValidationError(
                            section="strategy",
                            field=f"rules[{i}].lookback_seconds",
                            value=rule.lookback_seconds,
                            reason=(
                                "Must be int, got "
                                f"{type(rule.lookback_seconds).__name__}"
                            ),
                            severity="ERROR",
                        )
                    )
                elif rule.enabled:
                    if rule.lookback_seconds <= 0:
                        self.errors.append(
                            ValidationError(
                                section="strategy",
                                field=f"rules[{i}].lookback_seconds",
                                value=rule.lookback_seconds,
                                reason="Must be > 0 for enabled rules",
                                severity="ERROR",
                            )
                        )
                    elif rule.lookback_seconds > 7 * 24 * 3600:
                        self.warnings.append(
                            ValidationError(
                                section="strategy",
                                field=f"rules[{i}].lookback_seconds",
                                value=rule.lookback_seconds,
                                reason=(
                                    "Very long lookback (>7 days) may not have enough data"
                                ),
                                severity="WARNING",
                            )
                        )

        # bar_size
        valid_bar_sizes = [
            "1 secs",
            "5 secs",
            "10 secs",
            "15 secs",
            "30 secs",
            "1 min",
            "2 mins",
            "3 mins",
            "5 mins",
            "10 mins",
            "15 mins",
            "20 mins",
            "30 mins",
            "1 hour",
            "2 hours",
            "3 hours",
            "4 hours",
            "8 hours",
            "1 day",
            "1 week",
            "1 month",
        ]
        if cfg.bar_size not in valid_bar_sizes:
            self.errors.append(
                ValidationError(
                    section="strategy",
                    field="bar_size",
                    value=cfg.bar_size,
                    reason=f"Must be one of {valid_bar_sizes}",
                    severity="ERROR",
                )
            )

        # duration
        if not isinstance(cfg.duration, str):
            self.errors.append(
                ValidationError(
                    section="strategy",
                    field="duration",
                    value=cfg.duration,
                    reason=f"Must be string, got {type(cfg.duration).__name__}",
                    severity="ERROR",
                )
            )
        elif not cfg.duration:
            self.errors.append(
                ValidationError(
                    section="strategy",
                    field="duration",
                    value=cfg.duration,
                    reason="Cannot be empty",
                    severity="ERROR",
                )
            )

        # what_to_show
        valid_what_to_show = [
            "TRADES",
            "MIDPOINT",
            "BID",
            "ASK",
            "BID_ASK",
            "HISTORICAL_VOLATILITY",
            "OPTION_IMPLIED_VOLATILITY",
        ]
        if cfg.what_to_show not in valid_what_to_show:
            self.errors.append(
                ValidationError(
                    section="strategy",
                    field="what_to_show",
                    value=cfg.what_to_show,
                    reason=f"Must be one of {valid_what_to_show}",
                    severity="ERROR",
                )
            )

    def _validate_trading_config(self, cfg) -> None:
        """Validates trading config."""
        if not cfg:
            self.errors.append(
                ValidationError(
                    section="trading",
                    field="*",
                    value=None,
                    reason="Trading config missing",
                    severity="ERROR",
                )
            )
            return

        # position_size_pct
        if not isinstance(cfg.position_size_pct, (int, float)):
            self.errors.append(
                ValidationError(
                    section="trading",
                    field="position_size_pct",
                    value=cfg.position_size_pct,
                    reason=(
                        "Must be numeric, got "
                        f"{type(cfg.position_size_pct).__name__}"
                    ),
                    severity="ERROR",
                )
            )
        elif cfg.position_size_pct <= 0:
            self.errors.append(
                ValidationError(
                    section="trading",
                    field="position_size_pct",
                    value=cfg.position_size_pct,
                    reason="Must be > 0",
                    severity="ERROR",
                )
            )
        elif cfg.position_size_pct > 100:
            self.errors.append(
                ValidationError(
                    section="trading",
                    field="position_size_pct",
                    value=cfg.position_size_pct,
                    reason="Must be <= 100",
                    severity="ERROR",
                )
            )
        elif cfg.position_size_pct > 50:
            self.warnings.append(
                ValidationError(
                    section="trading",
                    field="position_size_pct",
                    value=cfg.position_size_pct,
                    reason="Very large position size (>50%) increases risk",
                    severity="WARNING",
                )
            )

        # take_profit_pct
        if not isinstance(cfg.take_profit_pct, (int, float)):
            self.errors.append(
                ValidationError(
                    section="trading",
                    field="take_profit_pct",
                    value=cfg.take_profit_pct,
                    reason=f"Must be numeric, got {type(cfg.take_profit_pct).__name__}",
                    severity="ERROR",
                )
            )
        elif cfg.take_profit_pct <= 0:
            self.errors.append(
                ValidationError(
                    section="trading",
                    field="take_profit_pct",
                    value=cfg.take_profit_pct,
                    reason="Must be > 0",
                    severity="ERROR",
                )
            )
        elif cfg.take_profit_pct < 0.1:
            self.warnings.append(
                ValidationError(
                    section="trading",
                    field="take_profit_pct",
                    value=cfg.take_profit_pct,
                    reason="Very small TP (<0.1%) may be difficult to achieve",
                    severity="WARNING",
                )
            )
        elif cfg.take_profit_pct > 50:
            self.warnings.append(
                ValidationError(
                    section="trading",
                    field="take_profit_pct",
                    value=cfg.take_profit_pct,
                    reason="Very large TP (>50%) may rarely hit",
                    severity="WARNING",
                )
            )

        # stop_loss_pct
        if not isinstance(cfg.stop_loss_pct, (int, float)):
            self.errors.append(
                ValidationError(
                    section="trading",
                    field="stop_loss_pct",
                    value=cfg.stop_loss_pct,
                    reason=f"Must be numeric, got {type(cfg.stop_loss_pct).__name__}",
                    severity="ERROR",
                )
            )
        elif cfg.stop_loss_pct <= 0:
            self.errors.append(
                ValidationError(
                    section="trading",
                    field="stop_loss_pct",
                    value=cfg.stop_loss_pct,
                    reason="Must be > 0",
                    severity="ERROR",
                )
            )
        elif cfg.stop_loss_pct < 0.5:
            self.warnings.append(
                ValidationError(
                    section="trading",
                    field="stop_loss_pct",
                    value=cfg.stop_loss_pct,
                    reason="Very tight SL (<0.5%) may be triggered by noise",
                    severity="WARNING",
                )
            )
        elif cfg.stop_loss_pct > 50:
            self.warnings.append(
                ValidationError(
                    section="trading",
                    field="stop_loss_pct",
                    value=cfg.stop_loss_pct,
                    reason="Very wide SL (>50%) creates excessive risk",
                    severity="WARNING",
                )
            )

        # auto_calculate_max_trades
        if not isinstance(cfg.auto_calculate_max_trades, bool):
            self.errors.append(
                ValidationError(
                    section="trading",
                    field="auto_calculate_max_trades",
                    value=cfg.auto_calculate_max_trades,
                    reason=(
                        "Must be bool, got "
                        f"{type(cfg.auto_calculate_max_trades).__name__}"
                    ),
                    severity="ERROR",
                )
            )

        # manual_max_open_trades
        if not isinstance(cfg.manual_max_open_trades, int):
            self.errors.append(
                ValidationError(
                    section="trading",
                    field="manual_max_open_trades",
                    value=cfg.manual_max_open_trades,
                    reason=(
                        "Must be int, got "
                        f"{type(cfg.manual_max_open_trades).__name__}"
                    ),
                    severity="ERROR",
                )
            )
        elif cfg.manual_max_open_trades <= 0:
            self.errors.append(
                ValidationError(
                    section="trading",
                    field="manual_max_open_trades",
                    value=cfg.manual_max_open_trades,
                    reason="Must be > 0",
                    severity="ERROR",
                )
            )
        elif cfg.manual_max_open_trades > 1000:
            self.warnings.append(
                ValidationError(
                    section="trading",
                    field="manual_max_open_trades",
                    value=cfg.manual_max_open_trades,
                    reason="Very high manual_max_open_trades (>1000) creates concentration risk",
                    severity="WARNING",
                )
            )

        # safety_reserve_pct
        if not isinstance(cfg.safety_reserve_pct, (int, float)):
            self.errors.append(
                ValidationError(
                    section="trading",
                    field="safety_reserve_pct",
                    value=cfg.safety_reserve_pct,
                    reason=(
                        "Must be numeric, got "
                        f"{type(cfg.safety_reserve_pct).__name__}"
                    ),
                    severity="ERROR",
                )
            )
        elif cfg.safety_reserve_pct < 0:
            self.errors.append(
                ValidationError(
                    section="trading",
                    field="safety_reserve_pct",
                    value=cfg.safety_reserve_pct,
                    reason="Must be >= 0",
                    severity="ERROR",
                )
            )
        elif cfg.safety_reserve_pct > 50:
            self.warnings.append(
                ValidationError(
                    section="trading",
                    field="safety_reserve_pct",
                    value=cfg.safety_reserve_pct,
                    reason="High safety reserve (>50%) reduces usable cash",
                    severity="WARNING",
                )
            )

        # max_daily_stop_losses
        if not isinstance(cfg.max_daily_stop_losses, int):
            self.errors.append(
                ValidationError(
                    section="trading",
                    field="max_daily_stop_losses",
                    value=cfg.max_daily_stop_losses,
                    reason=(
                        "Must be int, got "
                        f"{type(cfg.max_daily_stop_losses).__name__}"
                    ),
                    severity="ERROR",
                )
            )
        elif cfg.max_daily_stop_losses <= 0:
            self.errors.append(
                ValidationError(
                    section="trading",
                    field="max_daily_stop_losses",
                    value=cfg.max_daily_stop_losses,
                    reason="Must be > 0",
                    severity="ERROR",
                )
            )
        elif not cfg.auto_calculate_max_trades:
            if cfg.max_daily_stop_losses > cfg.manual_max_open_trades * 2:
                self.warnings.append(
                    ValidationError(
                        section="trading",
                        field="max_daily_stop_losses",
                        value=cfg.max_daily_stop_losses,
                        reason=(
                            "Very high max_daily_stop_losses "
                            f"(>{cfg.manual_max_open_trades * 2}) may allow excessive losses"
                        ),
                        severity="WARNING",
                    )
                )

        # max_entry_slippage_pct
        if hasattr(cfg, 'max_entry_slippage_pct'):
            if not isinstance(cfg.max_entry_slippage_pct, (int, float)):
                self.errors.append(
                    ValidationError(
                        section="trading",
                        field="max_entry_slippage_pct",
                        value=cfg.max_entry_slippage_pct,
                        reason=f"Must be numeric, got {type(cfg.max_entry_slippage_pct).__name__}",
                        severity="ERROR",
                    )
                )
            elif cfg.max_entry_slippage_pct < 0:
                self.errors.append(
                    ValidationError(
                        section="trading",
                        field="max_entry_slippage_pct",
                        value=cfg.max_entry_slippage_pct,
                        reason="Cannot be negative",
                        severity="ERROR",
                    )
                )
            elif cfg.max_entry_slippage_pct > 5.0:
                self.warnings.append(
                    ValidationError(
                        section="trading",
                        field="max_entry_slippage_pct",
                        value=cfg.max_entry_slippage_pct,
                        reason="Very high slippage tolerance (>5%) - consider reducing",
                        severity="WARNING",
                    )
                )
            elif cfg.max_entry_slippage_pct < 0.1:
                self.warnings.append(
                    ValidationError(
                        section="trading",
                        field="max_entry_slippage_pct",
                        value=cfg.max_entry_slippage_pct,
                        reason="Very tight slippage tolerance (<0.1%) - order may not fill",
                        severity="WARNING",
                    )
                )

        # entry_retry_block_seconds
        if hasattr(cfg, 'entry_retry_block_seconds'):
            if not isinstance(cfg.entry_retry_block_seconds, int):
                self.errors.append(
                    ValidationError(
                        section="trading",
                        field="entry_retry_block_seconds",
                        value=cfg.entry_retry_block_seconds,
                        reason=f"Must be int, got {type(cfg.entry_retry_block_seconds).__name__}",
                        severity="ERROR",
                    )
                )
            elif cfg.entry_retry_block_seconds < 0:
                self.errors.append(
                    ValidationError(
                        section="trading",
                        field="entry_retry_block_seconds",
                        value=cfg.entry_retry_block_seconds,
                        reason="Must be >= 0",
                        severity="ERROR",
                    )
                )
            elif cfg.entry_retry_block_seconds > 600:
                self.warnings.append(
                    ValidationError(
                        section="trading",
                        field="entry_retry_block_seconds",
                        value=cfg.entry_retry_block_seconds,
                        reason="Very high retry block (>600s) may skip too many valid re-entries",
                        severity="WARNING",
                    )
                )

        # exit_protection_verify_timeout_seconds
        if hasattr(cfg, 'exit_protection_verify_timeout_seconds'):
            if not isinstance(cfg.exit_protection_verify_timeout_seconds, (int, float)):
                self.errors.append(
                    ValidationError(
                        section="trading",
                        field="exit_protection_verify_timeout_seconds",
                        value=cfg.exit_protection_verify_timeout_seconds,
                        reason=(
                            "Must be numeric, got "
                            f"{type(cfg.exit_protection_verify_timeout_seconds).__name__}"
                        ),
                        severity="ERROR",
                    )
                )
            elif cfg.exit_protection_verify_timeout_seconds <= 0:
                self.errors.append(
                    ValidationError(
                        section="trading",
                        field="exit_protection_verify_timeout_seconds",
                        value=cfg.exit_protection_verify_timeout_seconds,
                        reason="Must be > 0 seconds",
                        severity="ERROR",
                    )
                )
            elif cfg.exit_protection_verify_timeout_seconds < 1.0:
                self.warnings.append(
                    ValidationError(
                        section="trading",
                        field="exit_protection_verify_timeout_seconds",
                        value=cfg.exit_protection_verify_timeout_seconds,
                        reason="Very low timeout (<1s) may miss valid broker updates",
                        severity="WARNING",
                    )
                )
            elif cfg.exit_protection_verify_timeout_seconds > 60.0:
                self.warnings.append(
                    ValidationError(
                        section="trading",
                        field="exit_protection_verify_timeout_seconds",
                        value=cfg.exit_protection_verify_timeout_seconds,
                        reason="Very high timeout (>60s) may delay failure handling",
                        severity="WARNING",
                    )
                )

        # exit_protection_verify_check_interval_seconds
        if hasattr(cfg, 'exit_protection_verify_check_interval_seconds'):
            if not isinstance(cfg.exit_protection_verify_check_interval_seconds, (int, float)):
                self.errors.append(
                    ValidationError(
                        section="trading",
                        field="exit_protection_verify_check_interval_seconds",
                        value=cfg.exit_protection_verify_check_interval_seconds,
                        reason=(
                            "Must be numeric, got "
                            f"{type(cfg.exit_protection_verify_check_interval_seconds).__name__}"
                        ),
                        severity="ERROR",
                    )
                )
            elif cfg.exit_protection_verify_check_interval_seconds <= 0:
                self.errors.append(
                    ValidationError(
                        section="trading",
                        field="exit_protection_verify_check_interval_seconds",
                        value=cfg.exit_protection_verify_check_interval_seconds,
                        reason="Must be > 0 seconds",
                        severity="ERROR",
                    )
                )
            elif cfg.exit_protection_verify_check_interval_seconds < 0.1:
                self.warnings.append(
                    ValidationError(
                        section="trading",
                        field="exit_protection_verify_check_interval_seconds",
                        value=cfg.exit_protection_verify_check_interval_seconds,
                        reason="Very low polling interval (<0.1s) increases IB request load",
                        severity="WARNING",
                    )
                )
            elif cfg.exit_protection_verify_check_interval_seconds > 5.0:
                self.warnings.append(
                    ValidationError(
                        section="trading",
                        field="exit_protection_verify_check_interval_seconds",
                        value=cfg.exit_protection_verify_check_interval_seconds,
                        reason="Very high polling interval (>5s) slows protection verification",
                        severity="WARNING",
                    )
                )

        # signal_queue_warning_bytes
        if hasattr(cfg, 'signal_queue_warning_bytes'):
            if not isinstance(cfg.signal_queue_warning_bytes, int):
                self.errors.append(
                    ValidationError(
                        section="trading",
                        field="signal_queue_warning_bytes",
                        value=cfg.signal_queue_warning_bytes,
                        reason=(
                            "Must be int, got "
                            f"{type(cfg.signal_queue_warning_bytes).__name__}"
                        ),
                        severity="ERROR",
                    )
                )
            elif cfg.signal_queue_warning_bytes < 1024:
                self.errors.append(
                    ValidationError(
                        section="trading",
                        field="signal_queue_warning_bytes",
                        value=cfg.signal_queue_warning_bytes,
                        reason="Must be >= 1024 bytes",
                        severity="ERROR",
                    )
                )
            elif cfg.signal_queue_warning_bytes > 2 * 1024 * 1024 * 1024:
                self.warnings.append(
                    ValidationError(
                        section="trading",
                        field="signal_queue_warning_bytes",
                        value=cfg.signal_queue_warning_bytes,
                        reason="Very high queue warning threshold (>2GB)",
                        severity="WARNING",
                    )
                )

        # signal_queue_warning_interval_seconds
        if hasattr(cfg, 'signal_queue_warning_interval_seconds'):
            if not isinstance(cfg.signal_queue_warning_interval_seconds, int):
                self.errors.append(
                    ValidationError(
                        section="trading",
                        field="signal_queue_warning_interval_seconds",
                        value=cfg.signal_queue_warning_interval_seconds,
                        reason=(
                            "Must be int, got "
                            f"{type(cfg.signal_queue_warning_interval_seconds).__name__}"
                        ),
                        severity="ERROR",
                    )
                )
            elif cfg.signal_queue_warning_interval_seconds < 10:
                self.errors.append(
                    ValidationError(
                        section="trading",
                        field="signal_queue_warning_interval_seconds",
                        value=cfg.signal_queue_warning_interval_seconds,
                        reason="Must be >= 10 seconds",
                        severity="ERROR",
                    )
                )
            elif cfg.signal_queue_warning_interval_seconds > 24 * 3600:
                self.warnings.append(
                    ValidationError(
                        section="trading",
                        field="signal_queue_warning_interval_seconds",
                        value=cfg.signal_queue_warning_interval_seconds,
                        reason="Very high warning interval (>24h)",
                        severity="WARNING",
                    )
                )

        # force_emergency_exit_if_any_protection_missing
        if hasattr(cfg, 'force_emergency_exit_if_any_protection_missing'):
            if not isinstance(cfg.force_emergency_exit_if_any_protection_missing, bool):
                self.errors.append(
                    ValidationError(
                        section="trading",
                        field="force_emergency_exit_if_any_protection_missing",
                        value=cfg.force_emergency_exit_if_any_protection_missing,
                        reason=(
                            "Must be bool, got "
                            f"{type(cfg.force_emergency_exit_if_any_protection_missing).__name__}"
                        ),
                        severity="ERROR",
                    )
                )

    def _validate_cross_section(self, config: dict) -> None:
        """Validates relationships between config sections."""
        ib_cfg = config.get("ib")
        trading_cfg = config.get("trading")

        if not ib_cfg or not trading_cfg:
            return

        # Critical: scanner and trader must not share client_id
        if ib_cfg.client_id == ib_cfg.trader_client_id:
            self.errors.append(
                ValidationError(
                    section="ib",
                    field="client_id / trader_client_id",
                    value=f"{ib_cfg.client_id} / {ib_cfg.trader_client_id}",
                    reason=(
                        "Scanner and Trader must have different client_ids. "
                        "IB will reject duplicate connections."
                    ),
                    severity="ERROR",
                )
            )

        # Risk/reward validation
        if isinstance(trading_cfg.take_profit_pct, (int, float)) and isinstance(
            trading_cfg.stop_loss_pct, (int, float)
        ):
            if trading_cfg.stop_loss_pct > trading_cfg.take_profit_pct:
                risk_reward = trading_cfg.take_profit_pct / trading_cfg.stop_loss_pct
                self.warnings.append(
                    ValidationError(
                        section="trading",
                        field="take_profit_pct / stop_loss_pct",
                        value=(
                            f"{trading_cfg.take_profit_pct}% / "
                            f"{trading_cfg.stop_loss_pct}%"
                        ),
                        reason=(
                            "Risk/reward ratio is unfavorable: "
                            f"{risk_reward:.2f}:1 (TP < SL). "
                            "High win rate required to be profitable."
                        ),
                        severity="WARNING",
                    )
                )

            # Extreme SL/TP ratios are risky but allow for testing.
            if trading_cfg.stop_loss_pct >= trading_cfg.take_profit_pct * 5:
                self.warnings.append(
                    ValidationError(
                        section="trading",
                        field="take_profit_pct / stop_loss_pct",
                        value=(
                            f"{trading_cfg.take_profit_pct}% / "
                            f"{trading_cfg.stop_loss_pct}%"
                        ),
                        reason=(
                            "Stop-loss is "
                            f"{trading_cfg.stop_loss_pct / trading_cfg.take_profit_pct:.1f}x "
                            "larger than take-profit. This is almost certainly a mistake."
                        ),
                        severity="WARNING",
                    )
                )

    def print_report(self) -> None:
        """Prints the validation report."""
        print(self.build_report())

    def build_report(self) -> str:
        """Builds the validation report as a string."""
        if not self.errors and not self.warnings:
            return "Config validation passed!"

        lines: List[str] = []
        lines.append("=" * 80)
        lines.append("CONFIG VALIDATION REPORT")
        lines.append("=" * 80)

        if self.errors:
            lines.append(f"\nERRORS ({len(self.errors)}):")
            lines.append("-" * 80)
            for err in self.errors:
                lines.append(f"  [{err.section}.{err.field}]")
                lines.append(f"    Value: {err.value}")
                lines.append(f"    Reason: {err.reason}")
                lines.append("")

        if self.warnings:
            lines.append(f"\nWARNINGS ({len(self.warnings)}):")
            lines.append("-" * 80)
            for warn in self.warnings:
                lines.append(f"  [{warn.section}.{warn.field}]")
                lines.append(f"    Value: {warn.value}")
                lines.append(f"    Reason: {warn.reason}")
                lines.append("")

        lines.append("=" * 80)
        return "\n".join(lines)


def _write_report_to_log(report: str) -> None:
    ensure_dirs()
    log_dir = LOGS_DIR / "config_validation"
    log_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    log_file = log_dir / f"config_validation_{today}.log"
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(report)
        f.write("\n")


def validate_config(config: dict, exit_on_error: bool = True) -> bool:
    """
    Validates config and prints report.

    Args:
        config: Config dict from get_config().
        exit_on_error: When True, exit on validation errors.

    Returns:
        True if valid.
    """
    import sys

    validator = ConfigValidator()
    is_valid = validator.validate(config)

    report = validator.build_report()
    print(report)
    _write_report_to_log(report)

    if not is_valid and exit_on_error:
        print("\nConfig validation failed!")
        print("Please fix the errors above and restart.")
        sys.exit(1)

    return is_valid
