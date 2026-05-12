from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import aiosqlite

import config

log = logging.getLogger(__name__)


def now_utc():
    return datetime.now(timezone.utc)


def parse_dt(value):
    try:
        return datetime.fromisoformat(
            str(value).replace("Z", "+00:00")
        )
    except Exception:
        return None


CREATE_MARKET_DATA_ALERTS = """
CREATE TABLE IF NOT EXISTS market_data_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_type TEXT,
    severity TEXT,
    message TEXT,
    created_at TEXT
)
"""


async def init_market_guard_db():

    async with aiosqlite.connect(
        config.DB_PATH
    ) as db:

        await db.execute(
            CREATE_MARKET_DATA_ALERTS
        )

        await db.commit()


async def create_alert(
    alert_type: str,
    severity: str,
    message: str,
):

    async with aiosqlite.connect(
        config.DB_PATH
    ) as db:

        await db.execute(
            """
            INSERT INTO market_data_alerts (
                alert_type,
                severity,
                message,
                created_at
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                alert_type,
                severity,
                message,
                now_utc().isoformat(),
            ),
        )

        await db.commit()

    log.warning(
        "MARKET DATA ALERT | %s | %s",
        alert_type,
        message,
    )


async def run_market_data_guard():

    await init_market_guard_db()

    alerts = []

    async with aiosqlite.connect(
        config.DB_PATH
    ) as db:

        db.row_factory = aiosqlite.Row

        cursor = await db.execute(
            """
            SELECT
                connected,
                last_sync_at,
                error
            FROM tws_heartbeat
            WHERE id = 1
            """
        )

        row = await cursor.fetchone()

        if not row:

            message = (
                "No TWS heartbeat found"
            )

            alerts.append(message)

            await create_alert(
                "NO_HEARTBEAT",
                "CRITICAL",
                message,
            )

            return {
                "ok": False,
                "alerts": alerts,
            }

        connected = bool(row["connected"])

        last_sync_at = parse_dt(
            row["last_sync_at"]
        )

        error = row["error"]

        # ==================================
        # TWS DISCONNECTED
        # ==================================

        if not connected:

            message = (
                f"TWS disconnected: {error}"
            )

            alerts.append(message)

            await create_alert(
                "TWS_DISCONNECTED",
                "CRITICAL",
                message,
            )

        # ==================================
        # STALE HEARTBEAT
        # ==================================

        if last_sync_at:

            age = (
                now_utc() - last_sync_at
            ).total_seconds()

            if age > 60:

                message = (
                    f"TWS heartbeat stale "
                    f"({int(age)} sec)"
                )

                alerts.append(message)

                await create_alert(
                    "STALE_HEARTBEAT",
                    "HIGH",
                    message,
                )

        # ==================================
        # POSITIONS STALE
        # ==================================

        cursor = await db.execute(
            """
            SELECT
                symbol,
                updated_at
            FROM tws_positions
            """
        )

        rows = await cursor.fetchall()

        for row in rows:

            symbol = row["symbol"]

            updated_at = parse_dt(
                row["updated_at"]
            )

            if not updated_at:
                continue

            age = (
                now_utc() - updated_at
            ).total_seconds()

            if age > 120:

                message = (
                    f"Position stale: "
                    f"{symbol} "
                    f"({int(age)} sec)"
                )

                alerts.append(message)

                await create_alert(
                    "STALE_POSITION",
                    "MEDIUM",
                    message,
                )

    return {
        "ok": len(alerts) == 0,
        "alerts_count": len(alerts),
        "alerts": alerts,
        "checked_at": now_utc().isoformat(),
    }