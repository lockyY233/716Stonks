import sqlite3
from pathlib import Path

from stockbot.config import DB_PATH


def get_connection() -> sqlite3.Connection:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db() -> None:
    with get_connection() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                joined_at TEXT NOT NULL,
                bank REAL NOT NULL,
                networth REAL NOT NULL DEFAULT 0,
                owe REAL NOT NULL DEFAULT 0,
                display_name TEXT NOT NULL DEFAULT '',
                rank TEXT NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS holdings (
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                shares INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (guild_id, user_id, symbol),
                FOREIGN KEY (guild_id, user_id)
                    REFERENCES users (guild_id, user_id)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS companies (
                guild_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                name TEXT NOT NULL,
                location TEXT NOT NULL DEFAULT '',
                industry TEXT NOT NULL DEFAULT '',
                founded_year INTEGER NOT NULL DEFAULT 2000,
                description TEXT NOT NULL DEFAULT '',
                evaluation TEXT NOT NULL DEFAULT '',
                base_price REAL NOT NULL,
                slope REAL NOT NULL,
                drift REAL NOT NULL,
                liquidity REAL NOT NULL DEFAULT 100.0,
                impact_power REAL NOT NULL DEFAULT 1.0,
                pending_buy REAL NOT NULL DEFAULT 0.0,
                pending_sell REAL NOT NULL DEFAULT 0.0,
                starting_tick INTEGER NOT NULL,
                current_price REAL NOT NULL DEFAULT 0,
                last_tick INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (guild_id, symbol)
            );

            CREATE TABLE IF NOT EXISTS price_history (
                guild_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                tick_index INTEGER NOT NULL,
                ts TEXT NOT NULL,
                price REAL NOT NULL,
                PRIMARY KEY (guild_id, symbol, tick_index)
            );

            CREATE TABLE IF NOT EXISTS app_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS daily_close (
                guild_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                date TEXT NOT NULL,
                close_price REAL NOT NULL,
                PRIMARY KEY (guild_id, symbol, date)
            );

            CREATE TABLE IF NOT EXISTS commodities (
                guild_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                price REAL NOT NULL,
                rarity TEXT NOT NULL DEFAULT 'common',
                spawn_weight_override REAL NOT NULL DEFAULT 0,
                image_url TEXT NOT NULL DEFAULT '',
                description TEXT NOT NULL DEFAULT '',
                perk_name TEXT NOT NULL DEFAULT '',
                perk_description TEXT NOT NULL DEFAULT '',
                perk_min_qty INTEGER NOT NULL DEFAULT 1,
                perk_effects_json TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (guild_id, name)
            );

            CREATE TABLE IF NOT EXISTS commodity_tags (
                guild_id INTEGER NOT NULL,
                commodity_name TEXT NOT NULL,
                tag TEXT NOT NULL,
                PRIMARY KEY (guild_id, commodity_name, tag),
                FOREIGN KEY (guild_id, commodity_name)
                    REFERENCES commodities (guild_id, name)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS user_commodities (
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                commodity_name TEXT NOT NULL,
                quantity INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (guild_id, user_id, commodity_name),
                FOREIGN KEY (guild_id, user_id)
                    REFERENCES users (guild_id, user_id)
                    ON DELETE CASCADE,
                FOREIGN KEY (guild_id, commodity_name)
                    REFERENCES commodities (guild_id, name)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS action_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                action_type TEXT NOT NULL,
                target_type TEXT NOT NULL,
                target_symbol TEXT NOT NULL DEFAULT '',
                quantity REAL NOT NULL DEFAULT 0,
                unit_price REAL NOT NULL DEFAULT 0,
                total_amount REAL NOT NULL DEFAULT 0,
                details TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS bank_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                request_type TEXT NOT NULL,
                amount REAL NOT NULL DEFAULT 0,
                reason TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending',
                decision_reason TEXT NOT NULL DEFAULT '',
                reviewed_by INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                reviewed_at TEXT NOT NULL DEFAULT '',
                processed_at TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                message TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS close_news (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                body TEXT NOT NULL DEFAULT '',
                image_url TEXT NOT NULL DEFAULT '',
                sort_order INTEGER NOT NULL DEFAULT 0,
                enabled INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS perks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                enabled INTEGER NOT NULL DEFAULT 1,
                priority INTEGER NOT NULL DEFAULT 100,
                stack_mode TEXT NOT NULL DEFAULT 'add',
                max_stacks INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS perk_requirements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                perk_id INTEGER NOT NULL,
                group_id INTEGER NOT NULL DEFAULT 1,
                req_type TEXT NOT NULL DEFAULT 'commodity_qty',
                commodity_name TEXT NOT NULL DEFAULT '',
                operator TEXT NOT NULL DEFAULT '>=',
                value INTEGER NOT NULL DEFAULT 1,
                FOREIGN KEY (perk_id)
                    REFERENCES perks (id)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS perk_effects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                perk_id INTEGER NOT NULL,
                effect_type TEXT NOT NULL DEFAULT 'stat_mod',
                target_stat TEXT NOT NULL DEFAULT 'income',
                value_mode TEXT NOT NULL DEFAULT 'flat',
                value REAL NOT NULL DEFAULT 0,
                scale_source TEXT NOT NULL DEFAULT 'none',
                scale_key TEXT NOT NULL DEFAULT '',
                scale_factor REAL NOT NULL DEFAULT 0,
                cap REAL NOT NULL DEFAULT 0,
                FOREIGN KEY (perk_id)
                    REFERENCES perks (id)
                    ON DELETE CASCADE
            );
            """
        )
        _ensure_users_bank_type(conn)
        _ensure_rank_column(conn)
        _ensure_display_name_column(conn)
        _ensure_networth_column(conn)
        _ensure_owe_column(conn)
        _ensure_company_columns(conn)
        _ensure_price_history_columns(conn)
        _ensure_commodities_tables(conn)
        _ensure_bank_requests_table(conn)
        _ensure_feedback_table(conn)
        _ensure_close_news_table(conn)
        _ensure_perks_tables(conn)


def _ensure_rank_column(conn: sqlite3.Connection) -> None:
    columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(users);").fetchall()
    }
    if "rank" not in columns:
        conn.execute(
            "ALTER TABLE users ADD COLUMN rank TEXT NOT NULL DEFAULT 'PRIVATE';"
        )


def _ensure_display_name_column(conn: sqlite3.Connection) -> None:
    columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(users);").fetchall()
    }
    if "display_name" not in columns:
        conn.execute(
            "ALTER TABLE users ADD COLUMN display_name TEXT NOT NULL DEFAULT '';"
        )


def _ensure_users_bank_type(conn: sqlite3.Connection) -> None:
    columns = {
        row["name"]: row["type"]
        for row in conn.execute("PRAGMA table_info(users);").fetchall()
    }
    if not columns:
        return
    if columns.get("bank", "").upper() == "REAL":
        return

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS users_new (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            joined_at TEXT NOT NULL,
            bank REAL NOT NULL,
            networth REAL NOT NULL DEFAULT 0,
            owe REAL NOT NULL DEFAULT 0,
            display_name TEXT NOT NULL DEFAULT '',
            rank TEXT NOT NULL,
            PRIMARY KEY (guild_id, user_id)
        );

        INSERT INTO users_new (
            guild_id,
            user_id,
            joined_at,
            bank,
            networth,
            owe,
            display_name,
            rank
        )
        SELECT
            guild_id,
            user_id,
            joined_at,
            bank,
            0 AS networth,
            0 AS owe,
            '' AS display_name,
            rank
        FROM users;

        DROP TABLE users;
        ALTER TABLE users_new RENAME TO users;
        """
    )


def _ensure_networth_column(conn: sqlite3.Connection) -> None:
    columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(users);").fetchall()
    }
    if "networth" not in columns:
        conn.execute(
            "ALTER TABLE users ADD COLUMN networth REAL NOT NULL DEFAULT 0;"
        )


def _ensure_owe_column(conn: sqlite3.Connection) -> None:
    columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(users);").fetchall()
    }
    if "owe" not in columns:
        conn.execute(
            "ALTER TABLE users ADD COLUMN owe REAL NOT NULL DEFAULT 0;"
        )


def _ensure_commodities_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS commodities (
            guild_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            price REAL NOT NULL,
            rarity TEXT NOT NULL DEFAULT 'common',
            spawn_weight_override REAL NOT NULL DEFAULT 0,
            image_url TEXT NOT NULL DEFAULT '',
            description TEXT NOT NULL DEFAULT '',
            perk_name TEXT NOT NULL DEFAULT '',
            perk_description TEXT NOT NULL DEFAULT '',
            perk_min_qty INTEGER NOT NULL DEFAULT 1,
            perk_effects_json TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (guild_id, name)
        );

        CREATE TABLE IF NOT EXISTS commodity_tags (
            guild_id INTEGER NOT NULL,
            commodity_name TEXT NOT NULL,
            tag TEXT NOT NULL,
            PRIMARY KEY (guild_id, commodity_name, tag),
            FOREIGN KEY (guild_id, commodity_name)
                REFERENCES commodities (guild_id, name)
                ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS user_commodities (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            commodity_name TEXT NOT NULL,
            quantity INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, user_id, commodity_name),
            FOREIGN KEY (guild_id, user_id)
                REFERENCES users (guild_id, user_id)
                ON DELETE CASCADE,
            FOREIGN KEY (guild_id, commodity_name)
                REFERENCES commodities (guild_id, name)
                ON DELETE CASCADE
        );
        """
    )
    columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(commodities);").fetchall()
    }
    if "rarity" not in columns:
        conn.execute(
            "ALTER TABLE commodities ADD COLUMN rarity TEXT NOT NULL DEFAULT 'common';"
        )
    if "spawn_weight_override" not in columns:
        conn.execute(
            "ALTER TABLE commodities ADD COLUMN spawn_weight_override REAL NOT NULL DEFAULT 0;"
        )
    if "perk_name" not in columns:
        conn.execute(
            "ALTER TABLE commodities ADD COLUMN perk_name TEXT NOT NULL DEFAULT '';"
        )
    if "perk_description" not in columns:
        conn.execute(
            "ALTER TABLE commodities ADD COLUMN perk_description TEXT NOT NULL DEFAULT '';"
        )
    if "perk_min_qty" not in columns:
        conn.execute(
            "ALTER TABLE commodities ADD COLUMN perk_min_qty INTEGER NOT NULL DEFAULT 1;"
        )
    if "perk_effects_json" not in columns:
        conn.execute(
            "ALTER TABLE commodities ADD COLUMN perk_effects_json TEXT NOT NULL DEFAULT '';"
        )


def _ensure_bank_requests_table(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS bank_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            request_type TEXT NOT NULL,
            amount REAL NOT NULL DEFAULT 0,
            reason TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'pending',
            decision_reason TEXT NOT NULL DEFAULT '',
            reviewed_by INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            reviewed_at TEXT NOT NULL DEFAULT '',
            processed_at TEXT NOT NULL DEFAULT ''
        );
        """
    )


def _ensure_feedback_table(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS feedback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            message TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        """
    )


def _ensure_close_news_table(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS close_news (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            body TEXT NOT NULL DEFAULT '',
            image_url TEXT NOT NULL DEFAULT '',
            sort_order INTEGER NOT NULL DEFAULT 0,
            enabled INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT NOT NULL
        );
        """
    )


def _ensure_perks_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS perks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            enabled INTEGER NOT NULL DEFAULT 1,
            priority INTEGER NOT NULL DEFAULT 100,
            stack_mode TEXT NOT NULL DEFAULT 'add',
            max_stacks INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS perk_requirements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            perk_id INTEGER NOT NULL,
            group_id INTEGER NOT NULL DEFAULT 1,
            req_type TEXT NOT NULL DEFAULT 'commodity_qty',
            commodity_name TEXT NOT NULL DEFAULT '',
            operator TEXT NOT NULL DEFAULT '>=',
            value INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY (perk_id)
                REFERENCES perks (id)
                ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS perk_effects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            perk_id INTEGER NOT NULL,
            effect_type TEXT NOT NULL DEFAULT 'stat_mod',
            target_stat TEXT NOT NULL DEFAULT 'income',
            value_mode TEXT NOT NULL DEFAULT 'flat',
            value REAL NOT NULL DEFAULT 0,
            scale_source TEXT NOT NULL DEFAULT 'none',
            scale_key TEXT NOT NULL DEFAULT '',
            scale_factor REAL NOT NULL DEFAULT 0,
            cap REAL NOT NULL DEFAULT 0,
            FOREIGN KEY (perk_id)
                REFERENCES perks (id)
                ON DELETE CASCADE
        );
        """
    )

    perks_cols = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(perks);").fetchall()
    }
    if "income_multiplier" in perks_cols:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS perks_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                enabled INTEGER NOT NULL DEFAULT 1,
                priority INTEGER NOT NULL DEFAULT 100,
                stack_mode TEXT NOT NULL DEFAULT 'add',
                max_stacks INTEGER NOT NULL DEFAULT 1
            );

            INSERT INTO perks_new (id, guild_id, name, description, enabled, priority, stack_mode, max_stacks)
            SELECT id, guild_id, name, description, enabled, 100, 'add', 1
            FROM perks;

            DROP TABLE perks;
            ALTER TABLE perks_new RENAME TO perks;
            """
        )
    else:
        if "priority" not in perks_cols:
            conn.execute(
                "ALTER TABLE perks ADD COLUMN priority INTEGER NOT NULL DEFAULT 100;"
            )
        if "stack_mode" not in perks_cols:
            conn.execute(
                "ALTER TABLE perks ADD COLUMN stack_mode TEXT NOT NULL DEFAULT 'add';"
            )
        if "max_stacks" not in perks_cols:
            conn.execute(
                "ALTER TABLE perks ADD COLUMN max_stacks INTEGER NOT NULL DEFAULT 1;"
            )

    req_cols = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(perk_requirements);").fetchall()
    }
    if req_cols and "required_qty" in req_cols:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS perk_requirements_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                perk_id INTEGER NOT NULL,
                group_id INTEGER NOT NULL DEFAULT 1,
                req_type TEXT NOT NULL DEFAULT 'commodity_qty',
                commodity_name TEXT NOT NULL DEFAULT '',
                operator TEXT NOT NULL DEFAULT '>=',
                value INTEGER NOT NULL DEFAULT 1,
                FOREIGN KEY (perk_id)
                    REFERENCES perks (id)
                    ON DELETE CASCADE
            );

            INSERT INTO perk_requirements_new (id, perk_id, group_id, req_type, commodity_name, operator, value)
            SELECT id, perk_id, 1, 'commodity_qty', commodity_name, '>=', required_qty
            FROM perk_requirements;

            DROP TABLE perk_requirements;
            ALTER TABLE perk_requirements_new RENAME TO perk_requirements;
            """
        )


def _ensure_company_columns(conn: sqlite3.Connection) -> None:
    columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(companies);").fetchall()
    }
    if not columns:
        return
    if "guild_id" not in columns:
        _migrate_companies_table(conn)
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(companies);").fetchall()
        }
    if "offset" in columns:
        _migrate_companies_remove_offset(conn)
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(companies);").fetchall()
        }
    if "starting_tick" not in columns:
        conn.execute(
            "ALTER TABLE companies ADD COLUMN starting_tick INTEGER NOT NULL DEFAULT 0;"
        )
    if "current_price" not in columns:
        conn.execute(
            "ALTER TABLE companies ADD COLUMN current_price REAL NOT NULL DEFAULT 0;"
        )
    if "last_tick" not in columns:
        conn.execute(
            "ALTER TABLE companies ADD COLUMN last_tick INTEGER NOT NULL DEFAULT 0;"
        )
    if "liquidity" not in columns:
        conn.execute(
            "ALTER TABLE companies ADD COLUMN liquidity REAL NOT NULL DEFAULT 100.0;"
        )
    if "location" not in columns:
        conn.execute(
            "ALTER TABLE companies ADD COLUMN location TEXT NOT NULL DEFAULT '';"
        )
    if "industry" not in columns:
        conn.execute(
            "ALTER TABLE companies ADD COLUMN industry TEXT NOT NULL DEFAULT '';"
        )
    if "founded_year" not in columns:
        conn.execute(
            "ALTER TABLE companies ADD COLUMN founded_year INTEGER NOT NULL DEFAULT 2000;"
        )
    if "description" not in columns:
        conn.execute(
            "ALTER TABLE companies ADD COLUMN description TEXT NOT NULL DEFAULT '';"
        )
    if "evaluation" not in columns:
        conn.execute(
            "ALTER TABLE companies ADD COLUMN evaluation TEXT NOT NULL DEFAULT '';"
        )
    if "impact_power" not in columns:
        conn.execute(
            "ALTER TABLE companies ADD COLUMN impact_power REAL NOT NULL DEFAULT 1.0;"
        )
    if "pending_buy" not in columns:
        conn.execute(
            "ALTER TABLE companies ADD COLUMN pending_buy REAL NOT NULL DEFAULT 0.0;"
        )
    if "pending_sell" not in columns:
        conn.execute(
            "ALTER TABLE companies ADD COLUMN pending_sell REAL NOT NULL DEFAULT 0.0;"
        )


def _ensure_price_history_columns(conn: sqlite3.Connection) -> None:
    columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(price_history);").fetchall()
    }
    if not columns:
        return
    if "tick_index" not in columns:
        _migrate_price_history_table(conn)
        return
    if "guild_id" not in columns:
        conn.execute(
            "ALTER TABLE price_history ADD COLUMN guild_id INTEGER NOT NULL DEFAULT 0;"
        )


def _migrate_companies_table(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS companies_new (
            guild_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            name TEXT NOT NULL,
            location TEXT NOT NULL DEFAULT '',
            industry TEXT NOT NULL DEFAULT '',
            founded_year INTEGER NOT NULL DEFAULT 2000,
            description TEXT NOT NULL DEFAULT '',
            evaluation TEXT NOT NULL DEFAULT '',
            base_price REAL NOT NULL,
            slope REAL NOT NULL,
            drift REAL NOT NULL,
            liquidity REAL NOT NULL DEFAULT 100.0,
            impact_power REAL NOT NULL DEFAULT 1.0,
            pending_buy REAL NOT NULL DEFAULT 0.0,
            pending_sell REAL NOT NULL DEFAULT 0.0,
            starting_tick INTEGER NOT NULL DEFAULT 0,
            current_price REAL NOT NULL DEFAULT 0,
            last_tick INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (guild_id, symbol)
        );

        INSERT INTO companies_new (
            guild_id,
            symbol,
            name,
            location,
            industry,
            founded_year,
            description,
            evaluation,
            base_price,
            slope,
            drift,
            liquidity,
            impact_power,
            pending_buy,
            pending_sell,
            starting_tick,
            current_price,
            last_tick,
            updated_at
        )
        SELECT
            0 AS guild_id,
            symbol,
            name,
            '' AS location,
            '' AS industry,
            2000 AS founded_year,
            '' AS description,
            '' AS evaluation,
            base_price,
            slope,
            drift,
            100.0 AS liquidity,
            1.0 AS impact_power,
            0.0 AS pending_buy,
            0.0 AS pending_sell,
            0 AS starting_tick,
            base_price AS current_price,
            0 AS last_tick,
            updated_at
        FROM companies;

        DROP TABLE companies;
        ALTER TABLE companies_new RENAME TO companies;
        """
    )


def _migrate_companies_remove_offset(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS companies_new (
            guild_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            name TEXT NOT NULL,
            location TEXT NOT NULL DEFAULT '',
            industry TEXT NOT NULL DEFAULT '',
            founded_year INTEGER NOT NULL DEFAULT 2000,
            description TEXT NOT NULL DEFAULT '',
            evaluation TEXT NOT NULL DEFAULT '',
            base_price REAL NOT NULL,
            slope REAL NOT NULL,
            drift REAL NOT NULL,
            liquidity REAL NOT NULL DEFAULT 100.0,
            impact_power REAL NOT NULL DEFAULT 1.0,
            pending_buy REAL NOT NULL DEFAULT 0.0,
            pending_sell REAL NOT NULL DEFAULT 0.0,
            starting_tick INTEGER NOT NULL,
            current_price REAL NOT NULL DEFAULT 0,
            last_tick INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (guild_id, symbol)
        );

        INSERT INTO companies_new (
            guild_id,
            symbol,
            name,
            location,
            industry,
            founded_year,
            description,
            evaluation,
            base_price,
            slope,
            drift,
            liquidity,
            impact_power,
            pending_buy,
            pending_sell,
            starting_tick,
            current_price,
            last_tick,
            updated_at
        )
        SELECT
            guild_id,
            symbol,
            name,
            '' AS location,
            '' AS industry,
            2000 AS founded_year,
            '' AS description,
            '' AS evaluation,
            base_price,
            slope,
            drift,
            100.0 AS liquidity,
            1.0 AS impact_power,
            0.0 AS pending_buy,
            0.0 AS pending_sell,
            starting_tick,
            current_price,
            last_tick,
            updated_at
        FROM companies;

        DROP TABLE companies;
        ALTER TABLE companies_new RENAME TO companies;
        """
    )


def _migrate_price_history_table(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS price_history_new (
            guild_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            tick_index INTEGER NOT NULL,
            ts TEXT NOT NULL,
            price REAL NOT NULL,
            PRIMARY KEY (guild_id, symbol, tick_index)
        );

        INSERT INTO price_history_new (
            guild_id,
            symbol,
            tick_index,
            ts,
            price
        )
        SELECT
            COALESCE(guild_id, 0) AS guild_id,
            symbol,
            CAST(strftime('%s', ts) AS INTEGER) AS tick_index,
            ts,
            price
        FROM price_history;

        DROP TABLE price_history;
        ALTER TABLE price_history_new RENAME TO price_history;
        """
    )
