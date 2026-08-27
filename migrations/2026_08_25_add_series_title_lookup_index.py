"""Index case-insensitive series title lookups used by Library cards."""


def up(conn) -> None:
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_entries_type_lower_cleaned_title
        ON entries(type, lower(cleaned_title))
        """
    )
    conn.commit()