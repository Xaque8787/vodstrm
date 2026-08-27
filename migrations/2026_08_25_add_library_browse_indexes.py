"""Add indexes used by Library grouping, ordering, and provider lookups."""


def up(conn) -> None:
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_entries_type_cleaned_title
        ON entries(type, cleaned_title)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_streams_provider_entry
        ON streams(provider, entry_id)
        """
    )
    conn.commit()