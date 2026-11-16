-- Stage 2: core actions over entities. Tables of modules live in their own migrations
-- (autocash/modules/<name>/migrations), applied when the module is connected.

-- set_state with a timeout (FT-8.4): when state_until passes, the state becomes timeout_state.
ALTER TABLE entities ADD COLUMN state_until timestamptz, ADD COLUMN timeout_state text;
CREATE INDEX entities_state_until ON entities (state_until) WHERE state_until IS NOT NULL;
CREATE INDEX entity_links_from ON entity_links (from_type, from_key);
