-- Dimension: the three kinds of editor (grain: editor_type). Assigned by the stream processor.

select *
from (values
    ('bot',        'Bot account (self-declared bot flag)',             false),
    ('registered', 'Registered human account',                         true),
    ('anonymous',  'Logged-out human (IP address or temporary account)', true)
) as t(editor_type, description, is_human)
