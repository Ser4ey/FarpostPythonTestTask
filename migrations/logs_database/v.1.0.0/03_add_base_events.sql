INSERT INTO event_type(name, space_type_id)
SELECT 'login', id FROM space_type WHERE name = 'global';

INSERT INTO event_type(name, space_type_id)
SELECT 'logout', id FROM space_type WHERE name = 'global';

INSERT INTO event_type(name, space_type_id)
SELECT 'create_post', id FROM space_type WHERE name = 'blog';

INSERT INTO event_type(name, space_type_id)
SELECT 'delete_post', id FROM space_type WHERE name = 'blog';

INSERT INTO event_type(name, space_type_id)
SELECT 'comment', id FROM space_type WHERE name = 'post';

