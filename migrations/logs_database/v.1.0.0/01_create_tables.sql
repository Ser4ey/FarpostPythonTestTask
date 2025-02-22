CREATE TABLE space_type (
    id SERIAL PRIMARY KEY UNIQUE,
    name VARCHAR(255) UNIQUE NOT NULL
);


CREATE TABLE event_type (
    id SERIAL PRIMARY KEY UNIQUE,
    name VARCHAR(255) NOT NULL,
    space_type_id INTEGER NOT NULL,
    FOREIGN KEY (space_type_id) REFERENCES space_type(id) ON DELETE RESTRICT
);


CREATE TABLE logs (
    id SERIAL PRIMARY KEY UNIQUE,
    datetime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    user_id INTEGER NOT NULL,
    event_type_id INTEGER NOT NULL,
    FOREIGN KEY (event_type_id) REFERENCES event_type(id) ON DELETE RESTRICT
);

