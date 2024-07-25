CREATE TABLE author (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    login VARCHAR(255) UNIQUE NOT NULL
);


CREATE TABLE blog (
    id SERIAL PRIMARY KEY,
    owner_id INTEGER NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    FOREIGN KEY (owner_id) REFERENCES author(id) ON DELETE CASCADE
);


CREATE TABLE post (
    id SERIAL PRIMARY KEY,
    header VARCHAR(255) NOT NULL,
    text TEXT,
    author_id INTEGER NOT NULL,
    blog_id INTEGER NOT NULL,
    FOREIGN KEY (author_id) REFERENCES author(id) ON DELETE CASCADE,
    FOREIGN KEY (blog_id) REFERENCES blog(id) ON DELETE CASCADE
);

