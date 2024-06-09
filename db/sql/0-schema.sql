
CREATE TABLE movies (
    id serial PRIMARY KEY,
    title text NOT NULL ,
    year integer
);

CREATE TABLE genres (
    id serial PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE users (
    id serial PRIMARY KEY
);

CREATE TABLE genres_in_movies (
    movie_id serial REFERENCES movies(id),
    genre_id serial REFERENCES genres(id)
);

CREATE TABLE ratings (
    user_id serial REFERENCES users(id),
    movie_id serial REFERENCES movies(id),
    rating float NOT NULL,
    rated_at timestamp NOT NULL
);

CREATE TABLE links (
    movie_id serial REFERENCES movies(id),
    imdb_id text,
    tmdb_id text
);

CREATE TABLE tags (
    id serial PRIMARY KEY,
    tag text NOT NULL
);

CREATE TABLE user_tags (
    user_id serial REFERENCES users(id),
    movie_id serial REFERENCES movies(id),
    tag_id serial REFERENCES  tags(id),
    tagged_at timestamp,
    PRIMARY KEY (user_id, movie_id, tag_id)
);

CREATE TABLE movie_tag_relevance (
    movie_id serial REFERENCES movies(id),
    tag_id serial REFERENCES  tags(id),
    relevance float
);

