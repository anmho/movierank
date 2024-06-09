from typing import Tuple

import pandas as pd
import csv
import logging
import re

statements = []
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)



def parse_title(title):
    title = title.replace("\xa0", " ")
    title = title.strip()
    if title[-1] == '"':
        title = title[:-1]
    if title[0] == '"':
        title = title[0:]
    year_str = title[-6:]
    year_str = year_str[1:-1]
    year = None
    if year_str[-1] == '"':
        title.replace('"', "")
    try:
        year = int(year_str)
        title = title[:-6].strip()
    except ValueError as e:
        pattern = r"\d+"

        if re.search(pattern, year_str) is not None:
            print(title.split(" "))
            log.error(f"Error: %s -- title=%s year=%s", e, title, year_str)

    return title, year


# noinspection PyShadowingNames
def transform_movies() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    movies_rows = []
    genres_in_movies_rows = []
    genres_rows = []

    with open("./data/movie.csv", "r") as csv_file:
        reader = csv.DictReader(csv_file)
        i = 0

        genre_map = {}

        for row in reader:
            movieId = row.get("movieId")
            title = row.get("title")
            genres = row.get("genres")

            title, year = parse_title(title)

            genres = genres.split("|")
            if genres[0] == "(no genres listed)":
                genres = []

            movies_rows.append({"id": movieId, "title": title, "year": year})

            for genre in genres:
                if genre not in genre_map:
                    # genre_sql.write(f"INSERT INTO genres (name) VALUES ({genre});\n")
                    genre_map[genre] = len(genre_map) + 1

                    genres_rows.append({"id": genre_map[genre], "name": genre})
                genre_id = genre_map[genre]
                genres_in_movies_rows.append({"movie_id": movieId, "genre_id": genre_id})

            i += 1

    movies_df = pd.DataFrame(movies_rows).rename(columns={"id": "movie_id"})
    genres_in_movies_df = pd.DataFrame(genres_in_movies_rows)
    genres_df = pd.DataFrame(genres_rows)
    print("movies", movies_df.columns)
    print("genres_in_movies", genres_in_movies_df.columns)
    print("genres", genres_df.columns)
    return movies_df, genres_in_movies_df, genres_df


def transform_links():
    with open("./data/link.csv", "r") as f:
        reader = csv.DictReader(f)

        links_rows = []

        for row in reader:
            movie_id = row.get("movieId")
            imdb_id = row.get("imdbId")
            tmdb_id = row.get("tmdbId")

            zeroes = max(0, 7 - len(imdb_id))
            imdb_id = f"tt{'0' * zeroes}{imdb_id}"
            row = {
                "movie_id": movie_id,
                "imdb_id": imdb_id,
                "tmdb_id": tmdb_id
            }
            links_rows.append(row)

        links_df = pd.DataFrame(links_rows)
        print("links", links_df.columns)
        return links_df


def transform_ratings():
    ratings_df = pd.read_csv("./data/rating.csv")
    ratings_df = ratings_df.rename(columns={"userId": "user_id", "movieId": "movie_id"})
    print("ratings", ratings_df.columns)
    users_df = ratings_df[["user_id"]].rename(columns={"userId": "id"})
    print("users", users_df.columns)
    return ratings_df, users_df


def transform_tags():
    tags_df = pd.read_csv("./data/tag.csv")
    genome_scores_df = pd.read_csv("./data/genome_scores.csv")
    genome_tags_df = pd.read_csv("./data/genome_tags.csv")


    user_tags_df = pd.merge(tags_df, genome_tags_df, on="tag")
    user_tags_df = user_tags_df[["userId", "movieId", "tagId", "timestamp"]]
    tags_df = genome_tags_df
    movie_tag_relevance_df = genome_scores_df

    # Rename the columns for snake case
    user_tags_df = user_tags_df.rename(columns={"userId": "user_id", "movieId": "movie_id", "tagId": "tag_id"})
    tags_df = tags_df.rename(columns={"tagId": "tag_id"})
    movie_tag_relevance_df = movie_tag_relevance_df.rename(columns={"movieId": "movie_id", "tagId": "tag_id"})
    print("user_tags", user_tags_df.columns)
    print("tags", tags_df.columns)
    print("movie_tag_relevance", movie_tag_relevance_df.columns)
    return user_tags_df, tags_df, movie_tag_relevance_df


def write_movies_sql(movies_df: pd.DataFrame):
    stmts = []
    for row_id, data in movies_df.iterrows():
        id, title, year = data
        try:
            if pd.isnull(year):
                year = "NULL"
            else:
                year = int(year)
            # print(id, title, int(year))

            title = title.replace("'", "''")

            stmt = f"INSERT INTO movies (id, title, year) VALUES ({id}, '{title}', {year});\n"
            stmts.append(stmt)

        except ValueError as e:
            print(e, id, title, year)

    with open("./db/temp/movies.sql", "w") as f:
        f.writelines(stmts)


def write_genres_sql(genres_df: pd.DataFrame):
    stmts = []
    for row_id, data in genres_df.iterrows():
        id, name = data

        stmt = f"INSERT INTO genres (id, name) VALUES ({id}, '{name}');\n"
        stmts.append(stmt)

    with open("./db/temp/genres.sql", "w") as f:
        f.writelines(stmts)


def write_genres_in_movies_sql(genres_in_movies_df: pd.DataFrame):
    stmts = []
    for row_id, data in genres_in_movies_df.iterrows():
        movie_id, genre_id = data

        stmt = f"INSERT INTO genres_in_movies (movie_id, genre_id) VALUES ({movie_id}, {genre_id});\n"

        stmts.append(stmt)

    with open("./db/temp/genres_in_movies.sql", "w") as f:
        f.writelines(stmts)

def write_links_sql(links_df: pd.DataFrame):
    stmts = []
    for id, data in links_df.iterrows():
        movie_id, imdb_id, tmdb_id = data

        stmt = f"INSERT INTO links (movie_id, imdb_id, tmdb_id) VALUES ({movie_id}, '{imdb_id}', '{tmdb_id}');\n"

        stmts.append(stmt)

    with open("./db/temp/links.sql", "w") as f:
        f.writelines(stmts)


movies_df, genres_in_movies_df, genres_df = transform_movies()

write_movies_sql(movies_df)
write_genres_sql(genres_df)
write_genres_in_movies_sql(genres_in_movies_df)



links_df = transform_links()
write_links_sql(links_df)
# user_tags_df, tags_df, movie_tag_relevance_df = transform_tags()
# ratings_df, users_df = transform_ratings()
