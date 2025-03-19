# MATCH (n) DETACH DELETE n

# %%
import os
from dotenv import load_dotenv
from langchain_neo4j import Neo4jGraph

# Load environment variables from .env file
load_dotenv()

# Now the credentials will be loaded from the .env file
graph = Neo4jGraph()

# ---- STEP 0: DROP EXISTING INDEXES AND CONSTRAINTS ----
print("Dropping existing indexes and constraints...")

# Drop constraints individually
graph.query("DROP CONSTRAINT unique_user_id IF EXISTS")
graph.query("DROP CONSTRAINT unique_movie_id IF EXISTS")
graph.query("DROP CONSTRAINT unique_person_name IF EXISTS")
graph.query("DROP CONSTRAINT unique_tag_name IF EXISTS")
graph.query("DROP CONSTRAINT unique_genre_name IF EXISTS")

# Drop indexes individually
graph.query("DROP INDEX movie_id_index IF EXISTS")
graph.query("DROP INDEX user_id_index IF EXISTS") 
graph.query("DROP INDEX person_name_index IF EXISTS")
graph.query("DROP INDEX tag_name_index IF EXISTS")
graph.query("DROP INDEX genre_name_index IF EXISTS")

# ---- STEP 1: CREATE INDEXES AND CONSTRAINTS ----
print("Creating indexes and constraints...")
# Create constraint first (which automatically creates index)
graph.query("CREATE CONSTRAINT unique_user_id IF NOT EXISTS FOR (u:User) REQUIRE u.userId IS UNIQUE")
# Then create other indexes
graph.query("CREATE INDEX movie_id_index IF NOT EXISTS FOR (m:Movie) ON (m.id)")
graph.query("CREATE INDEX person_name_index IF NOT EXISTS FOR (p:Person) ON (p.name)")
graph.query("CREATE INDEX tag_name_index IF NOT EXISTS FOR (t:Tag) ON (t.name)")

# ---- STEP 2: LOAD MOVIES DATA ----
print("Loading movies data...")
movies_query = """
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/tomasonjo/blog-datasets/main/movies/movies_small.csv' AS row
MERGE (m: Movie {id: row.movieId})
SET m.released = date(row.released),
    m.title = row.title,
    m.tagline = row.tagline,
    m.imdbRating = toFloat(row.imdbRating),
    m.releaseYear = date(row.released).year
FOREACH (director in split(row.director, '|') | MERGE (p:Person {name: trim(director)}) MERGE (p)-[:DIRECTED]->(m))
FOREACH (actor in split(row.actors, '|') | MERGE (p:Person {name: trim(actor)}) MERGE (p)-[:ACTED_IN]->(m))
FOREACH (genre in split(row.genres, '|') | MERGE (g:Genre {name: trim(genre)}) MERGE (m)-[:IN_GENRE]->(g))
"""
graph.query(movies_query)

# ---- STEP 3: ADD SPECIALIZED LABELS TO PERSONS ----
print("Adding specialized labels to Person nodes...")
graph.query("""
MATCH (p:Person)-[:ACTED_IN]->()
SET p:Actor
""")

graph.query("""
MATCH (p:Person)-[:DIRECTED]->() 
SET p:Director
""")

# ---- STEP 4: LOAD RATINGS DATA ----
print("Loading ratings data...")
ratings_query = """
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/provezano/movielens/refs/heads/main/ratings.csv' AS row
CALL {
    WITH row
    MERGE (u:User {userId: row.userId})
    WITH u, row
    MATCH (m:Movie {id: row.movieId})
    WHERE m IS NOT NULL
    CREATE (u)-[r:RATED]->(m)
    SET r.rating = toFloat(row.rating),
        r.timestamp = datetime({epochSeconds: toInteger(row.timestamp)})
} IN TRANSACTIONS OF 1000 ROWS
"""
graph.query(ratings_query)

# ---- STEP 5: LOAD TAGS AND CONVERT TO PROPER NODES ----
print("Loading and normalizing tags...")
# First load tags as relationships
tags_query = """
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/provezano/movielens/refs/heads/main/tags.csv' AS row
CALL {
    WITH row
    MERGE (u:User {userId: row.userId})
    WITH u, row
    MATCH (m:Movie {id: row.movieId})
    WHERE m IS NOT NULL
    CREATE (u)-[t:TAGGED]->(m)
    SET t.tag = row.tag,
        t.timestamp = datetime({epochSeconds: toInteger(row.timestamp)})
} IN TRANSACTIONS OF 1000 ROWS
"""
graph.query(tags_query)

# Then convert tags to proper nodes
tag_normalization_query = """
MATCH (u:User)-[t:TAGGED]->(m:Movie)
WITH m, t.tag AS rawTag, count(*) AS tagFrequency, t.timestamp as tagTimestamp, u
MERGE (tag:Tag {name: trim(toLower(rawTag))})
MERGE (m)-[rel:HAS_TAG]->(tag)
SET rel.weight = tagFrequency * 0.1
MERGE (u)-[ut:APPLIED_TAG]->(tag)
SET ut.timestamp = tagTimestamp
"""
graph.query(tag_normalization_query)

# Remove old TAGGED relationships
graph.query("MATCH ()-[t:TAGGED]->() DELETE t")

# ---- STEP 6: ENRICH USER CONTEXT ----
print("Enriching user context...")
user_enrichment_query = """
MATCH (u:User)-[r:RATED]->(m:Movie)
WITH u, avg(r.rating) AS avgRating, count(*) AS totalRatings
SET u.avgRating = avgRating, 
    u.totalRatings = totalRatings
"""
graph.query(user_enrichment_query)

# ---- STEP 7: CALCULATE DERIVED PROPERTIES ----
print("Calculating derived properties...")
# Calculate director average ratings
graph.query("""
MATCH (d:Director)-[:DIRECTED]->(m:Movie)
WITH d, avg(m.imdbRating) AS avgDirRating, count(m) AS movieCount
SET d.avgRating = avgDirRating,
    d.movieCount = movieCount
""")

# Calculate actor movie counts
graph.query("""
MATCH (a:Actor)-[:ACTED_IN]->(m:Movie)
WITH a, count(m) AS movieCount
SET a.movieCount = movieCount
""")

# Calculate tag usage counts
graph.query("""
MATCH (m:Movie)-[:HAS_TAG]->(t:Tag)
WITH t, count(m) AS usageCount
SET t.usageCount = usageCount
""")

# ---- STEP 8: REFRESH SCHEMA ----
print("Refreshing schema...")
graph.refresh_schema()
print(graph.schema)

# ---- STEP 9: VERIFY RECOMMENDATION QUERIES ----
print("\nTesting recommendation query...")
# Sample recommendation query
test_query = """
MATCH (u:User {userId: "1"})-[r:RATED]->(m:Movie)
WHERE r.rating > 4.0
WITH u, m
MATCH (m)-[:IN_GENRE]->(g:Genre)
WITH u, g, count(*) AS genreStrength
ORDER BY genreStrength DESC
LIMIT 3
MATCH (g)<-[:IN_GENRE]-(rec:Movie)
WHERE NOT EXISTS((u)-[:RATED]->(rec))
AND rec.imdbRating > 7.0
RETURN rec.title, rec.imdbRating, collect(g.name) AS genres
ORDER BY rec.imdbRating DESC
LIMIT 5
"""
recommendations = graph.query(test_query)
print("\nSample recommendations:")
for rec in recommendations:
    print(f"- {rec['rec.title']} ({rec['rec.imdbRating']}) - Genres: {', '.join(rec['genres'])}")

print("\nGraph optimization complete! Ready for LLM-based recommendations.")
# %%
