1. Movie with the highest average rating (minimum 50 ratings)
SELECT m.title, ROUND(AVG(r.rating),2) AS avg_rating, COUNT(r.rating) AS total_ratings
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.movie_id
HAVING COUNT(r.rating) > 50
ORDER BY avg_rating DESC
LIMIT 1;

2. Top 5 genre with highest average rating
SELECT g.name AS genre, ROUND(AVG(r.rating),2) AS avg_rating
FROM genres g
JOIN movie_genres mg ON g.id = mg.genre_id
JOIN ratings r ON mg.movie_id = r.movie_id
GROUP BY g.id
ORDER BY avg_rating DESC
LIMIT 5;

3. Director with the most movies
SELECT director, COUNT(*) AS movie_count
FROM movies
WHERE director IS NOT NULL AND director != ''
GROUP BY director
ORDER BY movie_count DESC
LIMIT 1;

4. Average rating by release year
SELECT year, ROUND(AVG(r.rating),2) AS avg_rating, COUNT(DISTINCT m.movie_id) AS movie_count
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
WHERE year IS NOT NULL
GROUP BY year
ORDER BY year;
