from flask import Flask, render_template
from pyspark.sql import SparkSession

# Créer une session Spark
spark = SparkSession.builder.appName("MovieRatings").getOrCreate()

app = Flask(__name__)

# Chemin vers le fichier u.data
data_path = 'u.data'

# Charger les données dans un RDD
lines = spark.sparkContext.textFile(data_path)


# Fonction pour extraire user_id, rating
def parseLine(line):
    columns = line.split('\t')
    user_id = columns[0]
    rating = columns[2]
    return user_id, rating


# Mapper pour générer des tuples (user_id, 1)
user_ratings = lines.map(parseLine).map(lambda x: (x[0], 1))

# Réduire par clé pour compter le nombre d'évaluations par utilisateur
user_ratings_count = user_ratings.reduceByKey(lambda x, y: x + y)

# Trier par user_id
sorted_user_ratings_count = user_ratings_count.sortByKey(ascending=True)

# Collecter les résultats
results = sorted_user_ratings_count.collect()

# Convertir les résultats en un format adapté à l'affichage dans un template HTML
formatted_results = [{"user_id": result[0], "rating_count": result[1], "high_rating_count": 0} for result in results]

# Fonction pour extraire movie_id, rating
def parseLineMovie(line):
    columns = line.split('\t')
    movie_id = columns[1]
    rating = int(columns[2])
    return movie_id, rating

# Mapper pour générer des tuples (movie_id, 1) pour les évaluations supérieures à 4
high_ratings = lines.map(parseLineMovie).filter(lambda x: x[1] > 4).map(lambda x: (x[0], 1))

# Réduire par clé pour compter le nombre d'évaluations supérieures à 4 par film
high_ratings_count = high_ratings.reduceByKey(lambda x, y: x + y)

# Collecter les résultats des évaluations supérieures à 4
high_results = high_ratings_count.collect()

# Convertir les résultats en un format adapté à l'affichage dans un template HTML
formatted_results = [
    {"user_id": result[0], "rating_count": result[1], "high_rating_count": 0}
    for result in results
]

# Mettre à jour le nombre d'évaluations supérieures à 4 dans formatted_results
for result in high_results:
    user_id = result[0]
    high_rating_count = result[1]

    # Rechercher l'entrée correspondante dans formatted_results et mettre à jour le nombre d'évaluations supérieures à 4
    for entry in formatted_results:
        if entry["user_id"] == user_id:
            entry["high_rating_count"] = high_rating_count
            break

# Calculer le nombre d'user_id ayant laissé plus de 4 évaluations
total_high_ratings_users = sum(1 for entry in formatted_results if entry["high_rating_count"] > 4)

@app.route('/')
def index():
    return render_template('index.html', results=formatted_results, total_high_ratings_users=total_high_ratings_users)

if __name__ == '__main__':
    app.run(debug=True)

