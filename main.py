from fastapi import FastAPI
import pandas as pd
from pandas.io.parsers.readers import read_csv
from tabulate import tabulate

app = FastAPI(title= 'STEAM',
              description= 'El objetivo de esta API es mostrar los resultados para las siguientes funciones a partir de la bases de datos de STEAM')

df_games = pd.read_parquet('games.parquet')
df_items = pd.read_parquet('items.parquet')
df_reviews = pd.read_parquet('reviews.parquet')
df_generos = pd.read_parquet('generos.parquet')

@app.get('/userdata')
def userdata(user_id: str):
  """
  Devuelve la cantidad de dinero gastado por el usuario, el porcentaje de recomendación en 
  base a las recomendaciones hechas por el usuario y la cantidad de items.
  """
  if user_id in df_games['id'].values or user_id in df_items['user_id'].values or user_id in df_reviews['user_id'].values:

    # Gasto de usuario
    gasto = 0
    compras_usuario = pd.merge(df_games, df_items, left_on='id', right_on='item_id')
    usuario = compras_usuario[compras_usuario['user_id'] == user_id]['price']
    for i in usuario:
      gasto = gasto + i
    gasto = round(gasto, 2)
    gasto_usuario = f'El usuario {user_id} en total gasto ${gasto}'

    # Porcentaje de recomendacion
    recomendacion_usuario = df_reviews[df_reviews['user_id'] == user_id]['recommend']
    porcentaje = (recomendacion_usuario.sum()/len(recomendacion_usuario))*100
    porcentaje_usuario = f'El porcentaje de recomensacion de usuario {user_id} es del {porcentaje}%'

    # Cantidad de items
    cantidad_items = df_items[df_items['user_id'] == user_id]['items_count'].iloc[0]
    cantidad = f'La cantidad de items del usuario {user_id} es {cantidad_items}'

    return gasto_usuario, porcentaje_usuario, cantidad

  else:
    no_encontrado = f'El usuario {user_id} no fue encontrado en la base de datos'
    return no_encontrado

@app.get('/countreviews')
def countreviews(fecha1: str, fecha2: str):
  """
  Cantidad de usuarios que realizaron reviews entre las fechas dadas y el porcentaje de recomendación
  de los mismos en base a las recomendaciones
  """
  # Cantidad de usuarios que realizaron reviews entre fecha1 y fecha2
  usuarios = df_reviews[(df_reviews['posted'] >= fecha1) & (df_reviews['posted'] <= fecha2)]['user_id']
  cantidad_usuarios = len(set(usuarios))
  cantidad = f'La cantidad de usuarios que realizaron reviews entre {fecha1} y {fecha2} son {cantidad_usuarios}'

  # Porcentaje de recomendacion
  recomendacion = df_reviews[(df_reviews['posted'] >= fecha1) & (df_reviews['posted'] <= fecha2)]['recommend']
  porcentaje = (recomendacion.sum()/len(recomendacion))*100
  porcentaje = round(porcentaje, 2)
  porcentaje_usuario = f'El porcentaje de recomensacion de los {cantidad_usuarios} es del {porcentaje}%'

  return cantidad, porcentaje_usuario

@app.get('/generos')
def genero(genero: str):
  """
  Devuelve el puesto en el que se encuentra un género en el ranking de los mismos, analizado 
  a partir de la columna PlayTimeForever.
  """
  # Combinar DataFrames
  generos = pd.merge(df_generos, df_items, left_on='id', right_on='item_id', how='inner')

  # Filtrar por género
  genero_data = generos[generos['genres'] == genero]

  if genero_data.empty:
      return f'El género {genero} no fue encontrado en la base de datos'

    # Calcular el ranking del género
  tiempo_juego = genero_data.groupby('genres')['playtime_forever'].sum()
  tiempo_juego = tiempo_juego.reset_index()
  tiempo_juego = tiempo_juego.sort_values(by='playtime_forever', ascending=False)
  tiempo_juego['ranking'] = tiempo_juego.index + 1

  lugar_ranking = tiempo_juego.loc[tiempo_juego['genres'] == genero, 'ranking'].values[0]
  ranking = f'El género {genero} está ubicado en el puesto {lugar_ranking} del ranking'
    
  return ranking
  
@app.get('/userforgenre')
def userforgenre(genero: str):
  """
  Retorna el top 5 de usuarios con más horas de juego en el género dado, con su URL (del user) y user_id.
  """
  # Combinar DataFrames
  generos_usuario = pd.merge(df_generos, df_items, left_on='id', right_on='item_id', how='inner')

  # Filtrar por género
  generos_usuario = generos_usuario[generos_usuario['genres'] == genero]

  if generos_usuario.empty:
      return f'El género {genero} no fue encontrado en la base de datos'

  # Calcular las 5 mejores tuplas
  generos_usuario['playtime_forever'] = generos_usuario['playtime_forever'] / 60
  top_usuarios = generos_usuario.nlargest(5, 'playtime_forever')[['user_id', 'user_url']]
  top_usuarios['rank'] = top_usuarios.reset_index().index + 1

  tuplas = top_usuarios[['rank', 'user_id', 'user_url']].to_records(index=False).tolist()

  return tuplas
  
@app.get('/desarrollador')
def desarrollador(desarrollador: str):
  """
  Rentorna la cantidad de items y porcentaje de contenido Free por año según la empresa desarrolladora dada.
  """
  # Filtrar juegos del desarrollador
  games_items = pd.merge(df_games, df_items, left_on='id', right_on='item_id')
  desarrollador_data = games_items[games_items['developer'] == desarrollador]

  if desarrollador_data.empty:
      return f'El desarrollador {desarrollador} no fue encontrado en la base de datos'

  # Cantidad de items
  cantidad_items_desarrollador = len(desarrollador_data)

  # Porcentaje de contenido gratuito por empresa desarrolladora y año
  contenido = desarrollador_data[['id', 'developer', 'price', 'release_date']]
  contenido = contenido.dropna(subset=['release_date'])
    
  # Convertir release_date a cadena y manejar NaN
  contenido['release_date'] = contenido['release_date'].astype(str)
    
  contenido['year'] = contenido['release_date'].str.split('-').str[0].astype(int)
  contenido_gratis = contenido[contenido['price'] == 0]
    
  desarrollador_year_gratis = contenido_gratis.groupby(['developer', 'year'])[['id']].nunique().reset_index()
  desarrollador_year = contenido.groupby(['developer', 'year'])[['id']].nunique().reset_index()
    
  resultado = pd.merge(desarrollador_year_gratis, desarrollador_year, on=['developer', 'year'], how='inner')
  resultado['porcentaje'] = (resultado['id_x'] / resultado['id_y'] * 100).astype(int)
    
  # Formatear resultados
  cantidad = f'La cantidad de items del desarrollador {desarrollador} es {cantidad_items_desarrollador}'
  desarrollador_porcentaje = tabulate(resultado, headers='keys', tablefmt='psql')
    
  return cantidad, desarrollador_porcentaje
    
  
@app.get('/sentiment_analysis')
def sentiment_analysis(year: int):
  """
  Según el año de lanzamiento, se devuelve una lista con la cantidad de registros de reseñas de usuarios
  que se encuentren categorizados con un análisis de sentimiento.
  """
  games_reviews = pd.merge(df_games, df_reviews, left_on='id', right_on='item_id')
  sentimiento = games_reviews[['release_date', 'sentiment']]
  sentimiento = sentimiento.dropna(subset=['release_date'])  # Eliminar filas con release_date nulos
    
  # Convertir release_date a cadena y manejar NaN
  sentimiento['release_date'] = sentimiento['release_date'].astype(str)
    
  sentimiento['year'] = sentimiento['release_date'].str.split('-').str[0].astype(int)
    
  if year in sentimiento['year'].values:
      filtro_year = sentimiento[sentimiento['year'] == year]
      sentiment_mapping = {2: 'positivo', 1: 'neutro', 0: 'negativo'}
      filtro_year['sentiment'] = filtro_year['sentiment'].map(sentiment_mapping)
      sentiment_counts = filtro_year['sentiment'].value_counts().to_dict()
        
      return sentiment_counts
    
  else:
      no_encontrado = f'El año {year} no fue encontrado en la base de datos'
      return no_encontrado
  
  
  # http://127.0.0.1:8000
