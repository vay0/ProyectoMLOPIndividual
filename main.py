from fastapi import FastAPI
import pandas as pd
from pandas.io.parsers.readers import read_csv
from tabulate import tabulate

app = FastAPI(title= 'STEAM',
              description= 'El objetivo de esta API es mostrar los resultados para las siguientes funciones a partir de la bases de datos de STEAM')

df_games = read_csv('games.csv')
df_items = pd.read_parquet('items.parquet')
df_reviews = read_csv('reviews.csv')
df_generos = read_csv('generos.csv')

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
  generos = pd.merge(df_generos, df_items, left_on='id', right_on='item_id')

  if genero in generos['genres'].values:
    tiempo_juego = generos.groupby('genres')['playtime_forever'].sum()
    tiempo_juego = pd.DataFrame(tiempo_juego)
    tiempo_juego = tiempo_juego.sort_values(by = ['playtime_forever'], ascending=False)
    tiempo_juego['ranking'] = tiempo_juego.rank(ascending=False)
    tiempo_juego['ranking'] = tiempo_juego['ranking'].astype(int)
    lugar_ranking = tiempo_juego.loc[genero, 'ranking']
    ranking = f'El genero {genero} esta ubicado en el puesto {lugar_ranking} del ranking'
    return ranking

  else:
    no_encontrado = f'El genero {genero} no fue encontrado en la base de datos'
    return no_encontrado
  
@app.get('/userforgenre')
def userforgenre(genero: str):
  """
  Retorna el top 5 de usuarios con más horas de juego en el género dado, con su URL (del user) y user_id.
  """
  generos_usuario = pd.merge(df_generos, df_items, left_on='id', right_on='item_id')

  if genero in generos_usuario['genres'].values:
    generos_usuario_top = generos_usuario.groupby(['genres', 'user_id', 'user_url'])[['playtime_forever']].sum()
    generos_usuario_top['playtime_forever'] = generos_usuario_top['playtime_forever'] / 60
    grupo_genero = generos_usuario_top[generos_usuario_top.index.get_level_values('genres') == genero]
    grupo_genero_ordenado = grupo_genero.sort_values(by='playtime_forever', ascending=False)
    top_usuarios = grupo_genero_ordenado.reset_index()
    top_usuarios = top_usuarios[['user_id', 'user_url']].head(5)
    tuplas = []
    for indice, fila in top_usuarios.head(5).iterrows():
      columna1 = fila['user_id']
      columna2 = fila['user_url']
      tupla = (indice + 1, columna1, columna2)
      tuplas.append(tupla)
    return tuplas

  else:
    no_encontrado = f'El genero {genero} no fue encontrado en la base de datos'
    return no_encontrado
  
@app.get('/desarrollador')
def desarrollador(desarrollador: str):
  """
  Rentorna la cantidad de items y porcentaje de contenido Free por año según la empresa desarrolladora dada.
  """
  games_items = pd.merge(df_games, df_items, left_on='id', right_on='item_id')

  if desarrollador in games_items['developer'].values:
    # Cantidad de items
    cantidad_items = games_items.groupby('developer')[['item_id']].count()
    cantidad_items_desarrollador = cantidad_items.loc[desarrollador, 'item_id']
    cantidad = f'La cantidad de items del desarrollador {desarrollador} es {cantidad_items_desarrollador}'

    # Porcentaje de contenido free por empresa desarrolladora y año
    contenido = games_items[['id','developer', 'price', 'release_date']]
    contenido = contenido[contenido['release_date'].notna()]
    contenido['year'] = contenido['release_date'].str.split('-').str[0].astype(int)
    contenido_gratis = contenido[contenido['price'] == 0]
    desarrollador_year_gratis = contenido_gratis.groupby(['developer', 'year'])[['id']].nunique().reset_index()
    desarrollador_year_gratis['contenido_gratis'] = desarrollador_year_gratis['id']
    desarrollador_year_gratis.drop(columns=['id'], inplace=True)
    desarrollador_year = contenido.groupby(['developer', 'year'])[['id']].nunique().reset_index()
    desarrollador_year['contenido_total'] = desarrollador_year['id']
    desarrollador_year.drop(columns=['id'], inplace=True)
    resultado = pd.merge(desarrollador_year_gratis, desarrollador_year, on=['developer', 'year'], how='inner')
    resultado['porcentaje'] = resultado['contenido_gratis'] / resultado['contenido_total'] * 100
    resultado['porcentaje'] = resultado['porcentaje'].astype(int)
    resultado = resultado.groupby(['developer', 'year'], group_keys=True)[['porcentaje']].apply(lambda x: x)
    desarrollador_porcentaje = resultado[resultado.index.get_level_values('developer') == desarrollador]
    desarrollador_porcentaje = tabulate(desarrollador_porcentaje, headers='keys', tablefmt='psql')
    return cantidad, desarrollador_porcentaje

  else:
    no_encontrado = f'El desarrollador {desarrollador} no fue encontrado en la base de datos'
    return no_encontrado
  
@app.get('/sentiment_analysis')
def sentiment_analysis(year: int):
  """
  Según el año de lanzamiento, se devuelve una lista con la cantidad de registros de reseñas de usuarios
  que se encuentren categorizados con un análisis de sentimiento.
  """
  games_reviews = pd.merge(df_games, df_reviews, left_on='id', right_on='item_id')
  sentimiento = games_reviews[['release_date', 'sentiment']]
  sentimiento = sentimiento[sentimiento['release_date'].notna()]
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
