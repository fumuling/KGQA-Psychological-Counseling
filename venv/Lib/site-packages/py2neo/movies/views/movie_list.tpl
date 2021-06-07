<!doctype html>
<html>

  <head>
    <title>Movie List - The Movie Graph</title>
    <link rel="stylesheet" href="/static/main.css">
  </head>

  <body>

    <div class="header">
      <nav><a href="/">The Movie Graph</a> / <strong>Movies</strong></nav>
    </div>

    <h1>Movies</h1>
    <ul>
    % for movie in movies:
        <li><a href="/movie/{{ movie.title }}">{{ movie.title }} [{{ movie.released }}]</a></li>
    % end
    </ul>

    <div class="footer">
      <code>(graphs)-[:ARE]->(everywhere)</code>
    </div>

  </body>

</html>

