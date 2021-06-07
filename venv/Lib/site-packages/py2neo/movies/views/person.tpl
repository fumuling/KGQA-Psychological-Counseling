<!doctype html>
<html>

  <head>
    <title>{{ person.name }} - The Movie Graph</title>
    <link rel="stylesheet" href="/static/main.css">
  </head>

  <body>

    <div class="header">
      <nav><a href="/">The Movie Graph</a> / <a href="/person/">People</a> / <strong>{{ person.name }}</strong></nav>
    </div>

    <h1>{{ person.name }}</h1>

    <h2>Personal Details</h2>
    <dl>
      <dt>Name:</dt>
        <dd>{{ person.name }}</dd>
      % if person.born:
      <dt>Born:</dt>
        <dd>{{ person.born }}</dd>
      % end
    </dl>

    % if movies:
    <h2>Movies</h2>
    <ul>
    % for movie, role in movies:
      <li class="{{ role }}"><a href="/movie/{{ movie }}">{{ movie }}</a> [{{ role }}]</li>
    % end
    </ul>
    % end

    % if person.reviewed:
    <h2>Reviews</h2>
    % for movie in person.reviewed:
    <p>
      Reviewed <a href="/movie/{{ movie.title }}">{{ movie.title }}</a> and gave it {{ person.reviewed.get(movie, "rating") }}%, saying...
      <blockquote>{{ person.reviewed.get(movie, "summary") }}</blockquote>
    </p>
    % end
    % end


    <div class="footer">
      <code>(graphs)-[:ARE]->(everywhere)</code>
    </div>

  </body>

</html>

