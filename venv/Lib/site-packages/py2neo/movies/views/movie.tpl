<!doctype html>
<html>

  <head>
    <title>{{ movie.title }} [{{ movie.released }}] - The Movie Graph</title>
    <link rel="stylesheet" href="/static/main.css">
  </head>

  <body>

    <div class="header">
      <nav><a href="/">The Movie Graph</a> / <a href="/movies/">Movies</a> / <strong>{{ movie.title }}</strong></nav>
    </div>

    <h1>{{ movie.title }}</h1>

    <h2>Movie Details</h2>
    <dl>
        <dt>Title:</dt>
          <dd>{{ movie.title }}</dd>
        <dt>Released:</dt>
          <dd>{{ movie.released }}</dd>
        <dt>Directors:</dt>
          <dd>
            % for director in movie.directors:
                <a href="/person/{{director.name}}">{{director.name}}</a><br>
            % end
          </dd>
        % if movie.writers:
        <dt>Writers:</dt>
          <dd>
            % for writer in movie.writers:
                <a href="/person/{{ writer.name }}">{{writer.name}}</a><br>
            % end
          </dd>
        % end
    </dl>

    <h2>Cast</h2>
    <ul>
    % for actor in movie.actors:
        <li><a href="/person/{{ actor.name }}">{{ actor.name }}</a></li>
    % end
    </ul>
    
    <h2>Reviews</h2>
    % for reviewer in movie.reviewers:
    <p>
      <a href="/person/{{ reviewer.name }}">{{ reviewer.name }}</a> gave it {{ movie.reviewers.get(reviewer, "rating") }}% and said...
      <blockquote>{{ movie.reviewers.get(reviewer, "summary") }}</blockquote>
    </p>
    % end
    
    <form method="POST" action="review">
      <h3>Submit a new review</h3>

      <input type="hidden" name="title" value="{{ movie.title }}">

      <label>Name:<br>
      <input type="text" name="name" value="">
      </label><br>

      <label>Summary:<br>
      <textarea name="summary" cols="80" rows="6"></textarea>
      </label><br>

      <label>Rating:<br>
      <input type="text" name="rating" value="">%
      </label><br>

      <input type="submit" value="Submit">

    </form>

    <div class="footer">
      <code>(graphs)-[:ARE]->(everywhere)</code>
    </div>

  </body>

</html>

