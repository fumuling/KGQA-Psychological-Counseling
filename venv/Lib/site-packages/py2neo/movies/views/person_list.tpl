<!doctype html>
<html>

  <head>
    <title>Person List - The Movie Graph</title>
    <link rel="stylesheet" href="/static/main.css">
  </head>

  <body>

    <div class="header">
      <nav><a href="/">The Movie Graph</a> / <strong>People</strong></nav>
    </div>

    <h1>People</h1>
    <ul>
    % for person in people:
        <li><a href="/person/{{ person.name }}">{{ person.name }}</a></li>
    % end
    </ul>

    <div class="footer">
      <code>(graphs)-[:ARE]->(everywhere)</code>
    </div>

  </body>

</html>

