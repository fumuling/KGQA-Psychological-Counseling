# Py2neo Demo: The Movie Graph

Prerequisites:
- A local Neo4j server running with default configuration
- The _Movies_ data set (`:play movies` in browser)
- The Flask web framework (`pip install flask`)

To run use the following (setting the password to whatever is set for your Neo4j installation):
```
NEO4J_PASSWORD=P4ssw0rd FLASK_ENV=development FLASK_APP=py2neo.demo.movies flask run
```

Then open the browser at [http://localhost:5000/]( http://127.0.0.1:5000/).
