import os

# app.config builds Settings at import time and requires a Groq key. Tests that import
# app.main (transitively app.config) need one present before the module is loaded.
os.environ.setdefault("GROQ_API_KEY", "test-key")
