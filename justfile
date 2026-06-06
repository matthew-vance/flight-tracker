[working-directory("api")]
api:
    node src/index.ts

[working-directory("producer")]
[env("VIRTUAL_ENV", "")]
producer:
    uv run main.py

[working-directory("consumer")]
[env("VIRTUAL_ENV", "")]
consumer:
    uv run main.py

[working-directory("notebooks")]
[env("VIRTUAL_ENV", "")]
notebooks:
    uv run jupyter lab
