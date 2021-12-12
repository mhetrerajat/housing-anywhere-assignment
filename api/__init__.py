import os

from flask import Flask


def create_app():
    app = Flask(__name__)
    app.config.from_mapping(
        DATABASE=os.path.join(app.instance_path, "db.sqlite"),
        RAW_EVENTS_DATA=os.path.join(app.root_path, "events_data.json"),
    )

    os.makedirs(app.instance_path, exist_ok=True)

    # DB
    from api.db import close_db, init_db_command

    app.teardown_appcontext(close_db)
    app.cli.add_command(init_db_command)

    @app.route("/", methods=["GET"])
    def index():
        return {"message": "Mock API. Only supports /v1/events/ endpoint."}

    # Register Blueprint
    from api import events

    app.register_blueprint(events.bp)

    return app
