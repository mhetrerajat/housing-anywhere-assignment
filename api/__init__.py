from flask import Flask


def create_app():
    app = Flask(__name__)

    # TODO: Add DB with initdb command.
    # This commands creates the raw events table using DDL statement and uploads the entire json data into DB.
    # The REST API should fetch the raw events from this table.

    @app.route("/", methods=["GET"])
    def index():
        return {"message": "Mock API. Only supports /v1/events/ endpoint."}

    # Register Blueprint
    from api import events

    app.register_blueprint(events.bp)

    return app
