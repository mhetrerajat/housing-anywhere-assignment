from flask import Flask


def create_app():
    app = Flask(__name__)

    @app.route("/", methods=["GET"])
    def index():
        return {"message": "Mock API. Only supports /v1/events/ endpoint."}

    # Register Blueprint
    from api import events

    app.register_blueprint(events.bp)

    return app
