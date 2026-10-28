"""Flask routes for Trends.Earth sign-in and sign-out."""

import logging
from urllib.parse import urlencode

import flask
import oidc
import session_store
from auth import end_session, start_session
from flask import Blueprint, Response, redirect, request
from flask_wtf.csrf import validate_csrf
from wtforms.validators import ValidationError

logger = logging.getLogger(__name__)


def create_auth_blueprint(limiter):
    bp = Blueprint("auth", __name__)

    @bp.route("/auth/login")
    @limiter.limit("30 per minute")
    def login():
        try:
            url = oidc.begin_login(flask.session, request.args.get("next") or "/")
        except oidc.OIDCProviderUnavailable:
            logger.exception("Could not start Trends.Earth sign-in")
            return redirect("/login?" + urlencode({"error": "unavailable"}))
        return redirect(url)

    @bp.route("/auth/callback")
    @limiter.limit("30 per minute")
    def callback():
        try:
            record, next_url = oidc.handle_callback(flask.session, request.args)
            start_session(record)
        except oidc.OIDCAccessDenied as exc:
            return redirect("/access?" + urlencode({"status": exc.status}))
        except (oidc.OIDCProviderUnavailable, session_store.SessionUnavailable):
            logger.exception("Trends.Earth sign-in could not be completed")
            return redirect("/login?" + urlencode({"error": "unavailable"}))
        except oidc.OIDCTokenRejected as exc:
            logger.warning("Trends.Earth sign-in rejected: %s", exc)
            return redirect("/login?" + urlencode({"error": "failed"}))
        return redirect(next_url)

    @bp.route("/auth/logout", methods=["POST"])
    def logout():
        try:
            validate_csrf(request.form.get("csrf_token"))
        except ValidationError:
            return redirect("/logout")
        id_token = end_session()
        return Response(oidc.logout_page(id_token), mimetype="text/html")

    return bp
