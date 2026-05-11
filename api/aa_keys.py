from __future__ import annotations

from flask import Blueprint, request

from api.common import api_err, api_ok, CODE_PARAM_INVALID
from services.aa_key_pool import add_key, list_keys
import config

aa_keys_bp = Blueprint("aa_keys", __name__)


def _json_body() -> dict:
    return request.get_json(silent=True) or {}


def _check_secret(data: dict):
    if data.get("secret") != config.AA_KEY_ADMIN_SECRET:
        return api_err(CODE_PARAM_INVALID, "secret 无效", http_status=403)
    return None


@aa_keys_bp.route("/api/admin/aa-keys/add", methods=["POST"])
def add_aa_key():
    data = _json_body()
    err = _check_secret(data)
    if err:
        return err

    secret_key = str(data.get("key", "")).strip()
    if not secret_key:
        return api_err(CODE_PARAM_INVALID, "请提供 key", http_status=400)

    inserted = add_key(secret_key, source="api")
    return api_ok({"inserted": inserted, "keys": list_keys()})


@aa_keys_bp.route("/api/admin/aa-keys/list", methods=["POST"])
def list_aa_keys():
    data = _json_body()
    err = _check_secret(data)
    if err:
        return err

    return api_ok({"keys": list_keys()})
