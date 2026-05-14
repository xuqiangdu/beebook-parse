from __future__ import annotations

from flask import Blueprint, request

from api.common import api_err, api_ok, CODE_PARAM_INVALID
from services.aa_key_pool import add_key, list_keys, check_expiry
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


@aa_keys_bp.route("/api/admin/aa-keys/expiry-check", methods=["POST"])
def expiry_check_aa_keys():
    """检查池内所有 AA key 的会员到期时间（登录三方账户页解析真实到期日）。

    刷新策略：缓存的到期时间距今 >= AA_KEY_EXPIRY_REFRESH_DAYS 天时直接用缓存；
    不足该阈值（快过期）时才二次登录三方拉取，这样人工续费后能无感刷新。
    传 force=true 可强制全部重新拉取。
    """
    data = _json_body()
    err = _check_secret(data)
    if err:
        return err

    force = bool(data.get("force"))
    return api_ok({"keys": check_expiry(force=force)})
