"""Internal browser cover capture endpoint."""

import base64
import logging

from flask import Blueprint

from api.common import api_ok, api_err, CODE_PARAM_INVALID, CODE_UPSTREAM_FAIL, CODE_OVERLOADED
from services.cover_service import capture_cover, CoverCaptureBusy

logger = logging.getLogger(__name__)
cover_bp = Blueprint("cover", __name__)


@cover_bp.route("/api/covers/<md5>/screenshot", methods=["POST"])
def screenshot(md5):
    try:
        data, source_kind = capture_cover(md5)
        return api_ok({
            "image_base64": base64.b64encode(data).decode("ascii"),
            "mime_type": "image/png",
            "source_kind": source_kind,
        })
    except CoverCaptureBusy:
        return api_err(CODE_OVERLOADED, "cover browser is busy", http_status=429,
                       headers={"Retry-After": "5"})
    except ValueError as exc:
        return api_err(CODE_PARAM_INVALID, str(exc), http_status=400)
    except Exception as exc:
        logger.warning("Cover capture failed md5=%s error=%s", md5, type(exc).__name__)
        return api_err(CODE_UPSTREAM_FAIL, "cover screenshot unavailable", http_status=502)
