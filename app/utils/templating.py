"""
模板渲染兼容工具
兼容不同 Starlette/FastAPI 版本的 TemplateResponse 调用签名。
"""
from inspect import signature
from typing import Any, Dict


def render_template_response(
    templates: Any,
    request: Any,
    template_name: str,
    context: Dict[str, Any]
):
    """
    统一渲染模板，自动适配两种 TemplateResponse 签名：
    1) TemplateResponse(name, context, ...)
    2) TemplateResponse(request, name, context, ...)
    """
    full_context = {"request": request, **context}

    params = list(signature(templates.TemplateResponse).parameters.keys())
    first_param = params[0] if params else "name"

    if first_param == "request":
        return templates.TemplateResponse(request, template_name, full_context)
    return templates.TemplateResponse(template_name, full_context)
