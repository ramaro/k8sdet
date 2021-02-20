import logging
from importlib import import_module
from inspect import Parameter, signature

from kadet import BaseObj, Dict
from kubernetes.client import ApiClient

logger = logging.getLogger(__name__)

K8S_API = ApiClient()

K8S_CLIENT_MODELS_URL = "https://github.com/kubernetes-client/python/blob/master/kubernetes/client/models/__init__.py"

def from_model(model_name):
    "return class from model"
    model = k8s_model_class_from_name(model_name)
    class K8sdetModel(K8sObj):

        def new(self):
            _kwargs = {"model": model, "params": {},}
            for k, v in self.kwargs.items():
                _kwargs["params"][k] = v
            self.kwargs = Dict(_kwargs)
            super().new()

    K8sdetModel.model = model
    set_signature(K8sdetModel)

    return K8sdetModel

def set_signature(k8s_model):
    sig = signature(k8s_model)
    params = [Parameter(param, Parameter.POSITIONAL_OR_KEYWORD, annotation=None)
            for param, _ in k8s_model.model.attribute_map.items()]
    k8s_model.__signature__ = sig.replace(parameters=params)


def k8s_model_class_from_name(model_name):
    k8s_client_module = import_module("kubernetes.client.models")
    model_class = getattr(k8s_client_module, model_name, None)
    if model_class is None:
        raise ValueError(f"Could not load kubernetes-client model '{model_name}'")
    return model_class

class K8sObj(BaseObj):
    def new(self):
        self.need(
            "model", f"need kubernetes-client model class from {K8S_CLIENT_MODELS_URL}"
        )
        self.need("params", f"need kubernetes-client model params", istype=dict)


    def body(self):
        logger.debug("using model params", self.kwargs.params)

        # if param is BaseObj, run .dump()
        for param in self.kwargs.params:
            if isinstance(self.kwargs.params[param], BaseObj):
                self.kwargs.params[param] = self.kwargs.params[param].dump()

        # camelCase kubernetes key style
        # see https://github.com/kubernetes-client/python/issues/390
        sanitized_obj = K8S_API.sanitize_for_serialization(
            self.kwargs.model(**self.kwargs.params)
        )
        self.root = Dict(sanitized_obj)
