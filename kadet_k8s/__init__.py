from importlib import import_module
import logging

from kadet import BaseObj, Dict
from kubernetes.client import ApiClient

logger = logging.getLogger(__name__)

K8S_API = ApiClient()

K8S_CLIENT_MODELS_URL = "https://github.com/kubernetes-client/python/blob/master/kubernetes/client/models/__init__.py"


class K8sObj(BaseObj):
    def new(self):
        self.need(
            "model", f"need kubernetes-client model name from {K8S_CLIENT_MODELS_URL}"
        )
        self.need("params", f"need kubernetes-client model params", istype=dict)

    def body(self):
        model = self.kwargs.model
        k8s_client_module = import_module("kubernetes.client.models")
        model_class = getattr(k8s_client_module, model, None)

        if model_class is None:
            raise ValueError(f"Could not load kubernetes-client model '{model}'")
        self.k8s_model = model_class

        logger.debug("using model params", self.kwargs.params)

        # if param is BaseObj, run .dump()
        for param in self.kwargs.params:
            if isinstance(self.kwargs.params[param], BaseObj):
                self.kwargs.params[param] = self.kwargs.params[param].dump()

        # camelCase kubernetes key style
        # see https://github.com/kubernetes-client/python/issues/390
        sanitized_obj = K8S_API.sanitize_for_serialization(
            self.k8s_model(**self.kwargs.params)
        )
        self.root = Dict(sanitized_obj)

    def model_need(self, key, **kwargs):
        """
        run need() on model params
        set param kwargs
        """
        self.need(key, **kwargs)
        self.kwargs.params[key] = self.kwargs[key]

    def model_optional(self, key, istype=None):
        """
        if set, copy key into model params
        if set, check type
        """
        if key in self.kwargs:
            self.kwargs.params[key] = self.kwargs[key]

            if istype is not None:
                assert isinstance(
                    self.kwargs[key], istype
                ), f"'{key}' is not instance of '{istype}'"


class Deployment(K8sObj):
    def new(self):
        self.kwargs.model = "V1Deployment"  # set kubernetes.client.model
        self.kwargs.params.api_version = "apps/v1"  # set V1Deployment api_version
        self.kwargs.params.kind = "Deployment"  # set V1Deployment kind
        self.model_need("spec", istype=DeploymentSpec)  # V1Deployment needs spec
        super().new()


class DeploymentSpec(K8sObj):
    def new(self):
        self.kwargs.model = "V1DeploymentSpec"
        self.model_need("selector", istype=LabelSelector)
        self.model_need("template", istype=PodTemplateSpec)
        super().new()


class LabelSelector(K8sObj):
    def new(self):
        self.kwargs.model = "V1LabelSelector"
        self.model_need("match_labels", istype=dict)
        super().new()


class PodTemplateSpec(K8sObj):
    def new(self):
        self.kwargs.model = "V1PodTemplateSpec"
        self.model_need("metadata", istype=ObjectMeta)
        self.model_need("spec", istype=PodSpec)
        super().new()


class ObjectMeta(K8sObj):
    def new(self):
        self.kwargs.model = "V1ObjectMeta"
        self.model_need("name", istype=str)
        self.model_need("annotations", istype=dict)
        self.model_need("namespace", istype=str)
        self.model_need("labels", istype=dict)
        super().new()


class PodSpec(K8sObj):
    def new(self):
        self.kwargs.model = "V1PodSpec"
        self.model_need("containers", istype=list)
        self.model_optional("node_selector", istype=dict)
        # ...
        super().new()
