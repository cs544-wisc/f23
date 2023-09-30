import numpy as np
import torch
from server import PredictionCache  # type: ignore

prediction_cache = PredictionCache()

coefs = torch.Tensor(np.array([1, 2, 3], dtype=np.float32).reshape(-1, 1))

inputs = torch.Tensor(np.array([1, 2, 3], dtype=np.float32).reshape(1, -1))
inputs_delta = torch.Tensor(
    np.array([0.00004, 0.00001, -0.00004], dtype=np.float32).reshape(1, -1)
)

prediction_cache.SetCoefs(coefs=coefs)
(y, hit) = prediction_cache.Predict(X=inputs)
print(f"y={y.item()}, hit={hit}")  # should be 14.0, false
(y, hit) = prediction_cache.Predict(X=inputs)
print(f"y={y.item()}, hit={hit}")  # should be 14.0, true
(y, hit) = prediction_cache.Predict(X=inputs + inputs_delta)
print(f"y={y.item()}, hit={hit}")  # should be 14.0, true
(y, hit) = prediction_cache.Predict(X=inputs + 1)
print(f"y={y.item()}, hit={hit}")  # should be 20.0, false
