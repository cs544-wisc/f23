from modelserver import ModelServer

modelserver = ModelServer()

modelserver.SetCoefs(coefs=[1, 2, 3])
resp = modelserver.Predict(inputs=[1, 2, 3])
print(f"value={resp.value}, cache_hit={resp.cache_hit}")  # should be 14, false
resp = modelserver.Predict(inputs=[1, 2, 3])
print(f"value={resp.value}, cache_hit={resp.cache_hit}")  # should be 14, true
resp = modelserver.Predict(inputs=[1.00004, 2.00001, 2.99996])
print(f"value={resp.value}, cache_hit={resp.cache_hit}")  # should be 14, true
resp = modelserver.Predict(inputs=[2, 3, 4])
print(f"value={resp.value}, cache_hit={resp.cache_hit}")  # should be 20, false
