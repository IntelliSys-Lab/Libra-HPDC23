import time
import numpy as np
from sklearn.linear_model import SGDClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline
X = np.array([[-1, -1], [-2, -1], [1, 1], [2, 1]])
Y = np.array([1, 1, 2, 2])
# Always scale the input. The most convenient way is to use a pipeline.
clf = SGDClassifier(max_iter=1000, tol=1e-3)
clf.fit(X, Y)

start_time = time.time()
clf.partial_fit(
    np.array([[9, 2], [9, 2]]),
    np.array([2])
)
end_time = time.time()

print(end_time - start_time)