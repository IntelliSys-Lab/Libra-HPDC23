import numpy as np
import pickle
from sklearn.linear_model import SGDClassifier, LinearRegression
from sklearn.svm import LinearSVC, LinearSVR
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.neural_network import MLPClassifier, MLPRegressor
import params

RANDOM_STATE = 114514
TRAIN_SIZE = 0.7
MAX_ITER = 200
ACTIVATION = "tanh"

class Classifier():
    def __init__(self, function_name, model_name):
        self.function_name = function_name
        self.model_name = model_name

        if model_name == "clas_logistic":
            self.model = SGDClassifier(loss="log", random_state=RANDOM_STATE)
        elif model_name == "clas_svm":
            self.model = LinearSVC(random_state=RANDOM_STATE)
        elif model_name == "clas_rand":
            self.model = RandomForestClassifier(max_depth=2, random_state=RANDOM_STATE)
        elif model_name == "clas_mlp":
            self.model = MLPClassifier(max_iter=MAX_ITER, activation=ACTIVATION)
        
    def predict(self, x):
        return self.model.predict(x)

    def save(self):
        with open(params.MODEL_SAVE_PATH + '{}_{}.pkl'.format(self.function_name, self.model_name), 'wb') as f:
            pickle.dump(self.model, f)

    def load(self):
        with open(params.MODEL_SAVE_PATH + '{}_{}.pkl'.format(self.function_name, self.model_name), 'rb') as f:
            self.model = pickle.load(f)

class Regressor():
    def __init__(self, function_name, model_name):
        self.function_name = function_name
        self.model_name = model_name

        if model_name == "reg_linear":
            self.model = LinearRegression()
        elif model_name == "reg_svm":
            self.model = LinearSVR(random_state=RANDOM_STATE)
        elif model_name == "reg_rand":
            self.model = RandomForestRegressor(random_state=RANDOM_STATE)
        elif model_name == "reg_mlp":
            self.model = MLPRegressor(max_iter=MAX_ITER, activation=ACTIVATION)

    def predict(self, x):
        return self.model.predict(x)

    def save(self):
        with open(params.MODEL_SAVE_PATH + '{}_{}.pkl'.format(self.function_name, self.model_name), 'wb') as f:
            pickle.dump(self.model, f)

    def load(self):
        with open(params.MODEL_SAVE_PATH + '{}_{}.pkl'.format(self.function_name, self.model_name), 'rb') as f:
            self.model = pickle.load(f)

class LibraAgent():
    def __init__(
        self,
        user_config,
        cpu_model_name,
        memory_model_name,
        duration_model_name,
    ):  
        self.user_config = user_config
        self.cpu_model = {}
        self.memory_model = {}
        self.duration_model = {}
        for function_id in user_config.keys():
            self.cpu_model[function_id] = Classifier("{}_cpu".format(function_id), cpu_model_name)
            self.memory_model[function_id] = Classifier("{}_memory".format(function_id), memory_model_name)
            self.duration_model[function_id] = Regressor("{}_duration".format(function_id), duration_model_name)

    def load(self):
        for function_id in self.cpu_model.keys():
            self.cpu_model[function_id].load()
        for function_id in self.memory_model.keys():
            self.memory_model[function_id].load()
        for function_id in self.duration_model.keys():
            self.duration_model[function_id].load()

    def predict(self, function_id, input_size):
        input_size = np.array(input_size).reshape(-1, 1)
        predict_cpu = self.cpu_model[function_id].predict(input_size)
        predict_memory = self.memory_model[function_id].predict(input_size)
        predict_duration = self.duration_model[function_id].predict(input_size)[0]

        return predict_cpu, predict_memory, predict_duration

    def update(self, function_id, data):
        pass

