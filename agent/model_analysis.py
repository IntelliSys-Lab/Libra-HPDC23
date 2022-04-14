import numpy as np
import time
import pickle
import csv
from sklearn.linear_model import SGDClassifier, LinearRegression
from sklearn.svm import LinearSVC, LinearSVR
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.neural_network import MLPClassifier, MLPRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, mean_squared_error, r2_score
from sklearn.utils.multiclass import unique_labels
from sklearn.utils._testing import ignore_warnings
from sklearn.exceptions import ConvergenceWarning

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
        
    @ignore_warnings(category=ConvergenceWarning)
    def train(self, x, y):
        self.model.fit(x, y)

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

    @ignore_warnings(category=ConvergenceWarning)
    def train(self, x, y):
        self.model.fit(x, y)

    def predict(self, x):
        return self.model.predict(x)

    def save(self):
        with open(params.MODEL_SAVE_PATH + '{}_{}.pkl'.format(self.function_name, self.model_name), 'wb') as f:
            pickle.dump(self.model, f)

    def load(self):
        with open(params.MODEL_SAVE_PATH + '{}_{}.pkl'.format(self.function_name, self.model_name), 'rb') as f:
            self.model = pickle.load(f)

def create_dataset(function_id):
    x = []
    y_cpu = []
    y_memory = []
    y_duration = []

    with open("logs/{}_usage.csv".format(function_id), newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            x.append([int(row["size"])])
            y_cpu.append(np.clip(int(float(row["cpu_peak"]))+1, 1, 8))
            # y_cpu.append(np.clip(int(float(row["cpu_peak"])), 0, 8))
            y_memory.append(np.clip(int(float(row["mem_peak"])/128)+1, 1, 8))
            # y_memory.append(np.clip(int(float(row["mem_peak"])/128), 0, 8))
            # y_memory.append(np.clip(int(float(row["mem_peak"]))+1, 1, 1024))
            y_duration.append(float(row["duration"]))

        x = np.array(x)
        y_cpu = np.array(y_cpu)
        y_memory = np.array(y_memory)
        y_duration = np.array(y_duration)

        x_cpu_train, x_cpu_test, y_cpu_train, y_cpu_test = train_test_split(x, y_cpu, train_size=TRAIN_SIZE)
        x_memory_train, x_memory_test, y_memory_train, y_memory_test = train_test_split(x, y_memory, train_size=TRAIN_SIZE)
        x_duration_train, x_duration_test, y_duration_train, y_duration_test = train_test_split(x, y_duration, train_size=TRAIN_SIZE)
        # print(y_cpu_train)
        # print(np.insert(y_cpu_train, 0, [0]).reshape(-1, 1))
        dataset = {}
        dataset["cpu_train"] = [np.insert(x_cpu_train, 0, [0]).reshape(-1, 1), np.insert(y_cpu_train, 0, [0])]
        dataset["cpu_test"] = [np.insert(x_cpu_test, 0, [0]).reshape(-1, 1), np.insert(y_cpu_test, 0, [0])]
        dataset["memory_train"] = [np.insert(x_memory_train, 0, [0]).reshape(-1, 1), np.insert(y_memory_train, 0, [0])]
        dataset["memory_test"] = [np.insert(x_memory_test, 0, [0]).reshape(-1, 1), np.insert(y_memory_test, 0, [0])]
        dataset["duration_train"] = [x_duration_train, y_duration_train]
        dataset["duration_test"] = [x_duration_test, y_duration_test]

    return dataset

def train_model(train_data, model):
    # start_time = time.time()
    if "reg" not in model.model_name:
        if len(unique_labels(train_data[1])) <= 1:
            print("{} only has one unique label {}!".format(model.function_name, unique_labels(train_data[1])))

    model.train(train_data[0], train_data[1])
    model.save()
    # end_time = time.time()
    # print("{}, {}, {}".format(model.model_name, model.function_name, end_time-start_time))

def test_model(test_data, model, type):
    model.load()
    
    start_time = time.time()
    pred = model.predict(test_data[0])
    end_time = time.time()

    if type == "clas":
        out = [end_time - start_time, accuracy_score(test_data[1], pred)]
    else:
        out = [end_time - start_time, mean_squared_error(test_data[1], pred), r2_score(test_data[1], pred)]

    return out

def log_avg(result):
    for model_name in result.keys():
        if "clas" in model_name:
            csv_clas_cpu = [["function_id", "acc"]]
            csv_clas_memory = [["function_id", "acc"]]
            csv_clas_overhead = [["function_id", "overhead"]]
            for function_id in result[model_name].keys():
                csv_clas_cpu.append([function_id, result[model_name][function_id]["cpu"]])
                csv_clas_memory.append([function_id, result[model_name][function_id]["memory"]])
                csv_clas_overhead.append([function_id, result[model_name][function_id]["overhead"]])

            with open("logs/{}_cpu.csv".format(model_name), "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(csv_clas_cpu)
            with open("logs/{}_memory.csv".format(model_name), "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(csv_clas_memory)
            with open("logs/{}_overhead.csv".format(model_name), "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(csv_clas_overhead)

        if "reg" in model_name:
            csv_reg = [["function_id", "mse", "r2"]]
            csv_reg_overhead = [["function_id", "overhead"]]
            for function_id in result[model_name].keys():
                csv_reg.append([function_id, result[model_name][function_id]["mse"], result[model_name][function_id]["r2"]])
                csv_reg_overhead.append([function_id, result[model_name][function_id]["overhead"]])

            with open("logs/{}.csv".format(model_name), "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(csv_reg)
            with open("logs/{}_overhead.csv".format(model_name), "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(csv_reg_overhead)


if __name__ == "__main__":
    funcs = ["dh", "eg", "ip", "vp", "ir", "knn", "gd", "alu", "ms", "dv"]
    clas_names = ["clas_logistic", "clas_svm", "clas_rand", "clas_mlp"]
    reg_names = ["reg_linear", "reg_svm", "reg_rand", "reg_mlp"]
    loop_time = 10
    result = {}
    for model_name in clas_names:
        result[model_name] = {}
        for function_id in funcs:
            result[model_name][function_id] = {}
            result[model_name][function_id]["cpu"] = []
            result[model_name][function_id]["memory"] = []
            result[model_name][function_id]["overhead"] = []

    for model_name in reg_names:
        result[model_name] = {}
        for function_id in funcs:
            result[model_name][function_id] = {}
            result[model_name][function_id]["mse"] = []
            result[model_name][function_id]["r2"] = []
            result[model_name][function_id]["overhead"] = []

    for _ in range(loop_time):
        for function_id in funcs:
            dataset = create_dataset(function_id)

            for model_name in clas_names:
                train_model(dataset["cpu_train"], Classifier("{}_cpu".format(function_id), model_name))
                cpu_out = test_model(dataset["cpu_test"], Classifier("{}_cpu".format(function_id), model_name), "clas")
                result[model_name][function_id]["overhead"].append(cpu_out[0])
                result[model_name][function_id]["cpu"].append(cpu_out[1])

                train_model(dataset["memory_train"], Classifier("{}_memory".format(function_id), model_name))
                memory_out = test_model(dataset["memory_test"], Classifier("{}_memory".format(function_id), model_name), "clas")
                result[model_name][function_id]["overhead"].append(memory_out[0])
                result[model_name][function_id]["memory"].append(memory_out[1])

            for model_name in reg_names:
                train_model(dataset["duration_train"], Regressor("{}_duration".format(function_id), model_name))
                duration_out = test_model(dataset["duration_test"], Regressor("{}_duration".format(function_id), model_name), "reg")
                result[model_name][function_id]["overhead"].append(duration_out[0])
                result[model_name][function_id]["mse"].append(duration_out[1])
                result[model_name][function_id]["r2"].append(duration_out[2])

    for function_id in funcs:
        for model_name in clas_names:
            result[model_name][function_id]["overhead"] = np.mean(result[model_name][function_id]["overhead"])
            result[model_name][function_id]["cpu"] = np.mean(result[model_name][function_id]["cpu"])
            result[model_name][function_id]["memory"] = np.mean(result[model_name][function_id]["memory"])
        for model_name in reg_names:
            result[model_name][function_id]["overhead"] = np.mean(result[model_name][function_id]["overhead"])
            result[model_name][function_id]["mse"] = np.mean(result[model_name][function_id]["mse"])
            result[model_name][function_id]["r2"] = np.mean(result[model_name][function_id]["r2"])

    log_avg(result)

    print("")
    print("Model analysis finished!")
    print("")
