from utils.context import Context
import os
from glob import glob
import pandas as pd


def listAllFile(path):
    return [y for x in os.walk(path) for y in glob(os.path.join(x[0], '*.csv'))]


def mergeAllFiles(allFiles, target):
    reg = None
    selectedFile = list(filter(lambda x: target in x, allFiles))
    for file in selectedFile:
        name = file.split(sep=".")[1]
        if name.isnumeric():
            df = pd.read_csv(file, engine='c')
            df.columns = ['timestamps', name]
            df['timestamps'] = [i for i in range(df.shape[0])]

            if reg is None:
                reg = df
            else:
                reg = reg.merge(df)

    timestamps = reg.iloc[:, 0]
    value = (reg.iloc[:, 1:]).mean(axis=1).rename(target)
    pd.concat([timestamps,value], axis=1)

    return pd.concat([timestamps,value], axis=1)


def postProcessApplicationMetrics(path, appName):
    allFiles = listAllFile(path)

    selects = ["jvm.heap.usage.csv",
               "ExecutorMetrics.OnHeapStorageMemory.csv",
               "ExecutorMetrics.OnHeapExecutionMemory.csv",
               "ExecutorMetrics.OnHeapUnifiedMemory.csv",
               "executor.jvmGCTime.csv",
               "executor.cpuTime.csv",
               "executor.runTime.csv"]

    reg = None

    for tgt in selects:
        df = mergeAllFiles(allFiles, tgt)

        if tgt == "jvm.heap.usage.csv":
            df[tgt] = df[tgt]
        elif tgt == "executor.cpuTime.csv":
            df[tgt] = df[tgt] / 1000000000
        elif tgt == "executor.runTime.csv":
            df[tgt] = df[tgt] / 1000
        elif tgt == "executor.jvmGCTime.csv":
            df[tgt] = df[tgt] / 1000
        elif tgt == "ExecutorMetrics.OnHeapStorageMemory.csv":
            df[tgt] = df[tgt] / (2 * 1024 * 1024)
        elif tgt == "ExecutorMetrics.OnHeapExecutionMemory.csv":
            df[tgt] = df[tgt] / (2 * 1024 * 1024)
        elif tgt == "ExecutorMetrics.OnHeapUnifiedMemory.csv":
            df[tgt] = df[tgt] / (4 * 1024 * 1024)
        else:
            df[tgt] = df[tgt]

        if reg is None:
            reg = df
        else:
            reg = reg.merge(df)

    reg.to_csv(os.path.join(path, f"{appName}-result.csv"), index=False)
    return reg