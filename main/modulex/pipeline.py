#PIPELINE
#
# 1. use unmodified data (dont drop null by default)
#
#
#
#
#
#
#

import os
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import isnan, when, count, col, lit
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder
#from pyspark.ml.pipeline import PipelineModel
from simple_colors import *
from modulex.refresh import refresh
from modulex.refresh_linux import refresh_linux
import platform, time
from alive_progress import alive_bar

pipelineNAME = None
regressor = None
LabelCol = None



def refresher():
    if platform.system()=='Linux':
        refresh_linux()
    else:
        refresh()

def pipelineINIT(df):
    global pipelineNAME, regressor, LabelCol
    refresher()
    
    print(magenta("The Current active has following Columns ...", 'blink'))
    
    i=0
    for c in df.columns:
        print(f"{i+1}: {c}")
        i=i+1
    
    nInputCol = int(input(yellow("Enter number of input cols: ")))
    InputCol = []
    refresher()
    
    for n in range(nInputCol):
        print(magenta("The Current active has following Columns ...", 'blink'))
        x=0
        for c in df.columns:
            print(f"{x+1}: {c}")
            x=x+1
        value = input(yellow(f"Enter column {n+1}: "))
        InputCol.append(value)
        refresher()
        
    
    refresher()
    
    print(magenta("The Current active has following Columns ...", 'blink'))
    i=0
    for c in df.columns:
        print(f"{i+1}: {c}")
        i=i+1
    
    LabelCol = input(yellow("Enter Column to Predict: "))
    
    refresher()
    
    assembler = VectorAssembler(inputCols=InputCol, outputCol='Attributes')
    regressor = RandomForestRegressor(featuresCol='Attributes', labelCol=LabelCol)
    pipeline = Pipeline(stages = [assembler, regressor])
    pipelinePATH = os.path.join(os.path.join(os.getcwd(), 'main'), 'pipelines')
    pipelineNAME = input(yellow("Pipeline Name: ", 'blink'))
    
    if not os.path.exists(pipelinePATH):
        os.mkdir(pipelinePATH)
    
    pipeline.write().overwrite().save(os.path.join(pipelinePATH, pipelineNAME))
    print(f"Saving Pipeline to {os.path.join(pipelinePATH, pipelineNAME)}")
    time.sleep(5)
    refresher()

def pipelinePROVOKE(df):
    global pipelineNAME, regressor, LabelCol
    
    pipelineModel = Pipeline.load(os.path.join(os.path.join(os.path.join(os.getcwd(), 'main'), 'pipelines'), pipelineNAME))
    
    print(red("Training...", 'blink'))
    
    with alive_bar(100, bar='smooth') as bar:
        paramGrid = ParamGridBuilder().addGrid(regressor.numTrees, [10, 50, 100, 500, 1000]).build()
        time.sleep(2)
        bar(14)
        crossval = CrossValidator(estimator=pipelineModel, estimatorParamMaps=paramGrid, evaluator=RegressionEvaluator(labelCol=LabelCol), numFolds=15)
        time.sleep(2.5)
        bar(14)
        df_temp = df
        train_data, test_data = df_temp.randomSplit([0.8,0.2], seed=1)
        time.sleep(1)
        bar(10)
        with bar.pause():
            Model = crossval.fit(train_data)
            bestModel = Model.bestModel
        time.sleep(1)
        bar(20)
        pred = Model.transform(test_data)
        time.sleep(4)
        bar(42)
        time.sleep(4)
        
    refresher()
    print(magenta(f"The Pipeline model {pipelineNAME} predicts..."))
    pred.select(LabelCol, 'prediction').show()
    result = eval("pred.select(LabelCol, 'prediction')")
    
    
    if not os.path.exists(os.path.join(os.path.join(os.getcwd(),'main'), 'pipeline-results')):
        os.mkdir(os.path.join(os.path.join(os.getcwd(),'main'), 'pipeline-results'))
     
    result_location = os.path.join(os.path.join(os.path.join(os.getcwd(),'main'), 'pipeline-results'), pipelineNAME+"-results.csv")
    print(f"Saving Results to {result_location} ...")
    result.toPandas().to_csv(result_location)
    
    #Metrics
    evalu = RegressionEvaluator(labelCol=LabelCol)
    rmse = evalu.evaluate(pred)
    mse = evalu.evaluate(pred, {evalu.metricName: "mse"})
    mae = evalu.evaluate(pred, {evalu.metricName: "mae"})
    r2 = evalu.evaluate(pred, {evalu.metricName: "r2"})
    print("\nMetrics:")
    print(yellow("RMSE: ")+magenta(f"{rmse}")+yellow(" MSE: ")
          +magenta(f"{mse}")+yellow(" MAE: ")+magenta(f"{mae}")
          +yellow(" R2: ")+magenta(f"{r2}\n"))