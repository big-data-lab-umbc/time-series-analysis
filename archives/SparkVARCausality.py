# -*- coding: utf-8 -*-
from __future__ import print_function
import sys
from collections import defaultdict
from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import lag, col
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import lit
import numpy as np
from statsmodels.compat.python import (range, lrange, string_types,
                                       StringIO, iteritems)
from statsmodels.tsa.tsatools import vec, unvec, duplication_matrix
from statsmodels.tools.linalg import logdet_symm
import scipy.linalg
from statsmodels.tools.tools import chain_dot
import scipy.stats as stats
from pyspark.ml import Pipeline
import itertools
from multiprocessing.pool import ThreadPool
import csv
from datetime import datetime

startTime = datetime.now()
print("starting time: ", startTime)

spark = SparkSession \
    .builder \
    .appName("SparkVarCausality") \
    .getOrCreate()

spark.sparkContext.setLogLevel("FATAL")

# p_lag_input = int(sys.argv[1])
# var_caused_input = sys.argv[2].split(',')
# var_causing_input = sys.argv[3].split(',')
# data_file_name_input = sys.argv[2]
# thread_pool_num = int(sys.argv[3])
#
p_lag_input = 1
var_caused_input = 'x1'
var_causing_input = 'x2'
# data_file_name_input = 'synth_data_with_header.csv'
# data_file_name_input = '/Users/arjunpandya/JupyterNotebooks/var_smpl.csv'
data_file_name_input = '../source/testdata.csv'
thread_pool_num = 4

data_input = spark.read.csv(data_file_name_input, inferSchema=True, header=False)

x_list = data_input.columns
print(x_list)


def fitVar(p_lag_fitVar, var_caused, var_causing, call_method="causality", dataFrame=data_input):
    print('###### Fitting VAR ######')
    if call_method == "causality":
        p_lag = p_lag_input
        # var_caused = var_caused_input
        # var_causing = var_causing_input
        #         data_file_name = data_file_name_input
        current_lag = p_lag
        print("causality testing fitting tsa....")
    else:
        p_lag = p_lag_fitVar
        print("Hi! Welcome to in fitVar select-order loop, current lag is ")
        print(p_lag)
        #         data_file_name = data_file_name_input
        current_lag = p_lag

    # df_len_ori: number of variables in model, K
    x_list = data_input.columns
    df_len_ori = len(x_list)
    print("df_len_ori is ")
    print(df_len_ori)
    dataFrame_names = dataFrame.columns
    dataFrame = dataFrame.withColumn("id", monotonically_increasing_id())
    dataFrame.printSchema()

    # Here, VAR model regression_type is "const" same to R VAR library, and the default in Python VAR library
    w = Window().partitionBy().orderBy(col("id"))
    df_len = len(dataFrame.columns)
    ys_lagged_list = ["const"]
    for i in range(1, p_lag + 1):
        for j in range(0, df_len - 1):
            ys_lagged_list.append("%st-%s" % (x_list[j], str(i)))
            dataFrame = dataFrame.withColumn("%st-%s" % (x_list[j], str(i)), lag(dataFrame[j], i, 0).over(w))
    print("!!!!!!!!1")
    dataFrame.show(5)
    print(ys_lagged_list)

    # add "const" column of value 1 to get intercept when fitting the regression model
    dataFrame = dataFrame.withColumn("const", lit(1))
    dataFrame = dataFrame.withColumn("const", lag("const", p_lag, 0).over(w))
    #     for ii in range(0, df_len_ori):
    #         c_name_ii = "_c%s" % (str(ii))
    #         dataFrame = dataFrame.withColumn("_c%s-1" % (str(ii)), dataFrame[c_name_ii] - 1)

    # add "rid" column to control rollback process with p
    dataFrame = dataFrame.withColumn("rid", monotonically_increasing_id())
    dataFrame = dataFrame.filter(dataFrame.rid >= p_lag)
    dataFrame.show(5)
    print("====added rid columns Pappu padoda ====")

    #     build ys_lagged dataframe, will be used in F-test
    ys_lagged = dataFrame.select(ys_lagged_list)
    ys_lagged_len = ys_lagged.count()
    print('ye dikhai lagged value')
    ys_lagged.show(10)

    #     dataFrame = dataFrame.drop('id')
    dataFrame = dataFrame.drop('rid')
    dataFrame = dataFrame.drop('const')
    input_feature_name = dataFrame.schema.names
    print('Phele ka pappu',input_feature_name)

    input_feature_name.remove("id")
    for x_name in x_list:
        input_feature_name.remove('{}'.format(x_name))

    print('Baad ka pappu',input_feature_name)

    # assemble the vector for MLlib linear regression
    assembler_for_lag = VectorAssembler(
        inputCols=input_feature_name,
        outputCol="features")

    #     training_df = assembler_for_lag.transform(dataFrame)

    select_y = x_list[0]
    print(select_y)

    a = {}
    b = {}
    lrModels = []
    resids = []
    trainingSummaries = []

    for select_y in x_list:
        model_key = 'model_{}'.format(select_y)
        res_key = 'res_{}'.format(select_y)
        lr = LinearRegression(featuresCol='features', labelCol='{}'.format(select_y), maxIter=1000, fitIntercept=True)
        pipeline = Pipeline(stages=[assembler_for_lag, lr])
        model_val = pipeline.fit(dataFrame)
        predictions = model_val.transform(dataFrame)
        predictions.show(5)
        print("================{}===============".format(select_y))
        print(model_val.stages[1].coefficients)
        print(model_val.stages[1].intercept)
        print(model_val.stages[1].summary)
        trainingSummaries.append(model_val.stages[1].summary)
        model_val.stages[1].summary.residuals.show(10)
        print(model_val.stages[1].summary.residuals.count())
        residual_val = np.array(model_val.stages[1].summary.residuals.select('residuals').collect()).flatten()
        #         print(residual_val)
        a[model_key] = model_val
        b[res_key] = residual_val
        #         print(b)
        lrModels.append(model_val)
        resids.append(residual_val)

    result_len = len(lrModels[0].stages[1].coefficients)
    #     print(result_len)

    cov_resids = np.cov(resids)
    print("cov_resids is")
    print(cov_resids)

    # degree of freedom of cov_resids
    obs = ys_lagged.count()
    # print("obs is")
    # print(obs)
    df_cov_resids = obs - ((len(ys_lagged.columns) + df_len_ori) - df_len_ori)
    # print("len(ys_lagged.columns) is" )
    # print(len(ys_lagged.columns))
    # print("len(ys_lagged.columns) + df_len_ori   is ")
    # print(len(ys_lagged.columns) + df_len_ori)
    # print("df_cov_resids is")
    # print(df_cov_resids)
    # covres is sigma_u
    covres = (cov_resids * (obs - 1)) / df_cov_resids
    print("covres is ")
    print(covres)

    # =====================
    # build VAR result matrix
    print("====start building tsa result mtx====")
    result_mtx = []
    result_len = len(lrModels[0].stages[1].coefficients)
    # print("result length is")
    # print(result_len)
    for b in range(0, df_len_ori):
        result_arr = []
        result_arr.append(lrModels[b].stages[1].intercept)
        for a in range(0, result_len):
            result_arr.append(lrModels[b].stages[1].coefficients[a])
        result_mtx.append(result_arr)
    result_mtx = np.array(result_mtx)
    print(result_mtx)

    # print(result_mtx)
    # print("========len(result_mtx)=======")
    # print(len(result_mtx))
    # print("===============")
    # dataFrame.unpersist()
    print("now I'm building the return list :)")
    print("first return value is result_mtx, which is ")
    print(result_mtx)
    # print("dataFrame_names is ")
    # print(dataFrame_names)
    # print("df_len_ori")
    # print(df_len_ori)
    # print("current_lag")
    # print(current_lag)
    # print("ys_lagged")
    # print(ys_lagged)
    # print(result_mtx, dataFrame_names, df_len_ori, current_lag, ys_lagged, covres, df_cov_resids, ys_lagged_len)
    return result_mtx, dataFrame_names, df_len_ori, current_lag, ys_lagged, covres, df_cov_resids, ys_lagged_len


# varResultList = fitVar(p_lag_input, var_caused_input, var_causing_input, "causality")


def get_index(lst, name):
    try:
        result = lst.index(name)
    except Exception:
        if not isinstance(name, (int, int)):
            raise
        result = name
    return result


def test_causality(caused, causing, kind='f', signif=0.05):
    if not (0 < signif < 1):
        raise ValueError("signif has to be between 0 and 1")

    allowed_types = (string_types, int)

    if isinstance(caused, allowed_types):
        caused = [caused]
    if not all(isinstance(c, allowed_types) for c in caused):
        raise TypeError("caused has to be of type string or int (or a "
                        "sequence of these types).")

    inputList = fitVar(p_lag_input, caused, causing, "causality")

    result_mtx = inputList[0]
    dataFrame_names = inputList[1]
    df_len_ori = inputList[2]
    p_lag = inputList[3]
    ys_lagged = inputList[4]
    covres = inputList[5]
    df_cov_resids = inputList[6]
    test_causality_res = []

    print(dataFrame_names[0])
    caused = [dataFrame_names[c] if type(c) == int else c for c in caused]
    caused_ind = [get_index(dataFrame_names, c) for c in caused]

    if causing is not None:
        if isinstance(causing, allowed_types):
            causing = [causing]
        if not all(isinstance(c, allowed_types) for c in causing):
            raise TypeError("causing has to be of type string or int (or "
                            "a sequence of these types) or None.")
        causing = [dataFrame_names[c] if type(c) == int else c for c in causing]
        causing_ind = [get_index(dataFrame_names, c) for c in causing]

    if causing is None:
        causing_ind = [i for i in range(df_len_ori) if i not in caused_ind]
        causing = [dataFrame_names[c] for c in caused_ind]

    k = df_len_ori
    p = p_lag
    num_det_terms = 1
    num_restr = len(causing) * len(caused) * p
    C = np.zeros((num_restr, k * num_det_terms + k ** 2 * p), dtype=float)
    cols_det = k * num_det_terms
    row = 0

    for j in range(p):
        for ing_ind in causing_ind:
            for ed_ind in caused_ind:
                C[row, cols_det + ed_ind + k * ing_ind + k ** 2 * j] = 1
                row += 1

    Cb = np.dot(C, vec(result_mtx))
    # variance-covariance of model coefficients
    z = np.array(ys_lagged.collect())
    cov_params = np.kron(scipy.linalg.inv(np.dot(z.T, z)), covres)
    middle = scipy.linalg.inv(chain_dot(C, cov_params, C.T))
    lam_wald = chain_dot(Cb, middle, Cb)
    statistic = lam_wald / num_restr
    df = (num_restr, k * df_cov_resids)
    dist = stats.f(*df)
    pvalue = dist.sf(statistic)
    # crit_value = dist.ppf(1 - signif)

    print("==================result==================")
    print("pvalue is")
    print(pvalue)
    print("Test statistic is ")
    print(statistic)
    print("H0 is %s (causing) does not cause %s (caused)" % (causing, caused))
    if pvalue >= 0.05:
        print("fail to reject H0, p-value > 0.05")
    else:
        print("reject H0, p-value < 0.05")
        test_causality_res = [caused[0], causing[0], pvalue]


    return test_causality_res


# AIC = 2k + n Log(RSS/n)
# k = df_len_ori, RSS, n  → RSS?
# p.146
def info_criteria(ys_lagged_len, k, lag, sigma_u):
    # "information criteria for lagorder selection"
    # nobs = ys_lagged_len - plag
    print("hello here's info_criteria :)")
    nobs = ys_lagged_len
    print("nobs is ")
    print(nobs)
    # neqs is k
    neqs = k
    print("neqs is")
    print(neqs)
    # lag_order = k.ar
    lag_order = lag
    print("lag is")
    print(lag_order)
    k_exog = 1
    free_params = lag_order * neqs ** 2 + neqs * k_exog
    print("free_params is")
    print(free_params)

    df_resid = ys_lagged_len - (neqs * lag_order + 1)

    print("df_resid is")
    print(df_resid)

    # sigma_u_mle = self.sigma_u * self.df_resid / self.nobs
    print("sigma_u is")
    print(sigma_u)
    # error in computing mle
    sigma_u_mle = sigma_u * df_resid / nobs
    print("sigma_u_mle is ")
    print(sigma_u_mle)

    ld = logdet_symm(sigma_u_mle)
    # print("ld is ")
    # print(ld)

    # See Lutkepohl pp. 146-150

    df_model = nobs - df_resid

    aic = ld + (2. / nobs) * free_params
    bic = ld + (np.log(nobs) / nobs) * free_params
    # hqic = ld + (2. * np.log(np.log(nobs)) / nobs) * free_params
    # fpe = ((nobs + df_model) / df_resid) ** neqs * np.exp(ld)

    print("lag is")
    print("aic is")
    print(aic)
    print("bic is")
    print(bic)
    # print("hqic is")
    # print(hqic)
    # print("fpe is")
    # print(fpe)

    return {
        'aic': aic,
        'bic': bic
        # 'hqic': hqic,
        # 'fpe': fpe
    }


# def lag_selection(maxlag):
#     # to do: loop through each p_lag, fit model and get these 4 variables, put into info_criteria and print out all the
#     # results.
#
#     ics = defaultdict(list)
#     for p_lag_loop in range(0, maxlag + 1):
#         print("current lag is ")
#         print(p_lag_loop)
#         lag_selection_input_list = fitVar(p_lag_loop, var_caused_input, var_causing_input, "order_selection")
#         print("hello here's lag_selection, fitVar result will be printed")
#         k = lag_selection_input_list[2]
#         lag = lag_selection_input_list[3]
#         sigma_u = lag_selection_input_list[5]
#         ys_lagged_len = lag_selection_input_list[7]
#
#         for k, v in iteritems(info_criteria(ys_lagged_len, k, lag, sigma_u)):
#             ics[k].append(v)
#
#     selected_orders = dict((k, np.array(v).argmin() + 1)
#                            for k, v in iteritems(ics))
#
#     # need to compare these 4 info criteria and return the best one
#     # current: return all 4 info criteria results under each p lag value
#     print(selected_orders)
#     print("ics")
#     print(ics)
#     return selected_orders
#

### TODO: get (var_caused_input, var_causing_input) all causality test results

input_list = list(itertools.permutations(x_list, 2))
print(input_list)

pool = ThreadPool(thread_pool_num)
result = pool.map(lambda iter_item: test_causality(iter_item[0], iter_item[1]), input_list)
print(result)

with open("out.csv", "w", newline='') as f:
    for row in result:
        f.write("%s\n" % ','.join(str(col) for col in row))

with open('out.csv') as input, open('output.csv', 'w', newline='') as output:
    non_blank = (line for line in input if line.strip())
    output.writelines(non_blank)

# fitVar(p_lag, var_caused, var_causing, data_file_name, call_method = "causality")

# test_causality(var_caused_input, var_causing_input)

# lag_selection(5)
print(datetime.now() - startTime)

# spark.stop()