from __future__ import print_function
import sys
from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
# from pyspark.sql.functions import lag, col
from pyspark.sql.functions import lag
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id
# from pyspark.sql.functions import lit
import pyspark.sql.functions as f
import numpy as np
from statsmodels.compat.python import (range, lrange, string_types,
                                       StringIO, iteritems, long)
from statsmodels.tsa.tsatools import vec, unvec, duplication_matrix
import scipy.linalg
from statsmodels.tools.tools import chain_dot
import scipy.stats as stats

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("SparkVarTest") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("FATAL")
    np.set_printoptions(8)
    # 4 parameters, VAR type "const"
    # p_lag: time lag
    # p_lag = int(sys.argv[1])
    # var_caused = sys.argv[2].split(',')
    # var_causing = sys.argv[3].split(',')
    # data_file_name = sys.argv[4]

    p_lag = 1
    var_caused = '_cx'
    var_causing = '_cy'
    data_file_name = '/Users/arjunpandya/JupyterNotebooks/var_smpl.csv'


    # Load training data
    dataFrame = spark.read.csv(data_file_name, header=False, inferSchema=True)
    # df_len_ori: number of variables in model, K
    df_len_ori = len(dataFrame.columns)
    print("df_len_ori is ")
    print(df_len_ori)
    dataFrame_names = dataFrame.schema.names
    dataFrame = dataFrame.withColumn("id", monotonically_increasing_id())
    dataFrame.printSchema()

    # Here, VAR model regression_type is "const" same to R VAR library, and the default in Python VAR library
    w = Window().partitionBy().orderBy(f.col("id"))
    df_len = len(dataFrame.columns)
    for i in range(1, p_lag + 1):
        for j in range(0, df_len - 1):
            print("_c%st-%s" % (str(j), str(i)), lag(dataFrame[j], i, 0).over(w))
            dataFrame = dataFrame.withColumn("_c%st-%s" % (str(j), str(i)), lag(dataFrame[j], i, 0).over(w))

    # add "const" column of value 1 to get intercept when fitting the regression model
    dataFrame = dataFrame.withColumn("const", f.lit(1))
    dataFrame = dataFrame.withColumn("const", lag("const", p_lag, 0).over(w))
    for ii in range(0, df_len_ori):
        c_name_ii = "_c%s" % (str(ii))
        dataFrame = dataFrame.withColumn("_c%s-1" % (str(ii)), dataFrame[c_name_ii] - 1)

    # add "rid" column to control rollback process with p
    dataFrame = dataFrame.withColumn("rid", monotonically_increasing_id())
    dataFrame = dataFrame.filter(dataFrame.rid >= p_lag)
    print("====added rid columns ====")

    # build ys_lagged dataframe, will be used in F-test
    ys_lagged_list = ["const"]
    input_feature_name_const = []
    for ji in range(1, p_lag + 1):
        for ij in range(0, df_len_ori):
            ys_lagged_list.append("_c%st-%s" % (str(ij), str(ji)))
            input_feature_name_const.append("_c%st-%s" % (str(ij), str(ji)))
    ys_lagged = dataFrame.select(ys_lagged_list)
    ys_lagged.show()

    # assemble the vector for MLlib linear regression
    assembler_for_lag = VectorAssembler(
        inputCols=input_feature_name_const,
        outputCol="features")

    training_df = assembler_for_lag.transform(dataFrame)

    # fit the VAR models and put them into a list
    lrModels = []
    for select_y in range(0, df_len_ori):
        createVar = locals()
        # With L1 or L2 penalties, configure regParam and elasticNetParam
        # createVar['lrModel_c' + str(select_y)] = LinearRegression(featuresCol='features',
        #                                                           labelCol='_c%s-1' % str(select_y),
        #                                                           maxIter=1000, regParam=0.0, elasticNetParam=1,
        #                                                           fitIntercept=True).fit(training_df)
        # Without L1 and L2 penalties
        createVar['lrModel_c' + str(select_y)] = LinearRegression(featuresCol='features',
                                                                  labelCol='_c%s' % (str(select_y)),
                                                                  maxIter=1000, fitIntercept=True).fit(training_df)
        exec('print("==============c{}===================")'.format(str(select_y)))
        exec('print("Coefficients: %s" % str(lrModel_c{}.coefficients))'.format(str(select_y)))
        exec('print("Intercept: %s" % str(lrModel_c{}.intercept))'.format(str(select_y)))
        # Summarize the model over the training set and print out some metrics
        exec('trainingSummary_{} = lrModel_c{}.summary'.format(str(select_y), str(select_y)))
        exec('print("numIterations: %d" % trainingSummary_{}.totalIterations)'.format(str(select_y)))
        exec('print("labelCol: %s" % trainingSummary_{}.labelCol)'.format(str(select_y)))
        exec('print("objectiveHistory: %s" % str(trainingSummary_{}.objectiveHistory))'.format(str(select_y)))
        exec('trainingSummary_{}.residuals.show()'.format(str(select_y)))
        exec('print("RMSE: %f" % trainingSummary_{}.rootMeanSquaredError)'.format(str(select_y)))
        exec('print("r2: %f" % trainingSummary_{}.r2)'.format(str(select_y)))
        exec('lrModels.append(lrModel_c{})'.format(str(select_y)))

    # =============================================
    # build residual covariance matrix - covres
    # cov(resids) * (obs - 1) / (obs - (ncol(object$datamat) - object$K)) in R
    resids = []
    for covres_i in range(0, df_len_ori):
        create_res_var = locals()
        create_res_var['arr' + str(covres_i)] = np.array(create_res_var['trainingSummary_' + str(covres_i)].residuals
                                                         .select('residuals').collect()).flatten()
        exec('print("==============arr{} is===================")'.format(str(covres_i)))
        exec('print(arr{})'.format(str(covres_i)))
        exec('resids.append(arr{})'.format(str(covres_i)))
        # resids = np.vstack((arr0, arr1))
    # print("resids is")
    # print(resids)
    cov_resids = np.cov(resids)
    # print("cov_resids is")
    # print(cov_resids)
    # degree of freedom of cov_resids
    obs = ys_lagged.count()
    print("obs is")
    print(obs)
    df_cov_resids = obs - ((len(ys_lagged.columns) + df_len_ori) - df_len_ori)
    print("len(ys_lagged.columns) is" )
    print(len(ys_lagged.columns))
    print("len(ys_lagged.columns) + df_len_ori   is ")
    print(len(ys_lagged.columns) + df_len_ori)
    print("df_cov_resids is")
    print(df_cov_resids)
    covres = (cov_resids * (obs - 1)) / df_cov_resids
    # print(covres)

    # =====================
    # build VAR result matrix
    print("====start building tsa result mtx====")
    result_mtx = []
    result_len = len(lrModel_c0.coefficients)
    # result_len = 6
    print("result length is")
    print(result_len)

    # result_len = len(lrModels.coefficients)
    print("result length is")
    print(result_len)
    for b in range(0, df_len_ori):
        result_arr = []
        result_arr.append(round(lrModels[b].intercept, 8))
        for a in range(0, result_len):
            result_arr.append(round(lrModels[b].coefficients[a], 8))
        result_mtx.append(result_arr)
    result_mtx = np.array(result_mtx)
    print(result_mtx)
    print("========len(result_mtx)=======")
    print(len(result_mtx))
    print("===============")

    # get_index function in F test
    def get_index(lst, name):
        try:
            result = lst.index(name)
        except Exception:
            if not isinstance(name, (int, long)):
                raise
            result = name
        return result

    # ====================
    # F-test
    # caused : int or str or sequence of int or str
    # If int or str, test whether the variable specified via this index
    # (int) or name (str) is Granger-caused by the variable(s) specified
    # by `causing`.
    # If a sequence of int or str, test whether the corresponding
    # variables are Granger-caused by the variable(s) specified
    # by `causing`.
    # causing : int or str or sequence of int or str or None, default: None
    # If int or str, test whether the variable specified via this index
    # (int) or name (str) is Granger-causing the variable(s) specified by
    # `caused`.
    # If a sequence of int or str, test whether the corresponding
    # variables are Granger-causing the variable(s) specified by
    # `caused`.
    # If None, `causing` is assumed to be the complement of `caused`.

    def test_causality(caused, causing, kind='f', signif=0.05):
        if not (0 < signif < 1):
            raise ValueError("signif has to be between 0 and 1")

        allowed_types = (string_types, int)

        if isinstance(caused, allowed_types):
            caused = [caused]
        if not all(isinstance(c, allowed_types) for c in caused):
            raise TypeError("caused has to be of type string or int (or a "
                            "sequence of these types).")
        print(dataFrame_names[0])  # ['_c0', '_c1']
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

    test_causality(var_caused, var_causing)