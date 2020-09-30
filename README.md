# Time-Series-Forecasting
**Abstract—** *The rise of the Internet of Things (IoT) devices and the streaming platform has tremendously increased the data in motion or streaming data. It incorporates a wide variety of data, for example, social media posts, online gamers in-game activities, mobile or web application logs, online e-commerce transactions, financial trading, or geospatial services. Accurate and efficient forecasting based on real-time data is a critical part of the operation in areas like energy & utility consumption, healthcare, industrial production, supply chain, weather forecasting, financial trading, agriculture, etc. Statistical time series forecasting methods like Autoregression (AR), Auto- Regressive moving average (ARIMA), and Vector Autoregression (VAR), face the challenge of concept drift in the streaming data, i.e., the properties of the stream may change over time. The other challenge is the efficiency of the system to update the Machine Learning (ML) models which are based on these algorithms to tackle the concept drift. In this paper, we propose a novel framework to tackle both of these challenges. The challenge of adaptability is addressed by applying the Lambda architecture to forecast future state based on three approaches simultaneously: batch (historic) data-based prediction, streaming (real-time) data-based prediction, and hybrid prediction by combining the first two. To address the challenge of efficiency, we implement a distributed V AR algorithm on top of the Apache Spark big data platform. To evaluate our framework, we conducted experiments on streaming time series forecasting with three types of data sets of experiments: data without drift, data with gradual drift, data with abrupt drift. The experiments show the differences of our three forecasting approaches in terms of accuracy and adaptability.*
## Our Architecture
![alt text](https://github.com/big-data-lab-umbc/time-series-analysis/blob/master/lambda_architecture.png)
## Content Description
| Directory | Content |
| ----------- | ----------- |
| jars | contains required jarfiles for Apache Kafka  |
| lambda | code all the layers in lambda architecture and custom VAR |
| models | saved VAR models |
| source | contains data with normal, abrupt and gradual drift |
| streaming | code to generate data streams |
| train | code to train the model for batch layer |

### Pre-requistes
-  Apache Kafka 2.11 or above
-  Python 3.6 or above
-  Pyspark 2.4 or above
