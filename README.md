# Deep Learning approach for stock price prediction

## Why use deep learning?
Deep learning have been applied to many fields. e.g. computer vision, speech recognition, NLP, etc.<br><br>

In traditional statistical models. Linear regression can explain the trend of data very well but it’s poor at predicting. Instead, deep learning is a good choice for stock price prediction.<br><br>

## Our Goal: Automated trading system
Step1: Understand what are the most crucial factors for stock prediction.<br><br>
Step2: Crawl the necessary data from TWSE website. e.g. daily stock price, foreign or other investors trade.<br><br>
Step3: Data preprocessing.<br><br>
Step4: Build model.<br><br>
Step5: Deploy the result of prediction on app.<br><br>

## Web Crawler

We choose 50 stock collected them from TWSE that included daily price, margin short sale, daily trading volume of Investors. In addition, we also collected futures and foreign stock market index data from other website.<br><br>

stock50.py -> Download 50 classes of stock in Taiwan from TWSE<br>
![image](https://imgur.com/NlVU1rk.jpg)<br><br>
crawler_investor.py -> Download stock trading of investors & others from TWSE<br>
![image](https://imgur.com/A2w8fMk.jpg)<br><br>
crawler_margin.py -> Download daily margin volumes from TWSE<br>
![image](https://imgur.com/ddQeHPw.jpg)<br><br>
crawler_margin_financing.py -> Download daily margin balance from TWSE<br>
![image](https://imgur.com/XJzHJjv.jpg)<br><br>

## Data Data preprocessing

1.Transform the raw data to daily unit data for 50 class of stock.<br><br>
2.Features selection and define technical indicator for stock prediction. e.g. EMA, KD, BBANDS, RSI, etc.<br><br>
3.Fulfill the missing data, Merge different tables by date.<br><br>

investors_merge.py -> Stock daily traiding price & daily trading volumes of investors & others data<br>
![image](https://imgur.com/ih40c0S.jpg)<br><br>
![image](https://imgur.com/HzdxWQ8.jpg)<br><br>
margin_clean.py -> Short Sales volume & margin transaction<br>
![image](https://imgur.com/rMaXYR4.jpg)<br><br>
![image](https://imgur.com/PeapOD0.jpg)<br><br>
Technical_Indicator_class.py -> Merging data & calculating technical indicator of stock price and volume & lag variables
![image](https://imgur.com/9EAd5wm.jpg)<br><br>

## Model selection

In our case, we use RNN or CNN that can deal with time series data.<br><br>

WHY RNN? ->If daily price is related to the price several days ago. recurrent neural network is a good choice. It can remember the pattern of several days and predict next day.<br><br>
![image](https://imgur.com/tHShXki.jpg)<br><br>

WHY CNN? ->convolutional neural network is usually used to apply in computer vision. It can filter neighboring area of image. So we can treat different time steps as neighboring area and use CNN to model.<br><br>
![image](https://imgur.com/Bt0M8ou.jpg)<br><br>

## Build GAN

Image there are two investment expert. One predicts the price next day(generator), and the other will distinguish real or fake which it is(discriminator).<br><br>

We use LSTM as generator to predict time-series, and use one dimension CNN with UNET structure as discriminator.<br><br>
![image](https://imgur.com/fM3Wx3c.jpg)<br><br>

[GAN implementation in here!](https://colab.research.google.com/drive/12a7IUkQMqzoK_oEWKnAVrGQSiWpbQ_vc)

## GAN result
![image](https://imgur.com/gBp83nK.jpg)<br><br>

## Convert idea-Autoencoder
If there were some abnormal price existed in history data. Can we find it?<br><br>

Suppose higher stock price change can be found. Perhaps we can try to use Autoencoder for anomaly detection.<br><br>

The output from Autoencoder is as near as input. We can treat ‘normal’ data as training data in Autoencoder. Then it will only recognize the pattern in normal data.<br><br>

After training, we treat abnormal data as testing data that may probably occur higher error. Then we can prescribe a threshold to find out abnormal points.<br><br>

[Autoencoder implementation in here!](https://colab.research.google.com/drive/1SXfjrI_w8Weg5JRiJC0p__cbHuWNw40N)

![image](https://imgur.com/FbTWzyb.jpg)<br><br>

Unfortunately, We didn’t separate to three mountains that represent high increase, normal and high decrease day respectively.<br><br>

![image](https://imgur.com/QAbbXP4.jpg)<br><br>
![image](https://imgur.com/Y8C7whP.jpg)<br><br>
