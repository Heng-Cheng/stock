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

## Data cleaning

investors_merge.py -> Stock daily traiding price & daily trading volumes of investors & others data<br>
![image](https://imgur.com/ih40c0S.jpg)<br><br>
![image](https://imgur.com/HzdxWQ8.jpg)<br><br>
margin_clean.py -> Short Sales volume & margin transaction<br>
![image](https://imgur.com/rMaXYR4.jpg)<br><br>
![image](https://imgur.com/PeapOD0.jpg)<br><br>
Technical_Indicator_class.py -> Merging data & calculating technical indicator of stock price and volume & lag variables
![image](https://imgur.com/9EAd5wm.jpg)<br><br>


[GAN](https://colab.research.google.com/drive/12a7IUkQMqzoK_oEWKnAVrGQSiWpbQ_vc)
