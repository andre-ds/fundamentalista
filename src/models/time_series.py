from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.seasonal import STL

result = seasonal_decompose(dataset['RECEITA_DE_VENDAS'], period=3)

def seasonal_decompose_graph(y1,y2,y3,cor1,cor2,cor3,label1,label2,label3):
    plt.figure(figsize=(20,7))
    plt.plot(y1,color=cor1,label=label1)
    plt.plot(y2,color=cor2,label=label2)
    plt.plot(y3,color=cor3,label=label3)
    plt.legend(fontsize=18)
    plt.show()

seasonal_decompose_graph(y1=result.observed,
                         y2 = result.trend,
                         y3 = result.seasonal, 
                         cor1 = 'red',
                         cor2='blue',
                         cor3='green',
                         label1='observed',
                         label2='Trend',
                         label3='Seasonal')
'

dataset.shape
autocorrelation_plot(dataset['RECEITA_DE_VENDAS'])
plt.show()