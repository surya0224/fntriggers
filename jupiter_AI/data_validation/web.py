from flask import Flask, redirect, url_for, request,render_template,redirect
from flask import session
import data as engClass
from datetime import datetime, timedelta
import calendar

# get the current date
# date formet should be YYYY-MM-DD so use .strftime('%Y-%m-%d')
toDay = datetime.now()
yesterDay = toDay -  timedelta(1)

## There is no data for current so for statically we can check old data so
## giving the old date also
toDay = datetime.now() - timedelta(99)
yesterDay = toDay -  timedelta(1)

app = Flask(__name__)
app.secret_key = 'some_secret'

@app.route("/data/<value>",methods = ['POST', 'GET'])
def getPage(value):

   print(value)
   if request.method == 'POST':
      date_result = request.form.getlist('date')
      pos_result = request.form.getlist('pos')
      airline_result = request.form.getlist('airline')
      od_result = request.form.getlist('od')
      
      ## Dynamic parameter from user side
      _dataParameter = dict()
      _dataParameter['typeof'] = value
      _dataParameter['pos'] = pos_result[0]
      _dataParameter['trxDate'] = date_result[0]
      _dataParameter['depDate'] = date_result[1]
      _dataParameter['airline'] = airline_result[0]
      _dataParameter['od'] = od_result[0]
      print(_dataParameter)
      gridData = engClass.query_for_data_validation(_dataParameter)
      returnResult = engClass._getData(value)
      _combineData ={'user':returnResult,'source':value,'gridData':gridData} 

      return render_template("index-screen.html",user = _combineData)
      #return render_template("result.html", result=result)

   ## Data for loading chart data
   returnResult = engClass._getData(value)

   ## Defaulf value for static
   _dataParameter = dict()
   _dataParameter['typeof'] = value
   _dataParameter['pos'] = 'MAA'
   _dataParameter['trxDate'] = yesterDay.strftime('%Y-%m-%d')
   _dataParameter['depDate'] = toDay.strftime('%Y-%m-%d')
   _dataParameter['airline'] = 'FZ'
   _dataParameter['od'] = 'DXBMAA'
   print(_dataParameter)
   gridData = engClass.query_for_data_validation(_dataParameter)
   _combineData ={'user':returnResult,'source':value,'gridData':gridData} 
   return render_template("index-screen.html",user = _combineData)



@app.route('/',methods = ['POST', 'GET'])
def login():
   if request.method == 'POST':
      date_result = request.form.getlist('date')
      pos_result = request.form.getlist('pos')
      airline_result = request.form.getlist('airline')
      od_result = request.form.getlist('od')
      ## Dynamic parameter from user side
      _dataParameter = dict()
      _dataParameter['typeof'] = value
      _dataParameter['pos'] = pos_result[0]
      _dataParameter['trxDate'] = date_result[0]
      _dataParameter['depDate'] = date_result[1]
      _dataParameter['airline'] = airline_result[0]
      _dataParameter['od'] = od_result[0]
      print(_dataParameter)
      returnResult = engClass._getData(value)
      gridData = engClass.query_for_data_validation(_dataParameter)
      _combineData ={'user':returnResult,'source':value,'gridData':gridData} 
      return render_template("index-screen.html",user = _combineData)

   ## Data for loading chart data
   returnResult = engClass._getData("Booking")
   
   ## Defaulf value for static 
   _dataParameter = dict()
   _dataParameter['typeof'] = "Booking";
   _dataParameter['pos'] = 'MAA'
   _dataParameter['trxDate'] = yesterDay.strftime('%Y-%m-%d')
   _dataParameter['depDate'] = toDay.strftime('%Y-%m-%d')
   _dataParameter['airline'] = 'FZ'
   _dataParameter['od'] = 'DXBMAA'
   print(_dataParameter)
   gridData = engClass.query_for_data_validation(_dataParameter)
   #gridData = engClass.query_for_data_validation("Booking")
   #print(returnResult)
   _combineData_ = {'user':returnResult,'source':"Booking",'gridData':gridData} 
   return render_template("index-screen.html",user = _combineData_)

if __name__ == '__main__':
    #app.run(debug = True)
    app.run()
