
// Booking
$(document).ready(function () {
	
		       var categories = [],
                   points = [],
				   Duplicate = [],
				   No_of_Market = [],
				   Affected_time = [];
		   {% for eachResult in user %}
		   var d = new Date({{ eachResult['Pre_Approval_Date'] }} *1000);
		   categories.push(d.getFullYear()+"-"+d.getMonth()+"-"+d.getDate());
		   points.push({{ eachResult['Raw_count'] }});
		   Duplicate.push({{ eachResult['Duplicate'] }});
		   No_of_Market.push({{ eachResult['No_of_Market'] }});
		   Affected_time.push({{ eachResult['Affected_time'] }});
		   {% endfor %}
			
		   var options = {
               chart: {
                   renderTo: 'Sale',
                   type: 'spline'
               },
               xAxis: {},
               series: [{}],
			     title: {
						text: 'Daily basis of Booking raw data row count'
					},

					subtitle: {
						text: 'Built chart in ...' // dummy text to reserve space for dynamic subtitle
					},
					
				plotOptions: {
					spline: {
						marker: {
							enabled: true
						}
					}
				},rangeSelector: {

            buttons: [{
                type: 'day',
                count: 3,
                text: '3d'
            }, {
                type: 'week',
                count: 1,
                text: '1w'
            }, {
                type: 'month',
                count: 1,
                text: '1m'
            }, {
                type: 'month',
                count: 6,
                text: '6m'
            }, {
                type: 'year',
                count: 1,
                text: '1y'
            }, {
                type: 'all',
                text: 'All'
            }],
            selected: 3
        },
           };
               options.xAxis.categories = categories;
			options.tooltip = {
					crosshairs: true,
					shared: true
				},	
			   options.series = [{
			    name: 'Row counts',
				marker: {
					symbol: 'square'
				},
				data: points
			   },{
				 name: 'Duplicate',
				marker: {
					symbol: 'diamond'
				},
				data: Duplicate
			   },{
			    name: 'No of Market',
				marker: {
					symbol: 'square'
				},
				data: No_of_Market
			   },{
				 name: 'Affected_time',
				marker: {
					symbol: 'diamond'
				},
				data: Affected_time
			   }
			   
			   ]
			    
               var chart = new Highcharts.Chart(options);
       });

