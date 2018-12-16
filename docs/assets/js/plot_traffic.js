

function plot_traffic(elem) {

  Plotly.d3.csv("assets/data/daily_traffic.csv", function(err, rows){

    function unpack(rows, key) {
      return rows.map(function(row) { return row[key]; });
    }

    var trace1 = {
      type: "scatter",
      mode: "lines",
      // name: 'Daily Active Users (60d avg)',
      x: unpack(rows, 'created'),
      y: unpack(rows, 'count_of_users_60d_avg'),
      line: {
        color: '#F6CA9B'
      }
    }

    // var trace2 = {
    //   type: "scatter",
    //   mode: "lines",
    //   name: 'Daily Comments (60d avg)',
    //   x: unpack(rows, 'created'),
    //   y: unpack(rows, 'count_of_comments_60d_avg'),
    //   line: {color: '#7F7F7F'}
    // }

    // var data = [trace1,trace2];
    var data = [trace1];

    var layout = {
      font: { size: 10 },
      titlefont: { size: 12 },
      title: 'Daily Comments Posted on Reddit',
      "margin": {
          pad: 10,
          r: 40,
          b: 40,
          l: 40,
          t: 20
      }
    };

    Plotly.newPlot(elem, data, layout, {responsive: true, displayModeBar: false});
  })
}
