
function plot_daily_agreement(elem) {

  Plotly.d3.csv("assets/data/daily_agreement.csv", function(err, rows){

    function unpack(rows, key) {
      return rows.map(function(row) { return row[key]; });
    }

    var trace1 = {
      type: "scatter",
      mode: "lines",
      // name: 'Daily Active Users (60d avg)',
      x: unpack(rows, 'created'),
      y: unpack(rows, 'daily_agreement_60d_avg'),
      line: {
        color: '#F6CA9B'
      }
    }

    var data = [trace1];

    var layout = {
        autosize: true,
        titlefont: { size: 10 },
        title: "Daily Agreement (All communities)",
        hovermode: "x",
        bargroupgap: 0.48,
        margin: {
          pad: 10,
          r: 40,
          b: 40,
          l: 40,
          t: 20
        },
        yaxis: {
            range: [ 0.8, 1 ],
            type: "linear"
        },
    };

    Plotly.newPlot(elem, data, layout, {responsive: true, displayModeBar: false});
  })
}
