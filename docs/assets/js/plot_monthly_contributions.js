
function plot_monthly_contributions(elem) {

  Plotly.d3.csv("assets/data/monthly_contributions.csv", function(err, rows){

    function unpack(rows, key) {
      return rows.map(function(row) { return row[key]; });
    }

    var trace1 = {
      type: "scatter",
      mode: "lines",
      // name: 'Daily Active Users (60d avg)',
      x: unpack(rows, 'date'),
      y: unpack(rows, 'avg_nb_sub'),
      line: {
        color: '#87A0CB'
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
        autosize: true,
        // font: { size: 12 },
        titlefont: { size: 10 },
        title: "Monthly Active Subreddits per User",
        hovermode: "x",
        bargroupgap: 0.48,
        margin: {
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
