
function plot_daily_nlp(elem) {

  Plotly.d3.csv("assets/data/neg_vs_pos_daily_avg.csv", function(err, rows){

    function unpack(rows, key) {
      return rows.map(function(row) { return row[key]; });
    }

    var data = [{
      type: "scatter",
      mode: "lines",
      name: 'Positivity',
      x: unpack(rows, 'creation_date'),
      y: unpack(rows, 'nltk_positivity_60d_avg'),
      line: {
        color: '#F6CA9B'
      }
    }, {
      type: "scatter",
      mode: "lines",
      name: 'Negativity',
      x: unpack(rows, 'creation_date'),
      y: unpack(rows, 'nltk_negativity_60d_avg'),
      line: {
        color: '#87A0CB'
      }
    }];

    var layout = {
        autosize: true,
        titlefont: { size: 10 },
        hovermode: "x",
        bargroupgap: 0.48,
        margin: {
          pad: 10,
          r: 40,
          b: 40,
          l: 40,
          t: 0
        },
        legend: {
          xanchor: "right",
          x: 1,
          yanchor: "center",
          y: 0.5
        }
    };

    Plotly.newPlot(elem, data, layout, {responsive: true, displayModeBar: false});
  })
}
