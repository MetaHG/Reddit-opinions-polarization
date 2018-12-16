


function plot_subreddit_agreement(elem) {

  Plotly.d3.csv("assets/data/subreddit_metrics.csv", function(err, rows){

    function unpack(rows, key) {
      return rows.map(function(row) { return row[key]; });
    }

    Plotly.d3.csv("assets/data/subreddit_metrics_names.csv", function(err, names){

      function unpack(names, key) {
        return names.map(function(row) { return row[key]; });
      }

      var layout = {
          "clickmode": "none",
          "autosize": true,
          "yaxis": {
              "range": [
                  0.09821387045066789,
                  0.17205671390263505
              ],
              "type": "linear",
              "autorange": true,
              "title": "Positivity"
          },
          "showlegend": false,
          "xaxis": {
              "title": "Agreement Score",
              "showgrid": true,
              "range": [
                  0.72,
                  1.0
              ],
              "fixedrange": true,
              "type": "linear",
              // "autorange": true
          },
          "hovermode": "closest",
          "margin": {
              // "r": 80,
              "pad": 0,
              "t": 20
          }
      };

      var data = [{
          "textposition": "top center",
          "ysrc": "jfperren:4:c62fc7",
          "hoverinfo": "text",
          "xsrc": "jfperren:4:db3f6b",
          "textsrc": "jfperren:4:f079b4",
          "text": unpack(rows, 'subreddit'),
          "y": unpack(rows, 'pos'),
          "x": unpack(rows, 'agreement_factor'),
          "type": "scatter",
          "mode": "markers"
        },{
          "textposition": "top center",
          "showlegend": false,
          "ysrc": "jfperren:3:70600b",
          "hoverinfo": "none",
          "xsrc": "jfperren:3:8d5a96",
          "marker": {
              "color": "rgb(0, 0, 0)",
              "opacity": 0,
              "maxdisplayed": 0
          },
          "stackgroup": null,
          "textsrc": "jfperren:3:10257c",
          "text": unpack(names, 'subreddit'),
          "y": unpack(names, 'pos'),
          "x": unpack(names, 'agreement_factor'),
          "type": "scatter",
          "hoveron": "points",
          "mode": "markers+text"
      }]

      Plotly.newPlot(elem, data, layout, {responsive: true, displayModeBar: false});
    });
  });
}
