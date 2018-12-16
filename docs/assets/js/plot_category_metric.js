
function plot_category_metric(metric) {

  Plotly.d3.csv("assets/data/category_metrics.csv", function(err, category_metrics){

    Plotly.d3.csv("assets/data/nlp_metrics_distrib.csv", function(err, metrics_distrib){

      function unpack(rows, key) {
        return rows.map(function(row) { return row[key]; });
      }

      var category_metrics_data = [
        {
              "orientation": "v",
              "ysrc": "jfperren:6:837260",
              "xsrc": "jfperren:6:1031a8",
              "marker": {
                  "colorsrc": "jfperren:6:c04ae4",
                  "autocolorscale": false,
                  "cmin": 0.5,
                  "colorscale": [
                      [ 0, "#e6f0f0" ],
                      [ 0.09090909090909091, "#bfdde5" ],
                      [ 0.18181818181818182, "#9cc9e2" ],
                      [ 0.2727272727272727, "#81b4e3" ],
                      [ 0.36363636363636365, "#739ae4" ],
                      [ 0.45454545454545453, "#757fdd" ],
                      [ 0.5454545454545454, "#7864ca" ],
                      [ 0.6363636363636364, "#774aaf" ],
                      [ 0.7272727272727273, "#71328d" ],
                      [ 0.8181818181818182, "#641f68" ],
                      [ 0.9090909090909091, "#501442" ],
                      [ 1, "#360e24" ]
                  ],
                  "color": [
                      "1.5",
                      "2",
                      "2.5",
                      "3"
                  ],
                  "reversescale": false,
                  "cauto": false,
                  "cmax": 5
              },
              "mode": "markers",
              "y": unpack(category_metrics, metric),
              "x": unpack(category_metrics, 'Group'),
              "type": "bar"
          }
      ];

      var category_metrics_layout = {
          "autosize": true,
          // "colorway": [
          //     "#440154",
          //     "#482878",
          //     "#3e4989",
          //     "#31688e",
          //     "#26828e",
          //     "#1f9e89",
          //     "#35b779",
          //     "#6ece58",
          //     "#b5de2b",
          //     "#fde725"
          // ],
          "xaxis": {
              "range": [
                  -0.5,
                  3.5
              ],
              "type": "category",
              "autorange": true
          },
          "yaxis": {
              "range": [
                  0,
                  0.16842105263157894
              ],
              "type": "linear",
              "autorange": true
          },
          margin: {
            pad: 10,
            r: 40,
            b: 40,
            l: 40,
            t: 20
          }
      };

      metric_distrib_layout = {
          "colorway": [
              "#440154",
              "#482878",
              "#3e4989",
              "#31688e",
              "#26828e",
              "#1f9e89",
              "#35b779",
              "#6ece58",
              "#b5de2b",
              "#fde725"
          ],
          "yaxis": {
              "range": [
                  0,
                  0.07537799849244002
              ],
              "type": "linear",
              "autorange": true
          },
          "autosize": true,
          "barmode": "group",
          "barnorm": "",
          "xaxis": {
              "range": [
                  9.974659986866641e-18,
                  0.3720000000000413
              ],
              "type": "linear",
              "autorange": true,
              "fixedrange": true
          },
          "showlegend": false,
          margin: {
            pad: 10,
            r: 40,
            b: 40,
            l: 40,
            t: 20
          }
      }

      switch(metric) {
        case 'agreement':
          xbins = {
              "start": 0.5,
              "end": 1.0,
              "size": 0.5 / 50
          }
          break;
        case 'positivity':
        xbins = {
          "start": 0.0,
          "end": 0.4,
          "size": 0.4 / 50
        }
        break;
        case 'negativity':
        xbins = {
            "start": 0.0,
            "end": 0.15,
            "size": 0.15 / 50
        }
        break;
        case 'vulgarity':
        xbins = {
            "start": 0,
            "end": 0.5,
            "size": 0.5 / 50
        }
        break;
      }

      metric_distrib_data = [
          {
              "ysrc": "jfperren:8:b6af61",
              "xbins": xbins,
              "xsrc": "jfperren:8:b6af61",
              "histnorm": "probability",
              "mode": "markers",
              "hoverinfo": "x+y",
              "y": unpack(metrics_distrib, metric),
              "x": unpack(metrics_distrib, metric),
              "type": "histogram",
              "marker": {
                  color: '#F6CA9B',
                  // "autocolorscale": true,
                  // "colorsrc": "jfperren:8:99dba3",
                  // "colorscale": [
                  //     [
                  //         0,
                  //         "#440154"
                  //     ],
                  //     [
                  //         1,
                  //         "#482878"
                  //     ],
                  //     [
                  //         2,
                  //         "#3e4989"
                  //     ],
                  //     [
                  //         3,
                  //         "#31688e"
                  //     ],
                  //     [
                  //         4,
                  //         "#26828e"
                  //     ]
                      // [
                      //     0.5555555555555556,
                      //     "#1f9e89"
                      // ],
                      // [
                      //     0.6666666666666666,
                      //     "#35b779"
                      // ],
                      // [
                      //     0.7777777777777778,
                      //     "#6ece58"
                      // ],
                      // [
                      //     0.8888888888888888,
                      //     "#b5de2b"
                      // ],
                      // [
                      //     1,
                      //     "#fde725"
                      // ]
                  // ],
                  // "color": unpack(rows, 'pos'),
                  // "color": [
                      // 1,2,3,4,5
                  // ],
                  // "coloragg": "mean",
                  // "reversescale": false,
                  // "colorbar": {
                  //     "y": 0.5,
                  //     "x": 1.02
                  // },
                  // "cauto": false,
                  "showscale": false
              }
          }
      ]

      switch(metric) {
        case 'agreement':
          text = "Our first metric aims to measure the extent to which people participating in a discussion are in agreement with each other. \
            Its calculation is fairly simple as it corresponds to the percentage of comments with a positive score.";
          break;
        case 'positivity':
          text = "Next, we aimed to measure how positive each one of the available comments are. \
            In order to do this, we used a dataset of NLTK positive words that we matched against the content of each comment.";
          break;
        case 'negativity':
          text = "Similarly to the positivity metric, we defined a negativity metric using another dataset provided by NLTK. \
            Note that in certain cases it makes sense to combine these two metrics, which is referred below as 'Polarity'."
          break;
        case 'vulgarity':
          text = "Finally, we also matched each comment against a database of known insults and hate words in order to get a measure of vulgarity. \
            It is to be noted that this metric does not take into account the intensity of an insult."
          break;
      }

      document.getElementById("metric-description").innerHTML = text;

      metric_list_items = document.getElementById("metric-list").childNodes;

      for (i = 0; i < metric_list_items.length; i++) {
        metric_list_items[i].className = "horizontal-item"
      }

      document.getElementById('metric-' + metric).className = "horizontal-item-select"

      Plotly.newPlot('plot-category-metric', category_metrics_data, category_metrics_layout, {responsive: true, displayModeBar: false});
      Plotly.newPlot('plot-metric-distrib', metric_distrib_data, metric_distrib_layout, {responsive: true, displayModeBar: false});
    });
  });
}
