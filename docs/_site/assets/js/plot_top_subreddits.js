
function plot_top_subreddits(elem) {

  Plotly.d3.csv("assets/data/top_subreddits.csv", function(err, rows){

    function unpack(rows, key) {
      return rows.map(function(row) { return row[key]; });
    }

    console.log(unpack(rows, 'subreddit'))

    var data = [{
      type: "bar",
      mode: "lines",
      orientation: "h",
      hoverinfo: "x",
      transforms: [{
          type: "sort"
      }],
      x: unpack(rows, 'count'),
      y: unpack(rows, 'subreddit'),
      marker: {
        color: [
          '#8CC17D',
          '#87A0CB',
          '#87A0CB',
          '#F6CA9B',
          '#F6CA9B',
          '#87A0CB',
          '#87A0CB',
          '#F6CA9B',
          '#F6CA9B',
          '#F6CA9B'
        ]
      },
      bar: {
        color: '#ffb347'
      }
    }]

    var layout = {
        autosize: true,
        font: { size: 12 },
        title: "Top 10 Most Active Subreddits",
        yaxis: {
            range: [ -0.5, 9.5 ],
            type: "category",
            autorange: true,
            title: "Subreddit",
            fixedrange: true
        },
        xaxis: {
            range: [ 0, 2269276.8421052634 ],
            type: "linear",
            autorange: true,
            title: "Total Count of Messages",
            fixedrange: true
        },
        hovermode: "y",
        bargroupgap: 0.48,
        margin: {
            pad: 10,
            r: 120,
            b: 80,
            l: 120
        }
    }

    Plotly.plot(elem, {
      data: data,
      layout: layout,
    });
  });
}
