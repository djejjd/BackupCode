<template>
  <div id="id" :style="{height:height,width:width}"></div>
</template>

<script>
// import { getDataKMeansChart } from "@/api/get-chart";
import resize from './mixins/resize'
import echarts from "echarts";

export default {
  mixins: [resize],
  props: {
    className: {
      type: String,
      default: 'chart'
    },
    id: {
      type: String,
      default: 'chart'
    },
    width: {
      type: String,
      default: '200px'
    },
    height: {
      type: String,
      default: '200px'
    }
  },
  data() {
    return {
      list: null,
      KMeansQuery: {

      }
    }
  },
  created() {
    this.fetchData()
  },
  methods: {
    fetchData() {
      // getDataKMeansChart(this.KMeansQuery).then(response => {
      //   console.log("---------------------------------------")
      //   console.log(response.data)
      //   this.updateChart(response.data)
      // })
    },
    // updateChart(chartData) {
    //   this.chart = echarts.init(document.getElementById(this.id))
    //
    //   let data = [
    //     [3.275154, 2.957587],
    //     [2.682252, 4.007573],
    //     [3.087776, 2.878713],
    //   ];
    //
    //   let CLUSTER_COUNT = 3;
    //   let DIENSIION_CLUSTER_INDEX = 2;
    //   let COLOR_ALL = [
    //     '#37A2DA', '#e06343', '#96BFFF'
    //   ];
    //   let pieces = [];
    //   for (let i = 0; i < CLUSTER_COUNT; i++) {
    //     pieces.push({
    //       value: i,
    //       label: 'cluster ' + i,
    //       color: COLOR_ALL[i]
    //     });
    //   }
    //
    //   this.chart.setOption({
    //     tooltip: {
    //       position: 'top'
    //     },
    //     visualMap: {
    //       type: 'piecewise',
    //       top: 'middle',
    //       min: 0,
    //       max: CLUSTER_COUNT,
    //       left: 10,
    //       dimension: DIENSIION_CLUSTER_INDEX,
    //       pieces: pieces
    //     },
    //     grid: {
    //       left: 120
    //     },
    //     xAxis: {
    //     },
    //     yAxis: {
    //     },
    //     series: {
    //       type: 'scatter',
    //       encode: { tooltip: [0, 1] },
    //       symbolSize: 15,
    //       itemStyle: {
    //         borderColor: '#555'
    //       },
    //       data: [
    //         [3.275154, 2.957587],
    //         [2.682252, 4.007573],
    //         [3.087776, 2.878713]
    //       ]
    //     }
    //   })
    // }
  }
}
</script>
