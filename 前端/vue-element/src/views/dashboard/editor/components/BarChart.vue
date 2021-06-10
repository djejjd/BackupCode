<template>
  <div :class="className" v-loading="listLoading" :style="{height:height,width:width}"/>
</template>

<script>
import echarts from 'echarts'
import resize from './mixins/resize'
import { getDataKMeansChart } from "@/api/get-chart";
import * as _ from "@/utils";

require('echarts/theme/macarons') // echarts theme

// const animationDuration = 6000
let columnIndex = null;

export default {

  mixins: [resize],
  props: {
    className: {
      type: String,
      default: 'chart'
    },
    width: {
      type: String,
      default: '100%'
    },
    height: {
      type: String,
      default: '450px'
    }
  },
  data() {
    return {
      chart: null,
      columnValue: 0,
      diseaseCode: "111",
      querySearchKey: null,
      listLoading: false,
      kMeansChartQuery: {
        diseaseCodeKey: ""
      },
    }
  },
  mounted() {
    this.bus.$on('sendDiseaseInfo',  (data) => {
      console.log("获得传送数据");
      this.diseaseCode = data['diseaseCodeKey'];
      this.initChart();
    })
  },
  watch: {
    columnValue(newValue, oldValue) {
      console.log("watch,,,,,,,")
      // this.bus.$emit("changeValuePlace", dataIndex)
    },
    immediate: true
  },
  beforeDestroy() {
    if (!this.chart) {
      return
    }
    this.chart.dispose()
    this.bus.$off('sendDiseaseInfo')
    this.chart = null
  },
  methods: {
    clickChart() {
      this.bus.$emit("changeTableValue", columnIndex)
    },
    initChart() {
      this.listLoading = true
      // this.updateChart(response.data)
      this.kMeansChartQuery.diseaseCodeKey = this.diseaseCode
      getDataKMeansChart(this.kMeansChartQuery).then(response => {
        console.log(response.data)
        this.bus.$emit("sendDiseaseAverageFee", response.data['averageFee'])
        this.bus.$emit("sendUnusualDiseaseInfo", response.data['diseaseInfo'])
        this.updateChart(response.data)
        this.listLoading = false
      })
    },
    updateChart(chartData) {
      this.chart = echarts.init(this.$el, 'macarons')

      let CLUSTER_COUNT = 3;

      let pieces = [];
      // (y_max, yMax]
      pieces.push({
        gt: chartData['line'][1],
        lte: chartData['line'][3],
        label: "异常点",
        color: '#14f505'
      })
      // [yMax, Inf)
      pieces.push({
        gt: chartData['line'][3],
        label: "极端异常点",
        color: '#fa0000'
      })
      // [y_min, y_max]
      pieces.push({
        gte: chartData['line'][0],
        lte: chartData['line'][1],
        label: "正常点",
        color: '#0eeaf6'
      })

      this.chart.setOption({
        tooltip: {
          position: 'top',
          enterable: true,
          formatter: function (data) {
            let dataIndex = data["dataIndex"];
            // this.columnValue = dataIndex
            columnIndex = chartData['diseaseInfo'][dataIndex];
            return "总费用: " +
              (Math.ceil(chartData['diseaseInfo'][dataIndex]["TotalFee"].toString()).toString().replace(/(\d)(?=(?:\d{3})+$)/g, '$1,'))
              + "元";
          }
        },
        visualMap: {
          type: 'piecewise',
          top: 'middle',
          min: 0,
          max: CLUSTER_COUNT,
          left: 5,
          splitNumber: CLUSTER_COUNT,
          dimension: 1,

          pieces: pieces
        },
        grid: {
          left: 120
        },
        xAxis: {
        },
        yAxis: {
        },
        series: {
          type: 'scatter',
          encode: { tooltip: [0, 1] },
          symbolSize: 10,
          // itemStyle: {
          //   borderColor: '#2ACCDB'
          // },
          data: chartData['lengthPoint']
        }
      })
      this.chart.on("click",  () => {
        this.clickChart()
      })
    }
  }
}
</script>


<style lang="scss" scoped>
.panel-group {
  margin-top: 18px;

  .card-panel-col {
    margin-bottom: 32px;
  }

  .card-panel {
    height: 108px;
    cursor: pointer;
    font-size: 12px;
    position: relative;
    overflow: hidden;
    color: #666;
    background: #fff;
    box-shadow: 4px 4px 40px rgba(0, 0, 0, .05);
    border-color: rgba(0, 0, 0, .05);

    &:hover {
      .card-panel-icon-wrapper {
        color: #fff;
      }

      .icon-people {
        background: #40c9c6;
      }

      .icon-message {
        background: #36a3f7;
      }

      .icon-money {
        background: #f4516c;
      }

      .icon-shopping {
        background: #34bfa3
      }
    }

    .icon-people {
      color: #40c9c6;
    }

    .icon-message {
      color: #36a3f7;
    }

    .icon-money {
      color: #f4516c;
    }

    .icon-shopping {
      color: #34bfa3
    }

    .card-panel-icon-wrapper {
      float: left;
      margin: 14px 0 0 14px;
      padding: 16px;
      transition: all 0.38s ease-out;
      border-radius: 6px;
    }

    .card-panel-icon {
      float: left;
      font-size: 48px;
    }

    .card-panel-description {
      float: right;
      font-weight: bold;
      margin: 26px;
      margin-left: 0px;

      .card-panel-text {
        line-height: 18px;
        color: rgba(0, 0, 0, 0.45);
        font-size: 16px;
        margin-bottom: 12px;
      }

      .card-panel-num {
        font-size: 20px;
      }
    }
  }
}

@media (max-width:550px) {
  .card-panel-description {
    display: none;
  }

  .card-panel-icon-wrapper {
    float: none !important;
    width: 100%;
    height: 100%;
    margin: 0 !important;

    .svg-icon {
      display: block;
      margin: 14px auto !important;
      float: none !important;
    }
  }
}
</style>
