<template>
  <div :class="className" :style="{height:height,width:width}" />
</template>

<script>
import echarts from 'echarts'
require('echarts/theme/macarons') // echarts theme
import resize from './mixins/resize'
import {getDataPredictChart} from "@/api/get-chart";

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
      default: '350px'
    },
    autoResize: {
      type: Boolean,
      default: true
    },
    chartData: {
      type: Object,
      required: true
    }
  },
  data() {
    return {
      chart: null,
      chartPredictQuery: {
      }
    }
  },
  watch: {
    chartData: {
      deep: true,
      handler(val) {
        this.setOptions(val)
      }
    }
  },
  mounted() {
    // this.$nextTick(() => {
    //   this.initChart()
    // })
    this.initChart()
  },
  beforeDestroy() {
    if (!this.chart) {
      return
    }
    this.chart.dispose()
    this.chart = null
  },
  methods: {
    initChart() {
      this.chart = echarts.init(this.$el, 'macarons')
      getDataPredictChart(this.chartPredictQuery).then(response => {
        this.setOptions(response.data)
      })
    },
    setOptions(chartData) {
      console.log(chartData)
      this.chart.setOption({
        title: {
          text: '费用预测图',
          left: 'center',
          align: 'right'
        },
        grid: {
          bottom: 80
        },
        toolbox: {
          feature: {
            dataZoom: {
              yAxisIndex: 'none'
            },
            restore: {},
            saveAsImage: {}
          }
        },
        tooltip: {
          trigger: 'axis',
          axisPointer: {
            type: 'cross',
            animation: false,
            label: {
              backgroundColor: '#505765'
            }
          }
        },
        legend: {
          data: ['预测费用', '实际费用', '预测费用上限', '预测费用下限'],
          left: 10
        },
        dataZoom: [
          {
            show: true,
            realtime: true,
            start: 79,
            end: 100
          },
          {
            type: 'inside',
            realtime: true,
            start: 79,
            end: 100
          }
        ],
        xAxis: [
          {
            type: 'category',
            boundaryGap: false,
            axisLine: {onZero: false},
            data: chartData['ds']
          }
        ],
        yAxis:
          {
            name: '费用',
            type: 'value'
          },

        series: [
          {
            name: '预测费用',
            type: 'line',
            // areaStyle: {},
            lineStyle: {
              width: 1
            },
            emphasis: {
              focus: 'series'
            },
            data: chartData['yhat']
          },
          {
            name: '实际费用',
            type: 'line',
            // areaStyle: {},
            lineStyle: {
              width: 1
            },
            emphasis: {
              focus: 'series'
            },
            data: chartData['y']
          },
          {
            name: '预测费用上限',
            type: 'line',
            lineStyle: {
              width: 1
            },
            emphasis: {
              focus: 'series'
            },
            areaStyle : {
              normal : {
                color: '#0000CD',
                origin: 'start',
                opacity: 0.5
              }
            },
            data: chartData['yhat_upper']
          },
          {
            name: '预测费用下限',
            type: 'line',
            // areaStyle: {},
            lineStyle: {
              width: 1
            },
            emphasis: {
              focus: 'series'
            },
            areaStyle : {
              normal : {
                color: '#fff',
                origin: 'start',
                shadowColor: '#F3F3F3',
                shadowOffsetX: 1
              }
            },
            data: chartData['yhat_lower']
          },
        ]
      })
    }
  }
}
</script>
