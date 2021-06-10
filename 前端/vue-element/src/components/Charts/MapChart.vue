<template>
  <div class="mapChart-container">
    <div :id="id" :class="className" :style="{width: width, height: height}" v-loading="listLoading">
    </div>
  </div>
</template>

<script>
import echarts from 'echarts'
import resize from './mixins/resize'
import BT from 'echarts/map/json/city/baotou.json'
import {getDataMapChart} from '@/api/get-chart'

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
      chart: null,
      listLoading: false,
      mapChartQuery: {
        Sex: '',
        keyWord: ''
      },
      yearsOptions: [{
        value: '2017',
        label: '2017'
      },{
        value: '2018',
        label: '2018',
      },{
        value: '2019',
        label: '2019'
      }]
    }
  },
  created() {
    // this.mapChartQuery.keyWord = this.yearsOptions[0].value
  },
  mounted() {
    this.initChart()
    // this.getData()
    // this.testOptions()
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
      this.chart = echarts.init(document.getElementById(this.id))
      this.getData()
    },
    getData() {
      this.listLoading = true
      getDataMapChart(this.mapChartQuery).then(response => {
        // console.log(response.data)
        this.updateChart(response.data)
        this.listLoading = false
      })
    },
    updateChart(chartData) {
      console.log(chartData)
      const chartSeries = []
      const years = ['建档立卡2017', '建档立卡2018', '建档立卡2019', '非建档立卡2017', '非建档立卡2018', '非建档立卡2019']
      for (let i = 0; i < chartData.length; i++) {
        chartSeries.push({
          name: years[i],
          type: 'bar',
          data: chartData[i]['Fee']
        })
      }
      const dataOption = {
        tooltip: {
          trigger: 'axis',
          axisPointer: {
            type: 'cross',
            crossStyle: {
              color: '#999'
            }
          },
          formatter: function (data) {
            let temp = data[0].name;
            for (let i = 0; i < data.length; i++) {
              temp += "<br/>" + data[i].seriesName + ': ' +
                (Math.ceil(data[i].value).toString().replace(/(\d)(?=(?:\d{3})+$)/g, '$1,'))
                + "元";
            }
            return temp;
          }
        },
        toolbox: {
          feature: {
            magicType: {show: true, type: ['line', 'bar']},
            restore: {show: true},
          }
        },
        xAxis: [
          {
            type: 'category',
            data: ['1月', '2月', '3月', '4月', '5月', '6月', '7月', '8月', '9月', '10月', '11月', '12月'],
            axisPointer: {
              type: 'shadow'
            }
          }
        ],
        yAxis: {
          type: 'value',
          name: '费用',
          min: 0,
          axisLabel: {
            formatter: '{value} /元'
          }
        },
        legend: {
          data: ['建档立卡2017', '建档立卡2018', '建档立卡2019']
        },
        series: chartSeries
      }
      console.log(dataOption)
      this.chart.setOption(dataOption)
    }
  },
}
</script>

<style>
.mapChart-container {
  width: 100%;
  height: 100%;
}
</style>
