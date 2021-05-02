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
    this.testOptions()
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
      echarts.registerMap('BT', BT);
      this.chart.setOption({
        tooltip: {
          trigger: 'item',
          formatter: '{b}<br/>{c} (p / km2)'
        },
        toolbox: {
          show: true,
          orient: 'vertical',
          left: 'right',
          top: 'center',
          feature: {
            dataView: {readOnly: false},
            restore: {},
            saveAsImage: {}
          }
        },
        visualMap: {
          min: 800,
          max: 50000,
          text: ['High', 'Low'],
          realtime: false,
          calculable: true,
          inRange: {
            color: ['lightskyblue', 'yellow', 'orangered']
          }
        },
        series: [
          {
            name: '包头',
            type: 'map',
            mapType: 'BT', // 自定义扩展图表类型
            label: {
              show: true
            },
            data: [
              {name: '东河区', value: 20057.34},
              {name: '昆都仑区', value: 15477.48},
              {name: '青山区', value: 31686.1},
              {name: '石拐区', value: 6992.6},
              {name: '白云鄂博矿区', value: 44045.49},
              {name: '九原区', value: 40689.64},
              {name: '土默特右旗', value: 37659.78},
              {name: '固阳县', value: 45180.97},
              {name: '达尔罕茂明安联合旗', value: 55204.26},
              // {name: '稀土高新区', value: 21900.9} 划归九原区
            ],
            // 自定义名称映射
            nameMap: {
              'DongHe': '东河区',
              'KunDuLun': '昆都仑区',
              'QingSan': '青山区',
              'ShiGuai': '石拐区',
              'BaiYun': '白云鄂博矿区',
              'JiuYuan': '九原区',
              'TuMoTe': '土默特右旗',
              'GuYang': '固阳县',
              'DaErHan': '达尔罕茂明安联合旗'
            }
          }
        ]
      })
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
      const Sex = chartData.sex
      const series = []
      for (let i = 0; i < chartData.items.length; i++) {
        let sex = chartData.sex[i]
        // console.log(chartData.items[i])
        series.push({
          name: sex,
          type: 'bar',
          stack: 'a',
          label: {
            normal: {
              show: true,
              formatter: function (params) {
                return (Math.abs(params.value))
              }
            }
          },
          emphasis: {
            focus: 'series'
          },
          data: chartData.items[i]
        })
      }
      const dataOption = {
        series: series
      }
      this.chart.setOption(dataOption)
    },
    testOptions() {
      this.getData()
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
