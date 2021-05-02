<template>
<div class="chart-container">
  <div>
    <el-select v-model="ageChartQuery.keyWord" style="width: 90px" class="filter-item">
      <el-option v-for="item in yearsOptions"  :key="item.key" :label="item.label" :value="item.value" @click.native="testOptions" ></el-option>
    </el-select>
  </div>
  <div :id="id" :class="className" :style="{width: width, height: height}" v-loading="listLoading"/>
</div>
</template>

<script>
import echarts from 'echarts'
import resize from './mixins/resize'
import {getDataAgeChart} from '@/api/get-chart'

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
      ageChartQuery: {
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
    this.ageChartQuery.keyWord = this.yearsOptions[0].value
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
      this.chart.setOption({
        tooltip: {
          trigger: 'axis',
          axisPointer: {            // 坐标轴指示器，坐标轴触发有效
            type: 'shadow',        // 默认为直线，可选为：'line' | 'shadow'
          },
          formatter: function (data) {
            let ttt = data[0].name;
            for (let i = 0; i < data.length; i++) {
              ttt += '<br/>' + data[i].seriesName + ': ' +
                (Math.abs(data[i].value))+'人';
            }
            return ttt;
          }
        },
        legend: {
          data: ['建档立卡男', '建档立卡女', '非建档立卡男', '非建档立卡女']
        },
        grid: {
          left: '3%',
          right: '4%',
          bottom: '3%',
          containLabel: true
        },
        xAxis: [
          {
            type: 'value',
            axisLabel: {
              formatter: function (data) {
                return (Math.abs(data))
              }
            }
          }
        ],
        yAxis: [
          {
            type: 'category',
            axisTick: {
              show: false
            },
            data: ['0-9', '10-19', '20-29', '30-39', '40-49', '50-59', '60-69', '70-79', '80-89', '90以上']
          }
        ]
      })
    },
    getData() {
      this.listLoading = true
      getDataAgeChart(this.ageChartQuery).then(response => {
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
.chart-container {
  width: 100%;
  height: 100%;
}
</style>
