<template>
  <div class="keyboard-container">
    <div>
      <el-select v-model="drugChartQuery.yearKey" style="width: 90px" class="filter-item" placeholder="年份">
        <el-option v-for="item in yearOptions"  :key="item.key" :label="item.label" :value="item.value"></el-option>
      </el-select>
      <el-select v-model="drugChartQuery.classKey" style="width: 90px" class="filter-item" placeholder="类别">
        <el-option v-for="item in classOptions"  :key="item.key" :label="item.label" :value="item.value"></el-option>
      </el-select>
      <el-select v-model="drugChartQuery.numKey" style="width: 105px" class="filter-item" placeholder="展示数量">
        <el-option v-for="item in numOptions"  :key="item.key" :label="item.label" :value="item.value"></el-option>
      </el-select>
      <el-button type="primary" class="el-icon-pie-chart" @click="showChart">
        展示
      </el-button>
    </div>
    <div :id="id" :class="className" :style="{height:height,width:width}" v-loading="listLoading"/>
  </div>

</template>

<script>
import echarts from 'echarts'
import resize from './mixins/resize'

import { getDrugChart } from '@/api/get-chart'
import {fetchList} from "@/api/article";

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
      drugChartQuery: {
        yearKey: "",
        classKey: "",
        numKey: ""
      },
      yearOptions: [{
        value: '2017',
        label: '2017'
      },{
        value: '2018',
        label: '2018'
      },{
        value: '2019',
        label: '2019'
      }],
      classOptions: [{
        value: '甲类',
        label: '甲类'
      },{
        value: '乙类',
        label: '乙类'
      },{
        value: '丙类',
        label: '丙类'
      }],
      numOptions: [{
        value: '5',
        label: '5'
      },{
        value: '10',
        label: '10'
      },{
        value: '20',
        label: '20'
      }]
    }
  },
  mounted() {
    this.initDrugNumChart()
    // this.getData()
  },
  beforeDestroy() {
    if (!this.chart) {
      return
    }
    this.chart.dispose()
    this.chart = null
  },
  methods: {
    initDrugNumChart() {
      this.chart = echarts.init(document.getElementById(this.id))

      // this.chart.setOption({
      //
      //   legend: {
      //
      //   },
      //   xAxis: [
      //     {type: 'category', name: '月份', gridIndex: 0},
      //     {type: 'category', name: '月份', gridIndex: 1}
      //   ],
      //   yAxis: [
      //     {gridIndex: 0, name: '药品数目'},
      //     {gridIndex: 1, name: '药品数目'}
      //   ],
      //   grid: [
      //     {top: '55%', width: 500, left: 100, containLabel: true},
      //     {top: '55%', width: 500, right: 100, containLabel: true}
      //   ]
      // })
    },
    showChart() {
      this.chart.dispose()
      this.chart = null
      this.initDrugNumChart()
      this.getDrugNumData()
    },
    getDrugNumData() {
      this.listLoading = true
      getDrugChart(this.drugChartQuery).then(response => {
        this.updateDrugNumChart(response.data)
        this.listLoading = false
      })
    },
    updateDrugNumChart(chartData) {
      let chartSeries = []

      for (let i = 1; i < chartData.items.length; i++) {
        chartSeries.push({type: 'line', smooth: true, seriesLayoutBy: 'row', emphasis: {focus: 'series'},xAxisIndex: 0, yAxisIndex: 0})
        chartSeries.push({type: 'line', smooth: true, seriesLayoutBy: 'row', emphasis: {focus: 'series'}, xAxisIndex: 1, yAxisIndex: 1, datasetIndex: 1})
      }

      chartSeries.push({
        type: 'pie',
        id: 'pie',
        radius: '30%',
        center: ['25%', '25%'],
        legend: {
          show: false
        },
        emphasis: {
          focus: 'data',
        },
        label: {
          formatter: '{b}: {@12} ({d}%)'
        },
        encode: {
          itemName: '药名',
          value: '12',
          tooltip: '12'
        }
      }
      ,{
        type: 'pie',
        id: 'pie1',
        radius: '30%',
        datasetIndex: 1,
        center: ['75%', '25%'],
        legend: {
          show: false
        },
        emphasis: {
          focus: 'data'
        },
        label: {
          formatter: '{b}: {@12} ({d}%)'
        },
        encode: {
          itemName: '药名',
          value: '12',
          tooltip: '12'
        }
      }
      )
      // console.log(chartData.items)
      // console.log(chartData.items1)

      const dataOption = {
        legend: {
        },
        tooltip: {
          trigger: 'axis',
          showContent: true
        },
        xAxis: [
          {type: 'category', name: '月份', gridIndex: 0},
          {type: 'category', name: '月份', gridIndex: 1}
        ],
        yAxis: [
          {gridIndex: 0, name: '药品数目'},
          {gridIndex: 1, name: '药品数目'}
        ],
        grid: [
          {top: '55%', width: 500, left: 100, containLabel: true},
          {top: '55%', width: 500, right: 100, containLabel: true}
        ],
        dataset: [{
          source: chartData.items
        }
        ,{
          source: chartData.items1
        }]
        ,
        series: chartSeries
      }

      this.chart.on('updateAxisPointer', (event) => {
        let xAxisInfo = event.axesInfo[0];
        if (xAxisInfo) {
          if (xAxisInfo.axisIndex === 1) {
            let dimension = xAxisInfo.value + 1;
            this.chart.setOption({
              series: {
                id: 'pie1',
                label: {
                  formatter: '{b}: {@[' + dimension + ']} ({d}%)'
                },
                encode: {
                  value: dimension,
                  tooltip: dimension
                }
              }
            });
          } else {
            let dimension1 = xAxisInfo.value + 1;
            this.chart.setOption({
              series: {
                id: 'pie',
                label: {
                  formatter: '{b}: {@[' + dimension1 + ']} ({d}%)'
                },
                encode: {
                  value: dimension1,
                  tooltip: dimension1
                }
              }
            });
          }
        }
      })
      // console.log(dataOption)
      this.chart.setOption(dataOption)


    }
  }
}
</script>

<style>
.keyboard-container {
  width: 100%;
  height: 100%;
}
</style>
