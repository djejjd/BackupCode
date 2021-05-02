<template>
  <div class="lineMarker-container">
    <el-select v-model="drugChartQuery.classKey" style="width: 110px" class="filter-item" placeholder="药品类别">
      <el-option v-for="item in classOptions"  :key="item.key" :label="item.label" :value="item.value"></el-option>
    </el-select>
    <el-select v-model="drugChartQuery.feeKey" style="width: 110px" class="filter-item" placeholder="费用类别">
      <el-option v-for="item in feeOptions"  :key="item.key" :label="item.label" :value="item.value"></el-option>
    </el-select>
    <el-button type="primary" class="el-icon-pie-chart" @click="lineMarkerShowChart">
      展示
    </el-button>
    <div :id="id" :class="className" :style="{height:height,width:width}"  v-loading="listLoading"/>
  </div>
</template>

<script>
import echarts from 'echarts'
import resize from './mixins/resize'
import {getDrugFeeChart} from '@/api/get-chart'

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
        classKey: '',
        feeKey: '',
        numKey: '',
        searchClass: "A"
      },
      classOptions: [{
        value: '甲类',
        label: '甲类'
      },{
        value: '乙类',
        label: '乙类'
      },{
        value: '丙类',
        label: '丙类'
      },{
        value: 'all',
        label: '所有'
      }],
      feeOptions: [{
        value: 'FeeSum',
        label: '费用总额'
      },{
        value: 'AllowedComp',
        label: '补贴费用'
      },{
        value: 'UnallowedComp',
        label: '自付费用'
      }]
    }
  },
  mounted() {
    this.initChart()
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
    lineMarkerShowChart() {
      this.initChart()
      this.getData()
    },
    initChart() {
      this.chart = echarts.init(document.getElementById(this.id))
    },
    getData() {
      this.listLoading = true
      getDrugFeeChart(this.drugChartQuery).then(response => {
        // console.log(response.data)
        this.updateChart(response.data)
        this.listLoading = false
      })
    },
    updateChart(chartData) {
      console.log(chartData.items)
      const chartSeries = []
      for (let i = 0; i < chartData.items.length; i++) {
        chartSeries.push({
          name: chartData.years[i],
          type: 'bar',
          data: chartData.items[i]
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
          data: chartData.years
        },
        series: chartSeries
      }
      console.log(dataOption)
      this.chart.setOption(dataOption)
    }
  }
}
</script>

<style>
.lineMarker-container {
  width: 100%;
  height: 100%;
}
</style>
